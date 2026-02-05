# =============================
# file: orchestrator.py
# =============================
import threading
import time
import math

from telegram_observer import TelegramObserver
from ws_ohlcv_manager import StreamManager as AggTrades
from ws_depth_manager import TokenOrderBooksManager
from snapshot_packer import SnapshotPacker
from action_codec import ActionCodec
from paper_broker import PaperBroker
from config import Config
from tools.profiling import StepProfiler    #profiling

def symbol_norm(s: str) -> str:
    return s.strip().lower()

class Orchestrator:
    """
    - Тикает 1Гц со «стенкой» + poll_delay_ms
    """
    def __init__(self, poll_delay_ms: int = 120):
        # ===== PROFILING =====
        self.profiling_activated = Config.PROFILING
        # =====================

        # delay for data services to ingest & aggregate data before consuming
        self._poll_delay_ms = poll_delay_ms

        #init data services 
        self._tg = TelegramObserver()
        self._ohlcv = AggTrades()
        self._depth = TokenOrderBooksManager()

        #init features services
        self.codec = ActionCodec(S_cap=64, K=2)   # S_cap = верхняя планка (маска скроет паддинг)
        #self.sampler = DepthSampler(top_n=40, tail_bins=32, tail_max_bps=50.0)
        self.packer  = SnapshotPacker(extra_keys=["sum_bid_n_usd","sum_ask_n_usd", "cum_imbalance_n_usd"])

        # новый: единовременная аллокация матриц
        self.packer.alloc_workspace(S_cap=64)

        #init trading services
        self.broker = PaperBroker(csv_path="trades.csv", taker_fee_rate=0.0004)

        #threads management
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run_loop, name="OrchestratorLoop", daemon=True)

    # === lifecycle ===
    def start(self) -> None:
        self._tg.start(self.on_token)
        # await tg connection
        print("[ORCH] waiting for TG ready...")
        if not self._tg._ready.wait(timeout=900):
            raise RuntimeError("Telegram did not become ready in time")
        print("[ORCH] TG ready, starting streams")

        self._ohlcv.start()
        self._depth.start()
        self._thread.start()

        # агрессивно урезаем numpy-печать (на всякий)
        try:
            import numpy as np
            np.set_printoptions(edgeitems=2, threshold=16, suppress=True)
        except Exception:
            print("[ORCH] не удалось настроить numpy-печать")
            pass

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=2)
        self._tg.stop()

    # === intake from TG ===
    def on_token(self, symbol: str) -> None:
        s = symbol_norm(symbol)
        self._ohlcv.touch(s)
        self._depth.touch(s)

    # === 1Hz wall-clock loop ===
    def _run_loop(self) -> None:
        # === планируем по monotonic, выравниваем на ближайшую "целую" секунду ===
        mono = time.monotonic
        poll_s = self._poll_delay_ms / 1000.0
        next_tick = math.floor(mono()) + 1  # выравниваемся

        # === profiling/debug ===
        orch_tb  = {}
        orch_prof = StepProfiler(orch_tb)
        #========================

        #debug
        self.on_token("BTCUSDT")

        iter_idx = 0    #index for printing states every 5sec --> debug purpose
        while not self._stop.is_set():
            # подождать до границы + дельта
            sleep_s = (next_tick + poll_s) - mono()
            print("sleep_s: ", sleep_s)     #debug --> see how much time left before the next iteration: > 0 is ok
            if sleep_s > 0:
                self._stop.wait(timeout=sleep_s)

            # === profiling/debug ===
            orch_prof.reset()           #profiling
            t0 = time.perf_counter()    #profiling
            #========================

            # job
            try:
                # получение данных
                bars = self._ohlcv.get_all_last_bars() 
                t0 = orch_prof.lap(t0, "bars")                  #profiling
                # ВМЕСТО get_all_doms + get_all_market_data берем ОДИН пакет
                ai_data = self._depth.get_all_ai_data()
                t0 = orch_prof.lap(t0, "ai_data")               #profiling

                # dom_all = self._depth.get_all_doms(n=100) 
                # t0 = orch_prof.lap(t0, "dom")                #profiling
                # feats = self._depth.get_all_market_data()
                # t0 = orch_prof.lap(t0, "feats")              #profiling
                
                # Вызов упаковщика
                payload = self.packer.pack(
                    bars=bars,
                    ai_data=ai_data, # Передаем сразу готовые данные
                    S_cap=64,       # та же планка, что и у ActionCodec
                    # dom_all=dom_all,
                    # feats=feats,
                    # symbols_order_hint=None,    # можно передать твой порядок, если нужен
                    debug_timings=self.profiling_activated   #profiling                 
                )

                # ============== debug ==============
                active_count = int(payload['mask'].sum())
                print(active_count)
                if active_count > 0:
                    first_sym = payload['symbols'][0]
                    print(f"--- [TEST] Payload OK | Symbols: {active_count} | Lead: {first_sym} ---")
                    # Проверка, что хвосты не пустые (32-й бин для примера)
                    tail_sum = payload['depth_tail_qty'][0].sum()
                    print(f"    Sample Tail Volume: {tail_sum:.2f} | Bars: {payload['bars'][0]}")
                # ===================================

                t0 = orch_prof.lap(t0, "pack_loop_compress_total")  #profiling
                if self.profiling_activated: 
                    payload.setdefault("timings", {})
                    payload["timings"]["orch"] = {
                        k: round(v, 3) for k, v in orch_tb.items()
                    }

                # выберем список активных символов (есть валидный DOM+bar)
                symbols = payload["symbols"]

                # --- компактный лог раз в N итераций ---
                if iter_idx % 5 == 0 and "timings" in payload:
                    print("symbols: ", len(symbols), symbols)
                    #print(f"t_bars={dt_bars:.1f}ms t_dom={dt_dom:.1f}ms t_feats={dt_feats:.1f}ms t_pack={dt_pack:.1f}ms")
                    print("PACK timings (ms):", payload["timings"])
                if not symbols:
                    continue


                # ACTION
                # здесь у вас будет вызов модели → получите дискретное действие `a`
                a = 0  # заглушка: HOLD
                kind, sym_idx, payload = self.codec.decode(a)
                #if iter_idx % 10 == 0:
                    #print(f"codec: kind={kind} sym_idx={sym_idx} payload={payload}")
                #print("codec: ")
                #print("kind: ", kind, "sym_idx: ", sym_idx, "payload :", payload)

                # if kind == "trade" and 0 <= sym_idx < len(symbols):
                #     if sym_idx >= len(symbols):
                #         pass  # действие в паддинг — игнор
                #     else:
                #         symbol = symbols[sym_idx]
                #         side_idx = payload // self.codec.K    # 0=buy,1=sell
                #         size_lvl = payload %  self.codec.K    # 0..K-1
                #         side = "buy" if side_idx == 0 else "sell"
                #         # простая дискретизация размеров:
                #         size_table = [0.001, 0.005]      # TODO: вынести в конфиг
                #         size = size_table[size_lvl]
                #         ts = int(time.time() * 1000)
                #         self.broker.execute_market(ts, symbol, side, size)

            finally:
                next_tick += 1
                lag = mono() - next_tick
                if lag >= 0:
                    # мы позади: наверстаем, но не спамим логом
                    miss = 0
                    while next_tick <= mono():
                        next_tick += 1
                        miss += 1
                    if miss > 0:
                        print(f"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! second missing x{miss} (lag {lag:.3f}s) !!!!!!!!!!!!!!!!!!!!!!!!!!")
                iter_idx += 1