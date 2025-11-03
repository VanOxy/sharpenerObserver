import threading
import time
import math

from telegram_observer import TelegramObserver
from ws_ohlcv_manager import StreamManager as AggTrades
from ws_depth_manager import DepthBooksManager
from ws_depth_sampler import DepthSampler
from snapshot_packer import SnapshotPacker
from action_codec import ActionCodec
from paper_broker import PaperBroker


def symbol_norm(s: str) -> str:
    return s.strip().lower()

def _ts_ms() -> int:
    return int(time.time() * 1000)

class Orchestrator:
    """
    - Тикает 1Гц со «стенкой» + poll_delay_ms
    """
    def __init__(self, poll_delay_ms: int = 120):
        # delay for data services to ingest & aggregate data before consuming
        self._poll_delay_ms = poll_delay_ms

        #init data services 
        self._tg = TelegramObserver()
        self._ohlcv = AggTrades()
        self._depth = DepthBooksManager()

        #init features services
        self.codec = ActionCodec(S_cap=64, K=2)   # S_cap = верхняя планка (маска скроет паддинг)
        self.sampler = DepthSampler(top_n=40, tail_bins=32, tail_max_bps=50.0)
        self.packer  = SnapshotPacker(self.sampler, extra_keys=["sum_bid_n_usd","sum_ask_n_usd"])

        #init trading services
        self.broker = PaperBroker(csv_path="trades.csv", taker_fee_rate=0.0004)

        #threads management
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run_loop, name="OrchestratorLoop", daemon=True)

    # === lifecycle ===
    def start(self) -> None:
        self._tg.start(self.on_token)
        self._ohlcv.start()
        self._depth.start()
        time.sleep(2)
        self._thread.start()

        # агрессивно урезаем numpy-печать (на всякий)
        try:
            import numpy as np
            np.set_printoptions(edgeitems=2, threshold=16, suppress=True)
        except Exception:
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
        perf = time.perf_counter
        poll_s = self._poll_delay_ms / 1000.0
        # выравниваемся
        next_tick = math.floor(mono()) + 1

        self.on_token("btcusdt")                     # debug

        # вспомогательный профайлер
        def lap(t0, tag):
            t1 = perf()
            dt = (t1 - t0) * 1000.0
            return t1, dt, tag
        
        iter_idx = 0

        while not self._stop.is_set():
            # подождать до границы + дельта
            sleep_s = (next_tick + poll_s) - mono()
            print("sleep_s: ", sleep_s)
            if sleep_s > 0:
                self._stop.wait(timeout=sleep_s)

            t0 = perf()

            # job
            try:
                # get data
                bars = self._ohlcv.get_all_last_bars() 
                t0, dt_bars, _ = lap(t0, "bars")
                dom_all = self._depth.get_all_dom(L=50)           # пока L=40; позже расширим sampler'ом
                t0, dt_dom, _  = lap(t0, "dom")
                feats = self._depth.get_all_features()
                t0, dt_feats, _ = lap(t0, "feats")

                payload = self.packer.pack(
                    bars=bars,
                    dom_all=dom_all,
                    feats=feats,
                    S_cap=64,                        # та же планка, что и у ActionCodec
                    symbols_order_hint=None          # можно передать твой порядок, если нужен
                )
                t0, dt_pack, _ = lap(t0, "pack")

                # выберем список активных символов (есть валидный DOM+bar)
                symbols = payload["symbols"]

                # --- компактный лог раз в N итераций ---
                if iter_idx % 5 == 0:
                    print("symbols: ", symbols)
                    S = len(symbols)
                    print(f"t_bars={dt_bars:.1f}ms t_dom={dt_dom:.1f}ms t_feats={dt_feats:.1f}ms t_pack={dt_pack:.1f}ms")
                    if S:
                        print(f"symbols: {symbols[:min(6,S)]}{'...' if S>6 else ''}")

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

                if kind == "trade" and 0 <= sym_idx < len(symbols):
                    if sym_idx >= len(symbols):
                        pass  # действие в паддинг — игнор
                    else:
                        symbol = symbols[sym_idx]
                        side_idx = payload // self.codec.K    # 0=buy,1=sell
                        size_lvl = payload %  self.codec.K    # 0..K-1
                        side = "buy" if side_idx == 0 else "sell"
                        # простая дискретизация размеров:
                        size_table = [0.001, 0.005]      # TODO: вынести в конфиг
                        size = size_table[size_lvl]
                        dom_sym = dom_all[symbol]
                        ts = int(time.time() * 1000)
                        self.broker.execute_market(ts, symbol, side, size, dom_sym)

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
                        print(f"second missing x{miss} (lag {lag:.3f}s)")
                iter_idx += 1