import threading
import time
from typing import Set

from config import Config
from telegram_observer import TelegramObserver
from ws_ohlcv_manager import StreamManager as AggTrades
from ws_depth_manager import DepthBooksManager
from action_codec import ActionCodec
from paper_broker import PaperBroker


def symbol_norm(s: str) -> str:
    return s.strip().lower()


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

        #init trading services
        self.codec = ActionCodec(S_cap=64, K=2)   # S_cap = верхняя планка (маска скроет паддинг)
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
        # выравниваемся на следующую целую секунду
        next_tick = int(time.time()) + 1  

        self.on_token("btcusdt")                                          # debug

        while not self._stop.is_set():
            # ждем до границы секунды + дельта
            now = time.time()  
            sleep_s = (next_tick - now) + (self._poll_delay_ms / 1000.0)
            print("sleep_s: ", sleep_s)
            if sleep_s > 0:
                self._stop.wait(timeout=sleep_s)

            # job
            try:
                # get data
                bars = self._ohlcv.get_all_last_bars() 
                dom_all = self._depth.get_all_dom(L=40)           # пока L=40; позже расширим sampler'ом
                feats = self._depth.get_all_features()

                # выберем список активных символов (есть валидный DOM+bar)
                symbols = []
                for s in sorted(set(dom_all.keys()) & set(bars.keys()) & set(feats.keys())):
                    if dom_all[s].get("mid", 0.0) > 0 and bars[s].o > 0 and feats[s].get("sum_bid_n_usd") > 0:
                        symbols.append(s)
                if not symbols:
                    continue
                print("symbols: ", symbols)

                # ACTION
                # здесь у вас будет вызов модели → получите дискретное действие `a`
                a = 0  # заглушка: HOLD
                kind, sym_idx, payload = self.codec.decode(a)
                print("codec: ")
                print("kind: ", kind, "sym_idx: ", sym_idx, "payload :", payload)

                if kind == "trade":
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
                while next_tick <= time.time():
                    print("second missing")
                    next_tick += 1