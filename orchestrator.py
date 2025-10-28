import threading
import time
from typing import Set

from config import Config
from telegram_observer import TelegramObserver
from ws_ohlcv_manager import StreamManager as AggTrades
from ws_depth_manager import DepthBooksManager


def symbol_norm(s: str) -> str:
    return s.strip().lower()


class Orchestrator:
    """
    - Тикает 1Гц со «стенкой» + poll_delay_ms
    """
    def __init__(self, poll_delay_ms: int = 120):
        self._poll_delay_ms = poll_delay_ms

        self._tg = TelegramObserver()
        self._ohlcv = AggTrades()
        self._depth = DepthBooksManager()

        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run_loop, name="OrchestratorLoop", daemon=True)

    # === lifecycle ===
    def start(self) -> None:
        self._tg.start(self.on_token)
        self._ohlcv.start()
        self._depth.start()
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
        next_tick = int(time.time()) + 1  # выравниваемся на следующую целую секунду
        # self.on_token("btcusdt")
        while not self._stop.is_set():
            now = time.time()  # ждем до границы секунды + дельта
            sleep_s = (next_tick - now) + (self._poll_delay_ms / 1000.0)
            if sleep_s > 0:
                self._stop.wait(timeout=sleep_s)

            # job
            ohlcv_bars = self._ohlcv.get_all_last_bars()
            if ohlcv_bars:
                print("ohlcv_bars", ohlcv_bars)

            feats = self._depth.get_all_features()
            if feats:
                print("_depth", feats)

            next_tick += 1