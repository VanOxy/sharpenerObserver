# file: ws_ohlcv_manager.py
from config import Config
import time
import math
import json
import threading
from queue import Queue, Empty
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional

import websocket  # pip install websocket-client

# ---- НАСТРОЙКИ ----
BINANCE_WS_URL = "wss://fstream.binance.com/ws"  # ФЬЮЧЕРСЫ!
TTL_SECONDS = Config.TTL_SECONDS          # держим поток 30 секунд с момента последнего touch()
AGG_INTERVAL_SEC = Config.AGG_INTERVAL_SEC           # один бар в секунду по ЛОКАЛЬНОМУ таймеру
QUEUE_MAXSIZE = 10000         # размер очереди тиков на символ
#TICK_STREAM = "@aggTrade"
TICK_STREAM = "@trade"

def symbol_norm(sym: str) -> str:
    return sym.lower()

# ---------- HUB: очереди тиков ----------
class StreamHub:
    def __init__(self, max_queue=QUEUE_MAXSIZE):
        self._q: Dict[str, Queue] = {}
        self._lock = threading.Lock()
        self._max_queue = max_queue

    def ensure_symbol(self, symbol: str):
        if symbol in self._q:
            return
        with self._lock:
            if symbol not in self._q:
                self._q[symbol] = Queue(maxsize=self._max_queue)

    def has_symbol(self, symbol: str) -> bool:
        return symbol in self._q

    def push(self, symbol: str, tick: dict):
        q = self._q.get(symbol)
        if q is None:
            self.ensure_symbol(symbol)
            q = self._q[symbol]
        try:
            q.put_nowait(tick)
        except Exception:
            # переполнение — дроп (или замени на q.put() для блокировки)
            pass

    def drain_now(self) -> Dict[str, List[dict]]:
        """Забрать всё, что есть в очередях, немедленно и неблокирующе."""
        out: Dict[str, List[dict]] = {}
        for sym, q in list(self._q.items()):
            acc = []
            while True:
                try:
                    acc.append(q.get_nowait())
                except Empty:
                    break
            if acc:
                out[sym] = acc
        return out

    def drop_symbol(self, symbol: str):
        with self._lock:
            self._q.pop(symbol, None)

# ---------- WS Reader ----------
class WsReader(threading.Thread):
    """Читает <symbol>{TICK_STREAM} c Binance Futures и пишет тики в hub."""
    def __init__(self, symbol: str, hub: StreamHub):
        super().__init__(daemon=True, name=f"WS-{symbol}")
        self.symbol = symbol
        self.hub = hub
        self.stop_event = threading.Event()
        self.wsapp: Optional[websocket.WebSocketApp] = None

    def run(self):
        stream_name = f"{self.symbol}{TICK_STREAM}"
        url = f"{BINANCE_WS_URL}/{stream_name}"

        backoff = 1
        while not self.stop_event.is_set():
            try:
                self.wsapp = websocket.WebSocketApp(
                    url,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )

                def on_open(wsapp):
                    nonlocal backoff
                    backoff = 1
                self.wsapp.on_open = on_open

                # Запуск; выходим, если stop() закрыл соединение
                self.wsapp.run_forever(ping_interval=15, ping_timeout=10)
            except Exception:
                pass

            if self.stop_event.is_set():
                break

            time.sleep(backoff)
            backoff = min(backoff * 2, 30)

    def _on_message(self, wsapp, message: str):
        try:
            msg = json.loads(message)
            # aggTrade: p=price, q=qty, T=tradeTime, E=eventTime
            tick = {
                "ts": int(msg.get("T") or msg.get("E")),  # сохраняем биржевой ts для информации
                "price": float(msg["p"]),
                "qty": float(msg["q"]),
            }
            self.hub.push(self.symbol, tick)
        except Exception:
            pass

    def _on_error(self, wsapp, error):
        pass

    def _on_close(self, wsapp, status_code, msg):
        pass

    def stop(self):
        self.stop_event.set()
        try:
            if self.wsapp is not None:
                # жестко рвем run_forever
                self.wsapp.keep_running = False
                self.wsapp.close()
        except Exception:
            pass

# ---------- Агрегатор: бар из "бакета" (порядок = порядок прихода) ----------
def ohlcv_bucket(ticks: List[dict], t_sec: int) -> Optional[dict]:
    if not ticks:
        return None
    prices = [t["price"] for t in ticks]        # порядок — как пришли в очередь
    qtys   = [t.get("qty", 0.0) for t in ticks]
    return {
        "t": t_sec,              # метка бара = секунда закрытия (целая)
        "o": prices[0],
        "h": max(prices),
        "l": min(prices),
        "c": prices[-1],
        "v": sum(qtys),
        "n": len(ticks),
    }

# ---------- Менеджер символов и TTL ----------
@dataclass
class SymState:
    reader: WsReader
    expires_at: float  # epoch seconds

class StreamManager:
    """
    РЕЖЕМ ПО ЛОКАЛЬНОМУ ТАЙМЕРУ:
      - старт с ближайшей целой секунды (ceil(now)),
      - на каждой итерации спим до ровной границы '…:01.000', '…:02.000', …
      - один бар на секунду, без дублей.
    """
    def __init__(self):
        self.hub = StreamHub()
        self._states: Dict[str, SymState] = {}
        self._lock = threading.Lock()
        self._stop = threading.Event()

        self._th_agg = threading.Thread(target=self._aggregator_loop,
                                        daemon=True, name="Aggregator")
        self._th_gc  = threading.Thread(target=self._gc_loop,
                                        daemon=True, name="GC")

        # текущий "бакет" для накапливания тиков между распечатками: symbol -> [ticks]
        self._bucket: Dict[str, List[dict]] = defaultdict(list)

    def start(self):
        self._th_agg.start()
        self._th_gc.start()

    def stop(self):
        self._stop.set()
        with self._lock:
            for st in self._states.values():
                st.reader.stop()
        self._th_agg.join(timeout=1)
        self._th_gc.join(timeout=1)

    def touch(self, symbol: str):
        symbol = symbol_norm(symbol)
        with self._lock:
            if symbol not in self._states:
                print(f"🚀 Starting stream for {symbol.upper()}")
                self.hub.ensure_symbol(symbol)
                reader = WsReader(symbol, self.hub)
                self._states[symbol] = SymState(
                    reader=reader,
                    expires_at=time.time() + TTL_SECONDS
                )
                reader.start()
            else:
                self._states[symbol].expires_at = time.time() + TTL_SECONDS

    # --- внутреннее ---
    def _sleep_to_wall_second(self, target_sec: int):
        """Поспать до ровной стеночной секунды target_sec (UTC)."""
        delay = target_sec - time.time()
        if delay > 0:
            time.sleep(delay)

    def _aggregator_loop(self):
        step = AGG_INTERVAL_SEC

        # целимся ровно в ближайшую целую секунду (ceil(now))
        next_sec_label = int(math.ceil(time.time()))

        while not self._stop.is_set():
            # 1) ждём РОВНОЭ секунду на «стене времени»
            self._sleep_to_wall_second(next_sec_label)

            # 2) забираем накопившиеся тики к этому моменту в текущий бакет
            batch = self.hub.drain_now()
            for sym, ticks in batch.items():
                self._bucket[sym].extend(ticks)

            # 3) финализируем секунду (метка = next_sec_label), печать и очистка бакета
            rows = []
            for sym, ticks in list(self._bucket.items()):
                if not ticks:
                    continue
                bar = ohlcv_bucket(ticks, t_sec=next_sec_label)
                if bar:
                    rows.append((bar["t"], sym.upper(), bar))
                self._bucket[sym].clear()

            for _, sym, bar in sorted(rows, key=lambda r: (r[0], r[1])):
                print(f"{sym} {time.strftime('%H:%M:%S', time.localtime(bar['t']))} "
                      f"O:{bar['o']} H:{bar['h']} L:{bar['l']} C:{bar['c']} V:{bar['v']} N:{bar['n']}")

            # 4) следующая ровная секунда
            now = time.time()
            next_sec_label = int(now) + 1

    def _gc_loop(self):
        while not self._stop.is_set():
            time.sleep(1)  # проверяем TTL каждую секунду
            now = time.time()
            expired: List[str] = []
            with self._lock:
                for sym, st in list(self._states.items()):
                    if now >= st.expires_at:
                        st.reader.stop()
                        expired.append(sym)
                        self.hub.drop_symbol(sym)
                        del self._states[sym]
                        # подчистим текущий бакет
                        self._bucket.pop(sym, None)
            for sym in expired:
                print(f"⏹️ Stopped {sym.upper()} (TTL expired)")


# ---------- Пример ----------
if __name__ == "__main__":
    mgr = StreamManager()
    mgr.start()

    # имитируем «уведомления из ТГ»
    mgr.touch("btcusdt")
    mgr.touch("ethusdt")

    # через 15с обновим BTC (продлеваем ещё на 10 минут)
    def refresher():
        time.sleep(4)
        print("[TG] refresh BTC")
        mgr.touch("btcusdt")

    # через 10с добавим BNB
    def add_bnb():
        time.sleep(7)
        print("[TG] BNB notification")
        mgr.touch("bnbusdt")

    threading.Thread(target=refresher, daemon=True).start()
    threading.Thread(target=add_bnb, daemon=True).start()

    try:
        while True:
            time.sleep(0.5)
    except KeyboardInterrupt:
        mgr.stop()
