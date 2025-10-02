# file: ws_kline_book_manager.py
import time
import math
import json
import threading
from dataclasses import dataclass
from typing import Dict, Optional

import websocket              # pip install websocket-client
import zmq                    # pip install pyzmq

# ===================== НАСТРОЙКИ =====================
BINANCE_WS_BASE = "wss://fstream.binance.com"
ZMQ_SUB_URL     = "tcp://127.0.0.1:5556"  # сюда ТГ-бот шлёт символы (btcusdt, ethusdt, ...)
TTL_SECONDS     = 1* 60                 # 10 минут жизни «активного» символа после touch()
RECONNECT_MAX_BACKOFF = 30

# печатать отладку ридеров/подписок
DEBUG_WS = True

def symbol_norm(sym: str) -> str:
    return sym.lower().strip()


# ===================== РИДЕР PER-СИМВОЛ =====================
@dataclass
class BookSnapshot:
    ts_ms: int = 0
    bid: float = 0.0
    ask: float = 0.0
    bid_qty: float = 0.0
    ask_qty: float = 0.0

    def features(self) -> Dict[str, float]:
        if self.bid and self.ask:
            spread = self.ask - self.bid
            mid    = (self.ask + self.bid) / 2.0
        else:
            spread = 0.0
            mid    = 0.0
        depth_sum = self.bid_qty + self.ask_qty
        imbalance = (self.bid_qty - self.ask_qty) / depth_sum if depth_sum > 0 else 0.0
        # микро-прайс (весим mid в сторону более «толстой» стороны)
        microprice = (self.ask * self.bid_qty + self.bid * self.ask_qty) / depth_sum if depth_sum > 0 else mid
        return {
            "bid": self.bid,
            "ask": self.ask,
            "bid_qty": self.bid_qty,
            "ask_qty": self.ask_qty,
            "spread": spread,
            "mid": mid,
            "imbalance": imbalance,
            "microprice": microprice,
        }


class WsReaderKlineBook(threading.Thread):
    """
    Пер-символьный ридер Binance Futures.
    Один WS соединение на символ, комбинированный поток:
      - {symbol}@kline_1s
      - {symbol}@bookTicker      (лучший бид/аск и их объёмы)
    На каждое 'kline.x == true' печатаем пакет с клайном + фичами стакана.
    """
    def __init__(self, symbol: str):
        super().__init__(daemon=True, name=f"WS-{symbol.upper()}")
        self.symbol = symbol_norm(symbol)
        self.stop_event = threading.Event()
        self.wsapp: Optional[websocket.WebSocketApp] = None
        print("WsReader created")

        # текущие данные для фич
        self._last_book = BookSnapshot()
        print("FeaturesBook created")

    # ---- WS lifecycle ----
    def run(self):
        streams = f"{self.symbol}@kline_1s/{self.symbol}@bookTicker"
        #url = f"{BINANCE_WS_BASE}/stream?streams={streams}"
        url = f"{BINANCE_WS_BASE}/ws/{streams}"

        backoff = 1
        while not self.stop_event.is_set():
            try:
                if DEBUG_WS:
                    print(f"[{self.name}] connecting: {url}")

                self.wsapp = websocket.WebSocketApp(
                    url,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )

                def on_open(wsapp):
                    if DEBUG_WS:
                        print(f"[{self.name}] opened")
                    nonlocal backoff
                    backoff = 1

                self.wsapp.on_open = on_open
                # Пинги обязательны, иначе обламывается на NAT/файрволах
                self.wsapp.run_forever(ping_interval=15, ping_timeout=10)

            except Exception as e:
                if DEBUG_WS:
                    print(f"[{self.name}] run() exception: {e}")

            if self.stop_event.is_set():
                break

            time.sleep(backoff)
            backoff = min(backoff * 2, RECONNECT_MAX_BACKOFF)

    def stop(self):
        self.stop_event.set()
        try:
            if self.wsapp is not None:
                self.wsapp.keep_running = False
                self.wsapp.close()
        except Exception:
            pass

    # ---- WS handlers ----
    def _on_message(self, wsapp, raw: str):
        try:
            msg = json.loads(raw)
            #print(msg)
            data = msg.get("data", msg)  # multiplex возвращает {"stream": ..., "data": {...}}
            e = data.get("e")            # тип события
            if e == "kline":
                self._handle_kline(data)
            elif e == "bookTicker":
                self._handle_bookticker(data)
            # игнор прочего
        except Exception as e:
            if DEBUG_WS:
                print(f"[{self.name}] on_message error: {e}")

    def _on_error(self, wsapp, error):
        if DEBUG_WS:
            print(f"[{self.name}] ws error: {error}")

    def _on_close(self, wsapp, status_code, msg):
        if DEBUG_WS:
            print(f"[{self.name}] closed: {status_code} {msg}")

    # ---- parsers ----
    def _handle_bookticker(self, data: dict):
        # https://binance-docs.github.io/apidocs/futures/en/#individual-symbol-book-ticker-streams
        # fields: u(updateId), s(symbol), b(bestBidPrice), B(bestBidQty), a(bestAskPrice), A(bestAskQty)
        try:
            self._last_book = BookSnapshot(
                ts_ms=int(data.get("E", 0)),
                bid=float(data["b"]),
                ask=float(data["a"]),
                bid_qty=float(data["B"]),
                ask_qty=float(data["A"]),
            )
        except Exception:
            pass

    def _handle_kline(self, data: dict):
        # https://binance-docs.github.io/apidocs/futures/en/#kline-candlestick-streams
        # data["k"] = { t, T, s, i, f, L, o,h,l,c, v, n, x, ... }
        k = data.get("k") or {}
        try:
            is_final = bool(k.get("x"))
            if not is_final:
                return  # печатаем только финальный 1s бар по версии биржи

            bar = {
                "t": int(k["T"]) // 1000,        # секунда закрытия бара (биржевая)
                "symbol": k["s"].lower(),
                "o": float(k["o"]),
                "h": float(k["h"]),
                "l": float(k["l"]),
                "c": float(k["c"]),
                "v": float(k["v"]),              # volume (base asset)
                "n": int(k["n"]),                # number of trades (matching engine)
            }

            # фичи с лучшего уровня стакана (последнее bookTicker-событие)
            book_feats = self._last_book.features()

            packet = {
                "t": bar["t"],                   # unix sec (биржевой closeTime/1000)
                "symbol": bar["symbol"],
                "kline_1s": bar,
                "book": book_feats,              # bid/ask/spread/mid/imbalance/microprice/...
                # можно добавить техническую метку получения:
                "recv_ts": int(time.time()*1000)
            }

            # >>> ТВОЙ единый объект для модели: печатаем в консоль
            print(json.dumps(packet, ensure_ascii=False))

        except Exception as e:
            if DEBUG_WS:
                print(f"[{self.name}] kline parse error: {e}")


# ===================== МЕНЕДЖЕР АКТИВНЫХ СИМВОЛОВ =====================
@dataclass
class SymState:
    reader: WsReaderKlineBook
    expires_at: float  # epoch seconds


class StreamManager:
    """
    - Поддерживает пул активных символов по TTL (touch() продлевает).
    - Для каждого символа поднимает ридер @kline_1s + @bookTicker и печатает
      пакеты при закрытии 1s бара.
    - Параллельно слушает ZMQ (SUB) и делает touch(sym) по входящим уведомлениям.
    """
    def __init__(self, zmq_sub_url: str = ZMQ_SUB_URL):
        self._states: Dict[str, SymState] = {}
        self._lock = threading.Lock()
        self._stop = threading.Event()

        # GC: снимаем истёкшие символы каждую секунду
        self._th_gc = threading.Thread(target=self._gc_loop, daemon=True, name="GC")

        # ZMQ: слушаем уведомления из ТГ
        self._zmq_sub_url = zmq_sub_url
        self._th_zmq = threading.Thread(target=self._zmq_loop, daemon=True, name="ZMQ-SUB")

    # ---- lifecycle ----
    def start(self):
        self._th_gc.start()
        self._th_zmq.start()

    def stop(self):
        self._stop.set()
        # остановить ридеры
        with self._lock:
            for st in self._states.values():
                st.reader.stop()

        self._th_gc.join(timeout=1)
        self._th_zmq.join(timeout=1)

    # ---- public API ----
    def touch(self, symbol: str):
        """
        Делает символ «активным» на TTL_SECONDS, поднимая поток если нужно.
        Многократные touch — просто продление таймера.
        """
        symbol = symbol_norm(symbol)
        with self._lock:
            if symbol not in self._states:
                reader = WsReaderKlineBook(symbol)
                self._states[symbol] = SymState(
                    reader=reader,
                    expires_at=time.time() + TTL_SECONDS
                )
                reader.start()
                print(f"[MANAGER] started {symbol.upper()}")
            else:
                self._states[symbol].expires_at = time.time() + TTL_SECONDS
                print(f"[MANAGER] refreshed {symbol.upper()} (TTL extended)")

    # ---- internals ----
    def _gc_loop(self):
        while not self._stop.is_set():
            time.sleep(1)
            now = time.time()
            expired = []
            with self._lock:
                for sym, st in list(self._states.items()):
                    if now >= st.expires_at:
                        st.reader.stop()
                        expired.append(sym)
                        del self._states[sym]
            for sym in expired:
                print(f"[GC] Stopped & removed {sym.upper()} (TTL expired)")

    def _zmq_loop(self):
        """
        Простейший SUB: ждём строку с именем символа.
        Форматы:
          - "btcusdt"
          - "symbol: ethusdt"
          - "ETHUSDT any text ..."
        Берём первое «слово», нормируем, делаем touch().
        """
        ctx = zmq.Context(io_threads=1)
        sub = ctx.socket(zmq.SUB)
        sub.setsockopt_string(zmq.SUBSCRIBE, "")
        sub.connect(self._zmq_sub_url)
        print(f"[ZMQ] SUB connected to {self._zmq_sub_url}")

        while not self._stop.is_set():
            try:
                msg = sub.recv_string(flags=zmq.NOBLOCK)
            except zmq.Again:
                time.sleep(0.01)
                continue
            except Exception as e:
                print(f"[ZMQ] recv error: {e}")
                time.sleep(0.5)
                continue

            # выдёргиваем 1-е «слово», отбрасываем префиксы вроде "symbol:"
            parts = msg.strip().split()
            if not parts:
                continue
            raw = parts[0]
            if ":" in raw:
                raw = raw.split(":", 1)[1]
            sym = symbol_norm(raw)
            if not sym:
                continue
            self.touch(sym)


# ===================== Пример запуска =====================
if __name__ == "__main__":
    mgr = StreamManager()
    mgr.start()

    # --- ДЕМО без ZMQ: руками «как будто пришли уведомления из ТГ»
    # (оставь, если хочешь быстро проверить без реального ZMQ)
    def demo_feeder():
        time.sleep(1)
        mgr.touch("btcusdt")
        mgr.touch("ethusdt")
        time.sleep(5)
        mgr.touch("bnbusdt")   # добавим через 5с
        time.sleep(8)
        mgr.touch("btcusdt")   # продлим BTC
    threading.Thread(target=demo_feeder, daemon=True).start()

    try:
        while True:
            time.sleep(0.5)
    except KeyboardInterrupt:
        mgr.stop()


# ===================== ПОДСКАЗКИ ПО ДОП. ФИЧАМ =====================
# Какие ещё потоки Binance Futures можно подключать и в какие фичи их превращают:
#
# 1) @markPrice            — «маркировочная» цена + индексная цена + funding rate таймер
#    Фичи: markPrice, indexPrice, spread(c - mark), delta(index - last), время до next funding, сам funding rate.
#
# 2) @aggTrade             — принт-линия «сделок» (аггрегированных); полезно для микроструктуры:
#    Фичи: last price, last qty, trade rate (count/sec), VWAP за окно, дельта объёмов (buy vs sell, через isBuyerMaker),
#           burst-score (всплески тиков), cv / variance межтик-таймингов.
#
# 3) @bookTicker           — уже подключили (top of book). Можно ещё хранить истории для производных: d(mid)/dt, d(spread)/dt.
#
# 4) @depth{N}@100ms       — прицельно даёт N уровней и дельты. Если готов строить локальный ордербук:
#    Фичи: суммарные объёмы topN, кумулятивные дисбалансы, slope (наклон книги), «стенка»/iceberg детектор, impact price.
#
# 5) @forceOrder           — ликвидации. Сигнальная редкая штука; можно как бинарный флаг и/или объём ликвидаций в окне.
#
# 6) @continuousKline_X    — если работаешь со спотовыми/перпетуальными синтетиками (index / mark), но для модели обычно хватит markPrice.
#
# Как добавить ещё поток к символу:
#   - В WsReaderKlineBook в `streams = f"{sym}@kline_1s/{sym}@bookTicker"` просто дописать `/{sym}@markPrice`
#   - В `_on_message` распарсить новый `e` и обновлять свои поля/фичи; в момент закрытия 1s клайна — класть их в packet.
#
# Если захочешь ровно «стеночные» секунды по локальному времени (как делали раньше) — можно дополнительно буферизовать
# входящие kline-апдейты и печатать по своему метрономy, но обычно для согласованности с историей лучше держаться `x==true`.
