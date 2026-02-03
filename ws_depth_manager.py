# file: ws_depth_manager.py
from config import Config
import requests
from websocket import WebSocketApp
import threading
import time
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional, TypedDict
from operator import itemgetter
import heapq
import orjson

# --- Config ---
BINANCE_FUTURES_WS = "wss://fstream.binance.com/ws"
BINANCE_FUTURES_API = "https://fapi.binance.com"
REST_DEPTH_LIMIT = 1000
CONNECT_TIMEOUT = 10
HTTP_TIMEOUT = 5

# --- Manager params ---
AUTO_EVICT_SEC = Config.TTL_SECONDS
GC_INTERVAL_SEC = 1

class DOMLevel(TypedDict):
    px: float
    qty: float
    usd: float
    rel: float

class DOMSnapshot(TypedDict):
    symbol: str
    mid: float
    spread: float
    bids: List[DOMLevel]
    asks: List[DOMLevel]

class TokenOrderBook:
    #Стакан
    #Thread-safe local order book for a SINGLE symbol.
    #Хранилище данных. Содержит лимитные заявки на покупку (bids) и продажу (asks).
    _price_key = itemgetter(0)  # Кэшируем itemgetter заранее (для оптимизации)

    def __init__(self, symbol: str):
        self.symbol = symbol.upper()
        self._lock = threading.RLock()

        self._bids: Dict[float, float] = {}
        self._asks: Dict[float, float] = {}
        self._last_update_id: Optional[int] = None

    # --------------------------- Utilities ---------------------------
    @staticmethod
    def _parse_price_qty(price_str: str, qty_str: str) -> Tuple[float, float]:
        #string ['89384.80', '0.026'] -> tuple [89384.80, 0.026]
        try:
            return float(price_str), float(qty_str)
        except (ValueError, TypeError):
            return 0.0, 0.0

    # ---------------- Snapshot & Updates ----------------
    def load_snapshot(self, bids: List[List[str]], asks: List[List[str]], last_update_id: int) -> None:
        #записывает новые данные, полученные через REST API
        #bids&asks: [['89384.80', '0.026'], ['89384.70', '0.020'], ['89384.60', '0.002'], ..]
        new_bids = {} 
        new_asks = {}

        for price, qty in bids:
            p, q = self._parse_price_qty(price, qty)
            if q > 0: new_bids[p] = q

        for price, qty in asks:
            p, q = self._parse_price_qty(price, qty)
            if q > 0: new_asks[p] = q

        with self._lock:
            self._bids = new_bids
            self._asks = new_asks
            self._last_update_id = last_update_id

    def apply_deltas(self, bid_deltas: List[List[str]], ask_deltas: List[List[str]], last_update_id: int) -> None:
        #Принимает изменения (диффы) из WebSocket
        #Если пришел объем 0, цена удаляется из стакана; если больше 0 — обновляется.
        prepared_bids = [self._parse_price_qty(price, qty) for price, qty in bid_deltas]
        prepared_asks = [self._parse_price_qty(price, qty) for price, qty in ask_deltas]

        with self._lock:
            for price, qty in prepared_bids:
                if qty == 0: self._bids.pop(price, None)
                else: self._bids[price] = qty

            for price, qty in prepared_asks:
                if qty == 0: self._asks.pop(price, None)
                else: self._asks[price] = qty

            self._last_update_id = last_update_id

    # ---------------- Queries ----------------
    def get_top_levels(self, n: int) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
        #Возвращает топ N уровней стакана: bids (самые дорогие), asks (самые дешевые).
        with self._lock:
            bids_top = heapq.nlargest(n, self._bids.items(), key=self._price_key) #покупки
            asks_top = heapq.nsmallest(n, self._asks.items(), key=self._price_key)#продажи
            return bids_top, asks_top
        
    def get_dom_snapshot(self, L: int = 50) -> DOMSnapshot:
        #DOM-снимок: топ-L уровней на сторону + mid/spread, всё потокобезопасно.
        bids, asks = self.get_top_levels(L)

        best_bid = bids[0][0] if bids else 0.0
        best_ask = asks[0][0] if asks else 0.0
        mid = (best_bid + best_ask) / 2.0 
        # Оптимизация математики в цикле: деление — дорогая операция. 
        # Вычисляем один раз коэффициент для умножения. Вместо ((px - mid) / mid)
        inv_mid_coefficient = 1.0 / mid if mid > 0 else 0.0

        return {
            "symbol": self.symbol,
            "mid": mid,
            "spread": best_ask - best_bid, 
            "bids": [
                {"px": px, "qty": qty, "usd": px * qty, "rel": (px - mid) * inv_mid_coefficient}
                for px, qty in bids
            ],
            "asks": [
                {"px": px, "qty": qty, "usd": px * qty, "rel": (px - mid) * inv_mid_coefficient}
                for px, qty in asks
            ],
        }

    @staticmethod
    def _process_side(levels: List[Tuple[float, float]], impact_usd: float) -> Dict[str, float]:
        #Вычисляет сумму, стенку, цену воздействия и данные для наклона за ОДИН проход. O(n*log(n))
        #1.Считает суммарный объем в долларах для первых n уровней. Это показатель ликвидности «в моменте»
        #2.Ищет «стенку» — уровень с самым большим объемом в долларах среди первых n. Это потенциальное сопротивление или поддержка.
        #3.Оценивает «цену исполнения». Если ты захочешь купить/продать сразу на target_usd, до какой цены ты «прошьешь» стакан? По сути — оценка проскальзывания.
        #4.Линейная регрессия показывает «наклон» ликвидности: как быстро растет/падает объем в зависимости от удаления от лучшей цены.
        
        total_usd = 0.0
        max_usd = -1.0
        wall_px = 0.0
        impact_px = levels[-1][0] if levels else 0.0
        impact_found = False

        # Для наклона (регрессии)
        sum_x = 0.0         #сумма индексов: 0, 1, 2...
        sum_y = 0.0         #сумма объемов
        sum_xy = 0.0        #сумма произведений индекса на объем
        sum_xx = 0.0        #сумма квадратов индексов

        # Мы работаем ровно с тем количеством уровней, которое пришло
        m = len(levels)

        for i in range(m):
            p, q = levels[i]
            usd = p * q
            # 1. Сумма
            total_usd += usd
            # 2. Стенка
            if usd > max_usd:
                max_usd = usd
                wall_px = p
            # 3. Impact (цена воздействия)
            if not impact_found:
                if total_usd >= impact_usd:
                    impact_px = p
                    impact_found = True
            # 4. Накопление данных для Slope (y = usd, x = i)
            sum_x += i              #sum_indices
            sum_y += usd            #sum_volumes
            sum_xy += i * usd       #sum_index_times_volume
            sum_xx += i * i         #sum_index_squared
            
        # Считаем наклон (Slope)
        if m > 1:
            # Формула линейной регрессии: (n*sum(xy) - sum(x)*sum(y)) / (n*sum(x^2) - sum(x)^2)
            numerator = (m * sum_xy) - (sum_x * sum_y)
            denominator = (m * sum_xx) - (sum_x**2)
            slope = numerator / denominator if denominator != 0 else 0.0
        else:
            slope = 0.0
            
        return {
            "sum": total_usd,
            "wall_px": wall_px,
            "wall_usd": max_usd,
            "impact_px": impact_px,
            "slope": slope
        }
    
    def get_features_usd(self, n: int = 100, impact_usdt: float = 10_000) -> Dict[str, float]:
        #Главный диспетчер. Генерит фичи из данных о стакане
        
        # Получаем данные из стакана (уже отсеченные до n под замком)
        bids, asks = self.get_top_levels(n)

        # 1. Считаем базовые параметры спреда
        best_bid = bids[0][0] if bids else 0.0
        best_ask = asks[0][0] if asks else 0.0
        mid = (best_bid + best_ask) / 2.0 
        spread = best_ask - best_bid
        # Относительный спред в базисных пунктах (1 bps = 0.01%)
        # Это одна из самых важных фичей для оценки стоимости входа/выхода
        rel_spread_bps = (spread / mid) * 10000 if mid > 0 else 0.0 #BPS_CONVERSION = 10_000
        
        # 2. Обрабатываем каждую сторону за один проход
        bid_features = self._process_side(bids, impact_usdt)
        ask_features = self._process_side(asks, impact_usdt)
        
        total_vol = bid_features["sum"] + ask_features["sum"]
        imbalance = (bid_features["sum"] - ask_features["sum"]) / total_vol if total_vol > 0 else 0.0
        
        return {
            "sum_bid_n_usd": round(bid_features["sum"], 6),
            "sum_ask_n_usd": round(ask_features["sum"], 6),
            "cum_imbalance_n_usd": float(imbalance),
            "slope_bid_n_usd": float(bid_features["slope"]),
            "slope_ask_n_usd": float(ask_features["slope"]),
            "wall_bid_px": bid_features["wall_px"],
            "wall_bid_usd": round(bid_features["wall_usd"], 6),
            "wall_ask_px": ask_features["wall_px"],
            "wall_ask_usd": round(ask_features["wall_usd"], 6),
            "impact_buy_px": ask_features["impact_px"],  
            "impact_sell_px": bid_features["impact_px"],
            "mid_price": float(mid),
            "spread_usd": float(spread),
            "rel_spread_bps": float(rel_spread_bps), # Относительный спред
        }


class _TokenOrderBookWorker(threading.Thread):
    #One worker per symbol: REST snapshot + WS diffs, sequence handling, resync.
    #«Рабочий», который отвечает за сетевое взаимодействие для конкретной монеты (подключение к сокету, загрузка снимка, синхронизация).
    def __init__(self, symbol: str, orderbook: TokenOrderBook, session: Optional[requests.Session] = None, verbose: bool = False):
        super().__init__(name=f"OrderBookWorker-{symbol.upper()}", daemon=True)
        self.symbol = symbol.lower()
        self.sym_u = symbol.upper()
        self.book = orderbook
        self._verbose = verbose
        self._session = session or requests.Session()

        self._stop_event = threading.Event() 
        self._is_synced = False     # "флаг-переключатель" -> после API снэпшота идут WS диффы

        self._buffer_lock = threading.Lock()
        self._buffer: List[Dict] = []
        self._prev_u: int = 0

        self._ws: Optional[WebSocketApp] = None

    def stop(self):
        self._stop_event.set()
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass

    def _on_message(self, ws, message: str):
        try:
            if '"depthUpdate"' not in message: 
                return
            
            data = orjson.loads(message)

            if not self._is_synced:     # Состояние SYNCING: просто копим в буфер
                with self._buffer_lock:
                    self._buffer.append(data)
            else:                       # Состояние LIVE: применяем мгновенно
                self._process_event(data)

        except Exception as e:
            self._handle_error(f"OnMessage Error: {e}")

    def _process_event(self, evt: Dict, is_first_after_sync: bool = False):
        """Проверка последовательности и применение дельты."""
        u = int(evt["u"])               #finalUpdateId
        pu = int(evt.get("pu", -1))     #prevFinalUpdateId --> should be (u - 1)

        if not is_first_after_sync:
            if self._prev_u != 0 and pu != self._prev_u:
                self._handle_error(f"Data gap detected! Expected pu={self._prev_u} -> but got pu={pu}")
                return

        # Накатываем изменения
        self.book.apply_deltas(evt["b"], evt["a"], u) 
        self._prev_u = u    #Запоминаем текущий u как "предыдущий" для следующего сообщения

    def _handle_error(self, reason: str):
        if self._verbose:
            print(f"[{self.name}] {reason}")
        self._is_synced = False
        if self._ws:
            self._ws.close() # Это спровоцирует перезапуск в run()

    def run(self):
        """Основной цикл жизни воркера."""
        while not self._stop_event.is_set():
            try:
                self._establish_connection()
            except Exception as e:
                if self._verbose:
                    print(f"[{self.name}] Connection failed: {e}. Retry in 1s...")
                time.sleep(1)

    def _establish_connection(self):
        """Логика запуска и синхронизации."""
        self._is_synced = False
        self._buffer = []
        self._prev_u = 0
        
        ws_url = f"{BINANCE_FUTURES_WS}/{self.symbol}@depth"
        self._ws = WebSocketApp(
            ws_url,
            on_message=self._on_message,
            on_error=lambda ws, e: print(f"WS Error: {e}"),
            on_close=lambda ws, c, r: print("WS Closed")
        )

        # Запускаем WS в текущем потоке воркера (через run_forever)
        # Нам не нужен отдельный ws_thread, так как run() уже в своем потоке!
        # Но чтобы выполнить синхронизацию ПАРАЛЛЕЛЬНО приему данных, 
        # нам нужно запустить синхронизацию в отдельном маленьком потоке
        threading.Thread(target=self._sync_sequence, daemon=True).start()
        
        self._ws.run_forever(ping_interval=15, ping_timeout=10)

    def _sync_sequence(self):
        """Фоновая синхронизация с логикой ожидания стрима.Run once on init, or later on reconnect if traffic lags accured"""
        try:
            # 1. Ждем, пока WebSocket вообще начнет получать данные (проверка жизни)
            for _ in range(50): 
                if self._buffer: break
                time.sleep(0.1) 

            if not self._buffer:
                raise Exception("WebSocket is not receiving data (buffer empty).")
            
            # 2. Получаем REST Snapshot
            snap = self._get_rest_snapshot()
            last_id = snap["lastUpdateId"]
            
            # 3. ЛОГИКА ДОГОНЯЛОК: Ждем, пока WebSocket добежит до ID снимка
            # Даем стриму до 5 секунд, чтобы он прислал нужный ID
            start_wait = time.time()
            caught_up = False
            while time.time() - start_wait < 5.0:
                with self._buffer_lock:
                    if self._buffer and int(self._buffer[-1]["u"]) >= last_id:
                        caught_up = True
                        break
                time.sleep(0.1)

            if not caught_up:
                raise Exception(f"WebSocket is lagging. Stream max ID < Snapshot ID ({last_id})")
            
            # 4. Стыковка
            with self._buffer_lock:
                # Загружаем в книгу
                self.book.load_snapshot(snap["bids"], snap["asks"], last_id)
                self._prev_u = last_id

                found_bridge = False 
                for evt in self._buffer:
                    u = int(evt["u"])   #finalUpdateId
                    U = int(evt["U"])   #firstUpdateId
                    
                    if U <= last_id <= u:
                        # ПЕРВЫЙ пакет (мост) - передаем True
                        self._process_event(evt, is_first_after_sync=True)
                        found_bridge = True
                    elif found_bridge:
                        # Все последующие события просто накатываем по цепочке
                        self._process_event(evt, is_first_after_sync=False)
                
                if not found_bridge:
                    # Если мы здесь, значит Snapshot ID оказался МЕНЬШЕ, чем самое старое событие в буфере
                    raise Exception(f"Sync Bridge not found. Snapshot is too OLD (Buffer starts after Snapshot).")
                
                self._buffer = []
                self._is_synced = True # ПЕРЕКЛЮЧАТЕЛЬ: теперь on_message работает LIVE
                
            if self._verbose:
                print(f"[{self.name}] Sync successful. Mode: LIVE. LastId: {last_id}")
                
        except Exception as e:
            self._handle_error(f"Sync failed: {e}")

    def _get_rest_snapshot(self) -> Dict:
        url = f"{BINANCE_FUTURES_API}/fapi/v1/depth"
        params = {"symbol": self.sym_u, "limit": REST_DEPTH_LIMIT}
        r = self._session.get(url, params=params, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        return r.json()

# ------- per-symbol state for manager -------
@dataclass
class _TokenState:
    book: TokenOrderBook
    worker: _TokenOrderBookWorker
    last_access_ts: float   # updated ONLY on touch()

class TokenOrderBooksManager:
    #Высокоуровневый интерфейс. Он управляет списком всех отслеживаемых монет и автоматически удаляет те, 
    #которыми давно не интересовались (Auto-eviction).
    def __init__(self, auto_evict_sec: int = AUTO_EVICT_SEC):
        self._states: Dict[str, _TokenState] = {}
        self._lock = threading.RLock()
        self._session = requests.Session()
        self._stop = threading.Event()
        self._auto_evict_sec = int(auto_evict_sec)
        self._gc_thread = threading.Thread(target=self._gc_loop, daemon=True, name="DepthGC")

    # ---------------- Lifecycle ----------------
    def touch(self, symbol: str) -> None:
        sym_l = symbol.lower()
        sym_u = symbol.upper()
        now = time.time()
        with self._lock:
            st = self._states.get(sym_l)
            if st is not None:
                st.last_access_ts = now  # TTL продлеваем ТОЛЬКО здесь
                return
            book = TokenOrderBook(sym_u)
            print(f"🚀 Starting Depth stream for {sym_u}")
            worker = _TokenOrderBookWorker(sym_l, book, session=self._session)
            self._states[sym_l] = _TokenState(book=book, worker=worker, last_access_ts=now)
            worker.start()

    def start(self):
        self._gc_thread.start()

    def stop(self, symbol: Optional[str] = None) -> None:
        with self._lock:
            if symbol is None:
                for st in list(self._states.values()):
                    st.worker.stop()
                self._states.clear()
                self._stop.set()
                return
            sym_l = symbol.lower()
            st = self._states.pop(sym_l, None)
            if st:
                st.worker.stop()

    # ---------------- GC / авто-эвикшн ----------------
    def _gc_loop(self):
        while not self._stop.is_set():
            time.sleep(GC_INTERVAL_SEC)
            if self._auto_evict_sec <= 0:
                continue
            deadline = time.time() - self._auto_evict_sec
            expired: List[str] = []
            with self._lock:
                for sym, st in list(self._states.items()):
                    if st.last_access_ts < deadline:
                        try:
                            st.worker.stop()
                        except Exception:
                            pass
                        expired.append(sym)
                        del self._states[sym]
            for sym in expired:
                print(f"⏹️ Depth GC: stopped {sym.upper()} (idle > {self._auto_evict_sec}s)")

    # ---------------- Queries (без продления TTL) ----------------
    def list_symbols(self) -> List[str]:
        with self._lock:
            return list(self._states.keys())

    def get_dom_snapshot(self, symbol: str, L: int = 20) -> Dict[str, object]:
        sym_l = symbol.lower()
        with self._lock:
            st = self._states.get(sym_l)
            if not st:
                return {}
            return st.book.get_dom_snapshot(L=L)

    def get_all_dom(self, L: int = 20, symbols: Optional[List[str]] = None) -> Dict[str, Dict[str, object]]:
        out: Dict[str, Dict[str, object]] = {}
        with self._lock:
            keys = [s.lower() for s in (symbols or self._states.keys())]
            for sym in keys:
                st = self._states.get(sym)
                if not st:
                    continue
                out[sym.lower()] = st.book.get_dom_snapshot(L=L)
        return out

    def get_features(self, symbol: str, n: int = 100, impact_usdt: float = 10_000) -> Dict[str, float]:
        sym_l = symbol.lower()
        with self._lock:
            st = self._states.get(sym_l)
            if not st:
                return {}
            return st.book.get_features_usd(n=n, impact_usdt=impact_usdt)

    def get_all_features(self, n: int = 100, impact_usdt: float = 10_000, symbols: Optional[List[str]] = None) -> Dict[str, Dict[str, float]]:
        out: Dict[str, Dict[str, float]] = {}
        with self._lock:
            keys = [s.lower() for s in (symbols or self._states.keys())]
            for sym in keys:
                st = self._states.get(sym)
                if not st:
                    continue
                out[sym.lower()] = st.book.get_features_usd(n=n, impact_usdt=impact_usdt)
        return out
    
"""
# --------------------------- Minimal self-test ---------------------------
if __name__ == "__main__":
    mgr = DepthBooksManager(AUTO_EVICT_SEC)
    mgr.touch("btcusdt")
    print("✅Started depth workers for BTC. Gathering data for ~2s...")

    def bnb():
        time.sleep(1.0)
        mgr.touch("bnbusdt")
        print("✅Started depth workers for BNB. Gathering data for ~2s...")
    
    def eth():
        time.sleep(2.5)
        mgr.touch("ethusdt")
        print("✅Started depth workers for eth. Gathering data for ~2s...")

    def AVAAIUSDT():
        time.sleep(4.0)
        mgr.touch("AVAAIUSDT")
        print("✅Started depth workers for avaai. Gathering data for ~2s...")

    def REZUSDT():
        time.sleep(6.0)
        mgr.touch("REZUSDT")
        print("✅Started depth workers for rez. Gathering data for ~2s...")

    def PORT3USDT():
        time.sleep(8.0)
        mgr.touch("PORT3USDT")
        print("✅Started depth workers for port3. Gathering data for ~2s...")


    # threading.Thread(target=bnb, daemon=True).start()
    # threading.Thread(target=eth, daemon=True).start()
    # threading.Thread(target=AVAAIUSDT, daemon=True).start()
    # threading.Thread(target=REZUSDT, daemon=True).start()
    # threading.Thread(target=PORT3USDT, daemon=True).start()

    try:
        while True:
            #batch = mgr.get_all_features(n=1000, impact_usdt=10_000)
            batch = mgr.get_all_dom()
            print(batch)
            time.sleep(1)
            
    except KeyboardInterrupt:
        mgr.stop()
"""

if __name__ == "__main__":
    # 1. Создаем объект стакана для BTC
    btc_book = TokenOrderBook("BTCUSDT") 
    
    # 2. Создаем воркера
    # Параметр verbose=True поможет нам видеть логи синхронизации
    worker = _TokenOrderBookWorker(
        symbol="BTCUSDT", 
        orderbook=btc_book, 
        verbose=True
    )

    print("🚀 Запуск воркера... Ждем синхронизации (около 2 сек)...")
    worker.start()

    try:
        # 3. Цикл мониторинга
        while True:
            time.sleep(1) # Раз в секунду выводим данные
            
            # Если стакан еще не синхронизирован, пропускаем
            if not worker._is_synced:
                continue
                
            # Получаем фичи (impact на 10,000 USDT)
            stats = btc_book.get_features_usd(n=100, impact_usdt=10_000)
            
            # Красивый вывод в консоль
            print("-" * 50)
            print(f"SYMBOL: BTCUSDT | LIVE DATA")
            print(f"Mid Price: {stats['mid_price']:.2f} | Spread: {stats['rel_spread_bps']:.2f} bps")
            print(f"Imbalance: {stats['cum_imbalance_n_usd']:.2%}")
            print(f"Slopes: Bid {stats['slope_bid_n_usd']:.4f} | Ask {stats['slope_ask_n_usd']:.4f}")
            print(f"Walls: Buy {stats['wall_bid_px']} ({stats['wall_bid_usd']:.0f} USD) | "
                  f"Sell {stats['wall_ask_px']} ({stats['wall_ask_usd']:.0f} USD)")
            
    except KeyboardInterrupt:
        print("\n🛑 Останавливаем воркер...")
        worker.stop()
        worker.join()
        print("✅ Тест завершен.")
