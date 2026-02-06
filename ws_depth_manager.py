# file: ws_depth_manager.py
import requests
from websocket import WebSocketApp
import threading
import time
import math
import numpy as np
import heapq
import orjson
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional, TypedDict
from config import Config
from operator import itemgetter

# --- Config ---
BINANCE_FUTURES_WS = "wss://fstream.binance.com/ws"
BINANCE_FUTURES_API = "https://fapi.binance.com"
REST_DEPTH_LIMIT = 1000 #max, possible: 500, 200, 100
CONNECT_TIMEOUT = 10 #30 if heavy, mb
HTTP_TIMEOUT = 5

# --- Manager params ---
AUTO_EVICT_SEC = Config.TTL_SECONDS
GC_INTERVAL_SEC = 10

# --- AI Params ---
AI_TOP_N = Config.AI_TOP_N                  # Кол-во лучших уровней цен, которые передаются точно
AI_TAIL_BINS = Config.AI_TAIL_BINS          # Кол-во «корзин» для дальних уровней.
AI_TAIL_MAX_BPS = Config.AI_TAIL_MAX_BPS    # Хвост охватывает 5% движения цены

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
    timestamp: float

class AISnapshot(TypedDict):
    # Готовые numpy массивы для упаковщика
    top_bid_px: np.ndarray
    top_bid_qty: np.ndarray
    top_ask_px: np.ndarray
    top_ask_qty: np.ndarray
    tail_bid_qty: np.ndarray
    tail_ask_qty: np.ndarray
    depth_feats: np.ndarray # [mid, spread, microprice, imb@k, bid_qty_k, ask_qty_k]
    extra_feats: Dict[str, float] # slope, wall, etc.
    timestamp: float

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

     # Предварительный расчет лог-шкалы для хвостов
        # Если tail_bins=32, мы хотим покрыть tail_max_bps логарифмически
        if AI_TAIL_BINS > 0:
            # log(1 + x) шкала. 
            # max_log = log(1 + 50) ≈ 3.93
            # scale = 32 / 3.93
            self._log_scale = AI_TAIL_BINS / math.log(1.0 + AI_TAIL_MAX_BPS)
        else:
            self._log_scale = 0.0

    # ---------------- Snapshot & Updates ----------------
    def load_snapshot(self, bids: List[List[str]], asks: List[List[str]], last_update_id: int) -> None:
        #записывает новые данные, полученные через REST API
        #bids&asks: [['89384.80', '0.026'], ['89384.70', '0.020'], ['89384.60', '0.002'], ..]
        new_bids = {float(price): float(qty) for price, qty in bids if float(qty) > 0}
        new_asks = {float(price): float(qty) for price, qty in asks if float(qty) > 0}
        with self._lock:
            self._bids = new_bids
            self._asks = new_asks
            self._last_update_id = last_update_id

    def apply_deltas(self, bid_deltas: List[List[str]], ask_deltas: List[List[str]], last_update_id: int) -> None:
        #Принимает изменения (диффы) из WebSocket
        #Если пришел объем 0, цена удаляется из стакана; если больше 0 — обновляется.
        with self._lock:
            for price_str, qty_str in bid_deltas:
                price, qty = float(price_str), float(qty_str)
                if qty == 0: self._bids.pop(price, None)
                else: self._bids[price] = qty

            for price_str, qty_str in ask_deltas:
                price, qty = float(price_str), float(qty_str)
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
    
    def _calc_tail_bins_log(self, orders: Dict[float, float], mid: float, exclude_top_px: float, is_bid: bool) -> np.ndarray:
        """
        Быстрый расчет хвостов БЕЗ полной сортировки. 
        Проходим по всем ордерам словаря. Если цена за пределами топа - кидаем в бин.
        """
        bins = np.zeros(AI_TAIL_BINS, dtype=np.float32)
        if mid <= 0: return bins

        # Для Bid: цена < exclude_top_px. Дистанция = (mid - px) / mid
        # Для Ask: цена > exclude_top_px. Дистанция = (px - mid) / mid
        
        for price, qty in orders.items():
            # Фильтр: берем только те, что хуже Top-N цены
            if is_bid:
                if price >= exclude_top_px: continue
                delta_bps = (mid - price) / mid * 10000.0
            else:
                if price <= exclude_top_px: continue
                delta_bps = (price - mid) / mid * 10000.0
            
            if delta_bps <= 0: continue # Ошибка или пересечение спреда
            
            # Логарифмический биннинг: idx = log(1 + bps) * scale
            # +1 чтобы избежать log(0) и сгладить начало
            idx = int(math.log(1.0 + delta_bps) * self._log_scale)
            
            if 0 <= idx < AI_TAIL_BINS:
                bins[idx] += qty
            elif idx >= AI_TAIL_BINS:
                # Все, что дальше max_bps, падает в последний бин (или игнорируется, по вкусу)
                bins[AI_TAIL_BINS - 1] += qty
                
        return bins

    def get_ai_snapshot(self) -> Optional[AISnapshot]:
        """
        Генерирует готовые numpy-массивы для нейронки за один вызов лока.
        Объединяет логику Sampler и Feature extraction.
        """
        ts = time.time()
        with self._lock:
            if not self._bids or not self._asks:
                return None
            
            # 1. Top-N (самая дорогая операция - сортировка)
            # Берем N лучших цен
            top_bids = heapq.nlargest(AI_TOP_N, self._bids.items()) # [(px, qty), ...]
            top_asks = heapq.nsmallest(AI_TOP_N, self._asks.items())
            
            if not top_bids or not top_asks: return None

            best_bid_px = top_bids[0][0]
            best_ask_px = top_asks[0][0]
            
            # Защита от перекрещенного стакана
            if best_bid_px >= best_ask_px:
                mid = best_bid_px
            else:
                mid = (best_bid_px + best_ask_px) / 2.0

            # 2. Заполняем Top-N массивы
            t_bid_px = np.zeros(AI_TOP_N, dtype=np.float32)
            t_bid_qty = np.zeros(AI_TOP_N, dtype=np.float32)
            t_ask_px = np.zeros(AI_TOP_N, dtype=np.float32)
            t_ask_qty = np.zeros(AI_TOP_N, dtype=np.float32)

            for i, (p, q) in enumerate(top_bids):
                t_bid_px[i], t_bid_qty[i] = p, q
            for i, (p, q) in enumerate(top_asks):
                t_ask_px[i], t_ask_qty[i] = p, q

            # 3. Считаем хвосты (Tail Bins)
            # Передаем цену отсечения (последняя цена топа)
            cutoff_bid = top_bids[-1][0]
            cutoff_ask = top_asks[-1][0]
            
            tail_bids = self._calc_tail_bins_log(self._bids, mid, cutoff_bid, is_bid=True)
            tail_asks = self._calc_tail_bins_log(self._asks, mid, cutoff_ask, is_bid=False)

            # 4. Считаем Фичи (Fast Feats + Extra Feats)
            # --- Fast Feats (для вектора) ---
            spread = best_ask_px - best_bid_px
            
            # Microprice
            bb_qty = top_bids[0][1]
            ba_qty = top_asks[0][1]
            micro = (best_ask_px * bb_qty + best_bid_px * ba_qty) / (bb_qty + ba_qty) if (bb_qty+ba_qty) > 0 else mid
            
            # Imbalance @ K (например, по топ-10)
            k_imb = min(10, AI_TOP_N)
            sum_bid_k = np.sum(t_bid_qty[:k_imb])
            sum_ask_k = np.sum(t_ask_qty[:k_imb])
            den_k = sum_bid_k + sum_ask_k
            imb_k = (sum_bid_k - sum_ask_k) / den_k if den_k > 0 else 0.0

            depth_feats = np.array([mid, spread, micro, imb_k, sum_bid_k, sum_ask_k], dtype=np.float32)

            # --- Extra Feats (Wall, Slope - старая логика, можно упростить) ---
            # Для упрощения возьмем полные суммы топа
            sum_bid_N_usd = float(np.sum(t_bid_px * t_bid_qty))
            sum_ask_N_usd = float(np.sum(t_ask_px * t_ask_qty))
            
            # Простой расчет Imbalance по всему Top-N в USD
            total_vol = sum_bid_N_usd + sum_ask_N_usd
            cum_imb = (sum_bid_N_usd - sum_ask_N_usd) / total_vol if total_vol > 0 else 0.0

            extra_feats = {
                "mid_price": mid, # дубль, но пусть будет для совместимости
                "cum_imbalance_n_usd": cum_imb,
                "sum_bid_n_usd": sum_bid_N_usd,
                "sum_ask_n_usd": sum_ask_N_usd,
                # Slopes и Walls можно считать тут же, если они критичны
                # Но для скорости пока оставим базовые
            }

            return {
                "top_bid_px": t_bid_px,
                "top_bid_qty": t_bid_qty,
                "top_ask_px": t_ask_px,
                "top_ask_qty": t_ask_qty,
                "tail_bid_qty": tail_bids,
                "tail_ask_qty": tail_asks,
                "depth_feats": depth_feats,
                "extra_feats": extra_feats,
                "timestamp": ts
            }


class TokenOrderBookWorker(threading.Thread):
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
        self._buffer = []
        self._prev_u: int = 0

        self._ws: Optional[WebSocketApp] = None

    def stop(self):
        self._stop_event.set()
        if self._ws:
            try: self._ws.close()
            except: pass

    def _on_message(self, ws, message: str):
        try:
            if '"depthUpdate"' not in message: return
            
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

        if not is_first_after_sync and self._prev_u != 0 and pu != self._prev_u:
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
        response = self._session.get(url, params={"symbol": self.sym_u, "limit": REST_DEPTH_LIMIT}, timeout=HTTP_TIMEOUT)
        response.raise_for_status()
        return response.json()

# ------- per-symbol state for manager -------
@dataclass
class TokenState:
    book: TokenOrderBook
    worker: TokenOrderBookWorker
    last_access_ts: float   # updated ONLY on touch()


class TokenOrderBooksManager:
    #Высокоуровневый интерфейс. Он управляет списком всех отслеживаемых монет и автоматически удаляет те, 
    #которыми давно не интересовались (Auto-eviction).
    def __init__(self, auto_evict_sec: int = AUTO_EVICT_SEC):
        self._states: Dict[str, TokenState] = {}
        self._lock = threading.RLock()
        self._session = requests.Session()
        self._stop_event = threading.Event()
        self._auto_evict_sec = int(auto_evict_sec)
        self._gc_thread = threading.Thread(target=self._gc_loop, daemon=True)

    # ---------------- Lifecycle ----------------
    def touch(self, symbol: str) -> bool:
        #Гарантирует, что воркер для символа запущен. Если уже запущен — обновляет время доступа (TTL).
        sym_l = symbol.lower()
        sym_u = symbol.upper()
        now = time.time()

        with self._lock:
            tokenState = self._states.get(sym_l)
            if tokenState:
                tokenState.last_access_ts = now
                return True
            
            try:
                print(f"🚀 Starting Depth stream for {sym_u}")
                book = TokenOrderBook(sym_u)
                worker = TokenOrderBookWorker(sym_l, book, self._session, verbose=True)
                self._states[sym_l] = TokenState(book, worker, now)
                worker.start()
                return True
            except Exception as e:
                print(f"❌ Failed to start worker for {sym_u}: {e}")
                return False

    def start(self):
        self._gc_thread.start()

    def stop(self, symbol: Optional[str] = None) -> None:
        with self._lock:
            if symbol is None: # stop_all
                for state in self._states.values():
                    state.worker.stop()
                self._states.clear()
                self._stop_event.set()
                return
            #stop token 
            sym_l = symbol.lower()
            state = self._states.pop(sym_l, None)
            if state: state.worker.stop()

    # ---------------- GC / Auto-eviction ----------------
    def _gc_loop(self):
        while not self._stop_event.is_set():
            time.sleep(GC_INTERVAL_SEC)
            if self._auto_evict_sec <= 0: continue

            deadline = time.time() - self._auto_evict_sec
            with self._lock:
                for token, state in list(self._states.items()):
                    if state.last_access_ts < deadline:
                        print(f"⏹️ Depth GC: stopped {token.upper()} due to inactivity...")
                        try:
                            state.worker.stop()
                            del self._states[token]
                        except Exception as e:
                            print("Error on GC stop (TTL eviction failed):", e)
                            pass

    # ---------------- Queries ----------------
    def get_all_doms(self, n: int = 100, tokens: Optional[List[str]] = None) -> Dict[str, Dict[str, object]]:
        #снэп всех стаканов(default), можно передать список токенов
        out: Dict[str, Dict[str, object]] = {}
        current_time = time.time()

        # 1. Быстро забираем список нужных нам объектов воркеров
        with self._lock:
            if tokens:
                # Фильтруем только те, что есть в наличии
                target_states = [(s.upper(), self._states.get(s.lower())) for s in tokens]
                target_states = [(token, state) for token, state in target_states if state]
            else:
                target_states = [(token.upper(), state) for token, state in self._states.items()]

        # 2. Выполняем тяжелое копирование данных ВНЕ лока менеджера
        for token, state in target_states:
            # Безопасность: пропускаем, если данных еще нет
            if not state.worker._is_synced:
                continue
            
            dom = state.book.get_dom_snapshot(L=n)
            dom['timestamp'] = current_time # Полезно знать, когда сделан слепок
            out[token] = dom
            
        return out

    def get_all_market_data(self, n: int = 100, impact_usdt: float = 10_000) -> Dict[str, Dict]:
        # ГЛАВНЫЙ МЕТОД ДЛЯ ОРКЕСТРАТОРА. Собирает фичи по ВСЕМ активным монетам за один проход
        snapshot = {}
        current_time = time.time()
        
        with self._lock:
            active_tokens = list(self._states.items())
            
        for token, state in active_tokens:
            # Если воркер еще не синхронизировался (нет данных), пропускаем
            if not state.worker._is_synced: continue
                
            # Получаем фичи из книги
            features = state.book.get_features_usd(n=n, impact_usdt=impact_usdt)
            features['timestamp'] = current_time    # Добавляем метку времени для контроля актуальности
            snapshot[token.upper()] = features
            
        return snapshot
    
    def get_all_ai_data(self) -> Dict[str, AISnapshot]:
        #Собирает готовые данные для модели без лишней обработки
        out = {}
        with self._lock:
            states = list(self._states.items())
        
        for token, state in states:
            if not state.worker._is_synced: continue
            # Вся магия теперь внутри book
            data = state.book.get_ai_snapshot()
            if data:
                out[token.upper()] = data
        return out
    
"""    
# --------------------------- Minimal self-test ---------------------------
#test TokenOrderBooksManager (1)
if __name__ == "__main__":
    mgr = TokenOrderBooksManager(AUTO_EVICT_SEC)
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


    threading.Thread(target=bnb, daemon=True).start()
    threading.Thread(target=eth, daemon=True).start()
    threading.Thread(target=AVAAIUSDT, daemon=True).start()
    threading.Thread(target=REZUSDT, daemon=True).start()

    try:
        while True:
            batch = mgr.get_all_doms()
            #batch = mgr.get_all_market_data()
            print(batch)
            time.sleep(1)
            
    except KeyboardInterrupt:
        mgr.stop()
"""
"""
#test TokenOrderBooksManager (2)
if __name__ == "__main__":
    manager = TokenOrderBooksManager(auto_evict_sec=60)
    
    # Список из 20 монет (пример)
    symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT", "AVAXUSDT", "DOTUSDT", "LINKUSDT", "MATICUSDT"]
    
    for s in symbols:
        manager.touch(s)
        
    try:
        while True:
            # Вот так будет работать оркестратор:
            data = manager.get_all_market_data(n=100)
            
            print(f"--- Snapshot at {time.strftime('%H:%M:%S')} ---")
            print(f"Active workers: {len(data)} / {len(symbols)}")
            
            for sym, feats in data.items():
                print(f"{sym}: Price {feats['mid_price']:.2f} | Imb: {feats['cum_imbalance_n_usd']:.2%}")
                
            time.sleep(1) # Тот самый секундный интервал
    except KeyboardInterrupt:
        manager.stop()
"""

"""
#test _TokenOrderBookWorker
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
"""