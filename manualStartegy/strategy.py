# file: strategy.py
from __future__ import annotations
from dataclasses import dataclass, field
from collections import deque
from typing import Deque, Dict, Optional, List
import math, time

# ===== ВСПОМОГАТЕЛЬНЫЕ ОНЛАЙН-МЕТРИКИ =====
class EMA:
    def __init__(self, alpha: float, init: Optional[float] = None):
        self.a = alpha
        self.v = init
    def update(self, x: float) -> float:
        if self.v is None:
            self.v = x
        else:
            self.v = self.a * x + (1 - self.a) * self.v
        return self.v

class EWMVar:
    """Экспоненциальная оценка дисперсии (для z-score объёма)."""
    def __init__(self, alpha: float):
        self.a = alpha
        self.mean = None
        self.var = 0.0
    def update(self, x: float) -> float:
        if self.mean is None:
            self.mean = x
            self.var = 0.0
            return 0.0
        prev_mean = self.mean
        self.mean = self.a * x + (1 - self.a) * self.mean
        # дисперсия по формуле для эксп. взвешивания
        self.var = self.a * (x - prev_mean) ** 2 + (1 - self.a) * self.var
        std = math.sqrt(max(self.var, 1e-12))
        return (x - self.mean) / std if std > 0 else 0.0

class ATR1s:
    """ATR на секундных барах с эксп. сглаживанием."""
    def __init__(self, alpha: float = 0.2):
        self.ema = EMA(alpha)
        self.prev_close = None
    def update(self, high: float, low: float, close: float) -> float:
        if self.prev_close is None:
            tr = high - low
        else:
            tr = max(high - low, abs(high - self.prev_close), abs(low - self.prev_close))
        self.prev_close = close
        return self.ema.update(tr)

@dataclass
class Bar:
    ts: float
    open: float
    high: float
    low: float
    close: float
    volume: float
    trades: int

@dataclass
class Position:
    side: str  # 'long' or 'short'
    entry: float
    size: float
    best: float
    ts_open: float

@dataclass
class ConfigStrategy:
    # пороги
    MOM3_TH: float = 0.0010      # ~0.10% за 3 секунды
    VOL_Z_TH: float = 1.5        # z-score объёма
    EMA_ALPHA: float = 0.3       # EMA по цене
    VOL_ALPHA: float = 0.15      # EWMVar по объёму
    ATR_ALPHA: float = 0.2       # ATR сглаживание

    # риск
    TP_PCT: float = 0.004        # 0.4%
    SL_PCT: float = 0.0025       # 0.25%
    TTL_SEC: int = 45
    TRAIL_PCT: float = 0.003     # 0.3%
    SIZE_USD: float = 100.0      # бумажный размер позиции в USD
    allow_short: bool = False

# ===== ОСНОВНОЙ ДВИЖОК =====
class StrategyEngine:
    def __init__(self, cfg: ConfigStrategy = ConfigStrategy()):
        self.cfg = cfg
        self.state: Dict[str, Dict[str, object]] = {}
        # храним последние close для момента
        self.buffers: Dict[str, Deque[Bar]] = {}

    def _ensure(self, sym: str):
        if sym in self.state:
            return
        self.state[sym] = {
            'ema_price': EMA(self.cfg.EMA_ALPHA),
            'vol_z': EWMVar(self.cfg.VOL_ALPHA),
            'atr': ATR1s(self.cfg.ATR_ALPHA),
            'pos': None,  # type: Optional[Position]
            'pnl_realized': 0.0,
        }
        self.buffers[sym] = deque(maxlen=12)  # ~12 секунд истории

    def _momentum(self, sym: str, n: int) -> float:
        buf = self.buffers[sym]
        if len(buf) < n+1:
            return 0.0
        c_now = buf[-1].close
        c_prev = buf[-(n+1)].close
        base = max(1e-9, self.state[sym]['ema_price'].v or c_prev)
        return (c_now - c_prev) / base

    def _target_size(self, price: float) -> float:
        usd = self.cfg.SIZE_USD
        if price <= 0:
            return 0.0
        return usd / price

    def on_bar(self, sym: str, bar: Bar) -> Optional[dict]:
        self._ensure(sym)
        st = self.state[sym]
        self.buffers[sym].append(bar)

        ema_p = st['ema_price'].update(bar.close)
        vol_z = st['vol_z'].update(bar.volume)
        atr = st['atr'].update(bar.high, bar.low, bar.close)

        pos: Optional[Position] = st['pos']
        now = bar.ts

        # ——— Выходы по активной позиции ———
        if pos is not None:
            if pos.side == 'long':
                # трейлинг
                pos.best = max(pos.best, bar.close)
                trail = pos.best * (1 - self.cfg.TRAIL_PCT)
                tp = pos.entry * (1 + self.cfg.TP_PCT)
                sl = pos.entry * (1 - self.cfg.SL_PCT)
                exit_price = None
                reason = None
                if bar.close <= sl:
                    exit_price = bar.close
                    reason = 'SL'
                elif bar.close >= tp:
                    exit_price = bar.close
                    reason = 'TP'
                elif bar.close <= trail:
                    exit_price = bar.close
                    reason = 'TRAIL'
                elif now - pos.ts_open >= self.cfg.TTL_SEC:
                    exit_price = bar.close
                    reason = 'TTL'
                if exit_price is not None:
                    pnl = (exit_price - pos.entry) * pos.size
                    st['pnl_realized'] += pnl
                    st['pos'] = None
                    return {
                        'type': 'exit', 'symbol': sym, 'side': pos.side,
                        'price': round(exit_price, 10), 'reason': reason,
                        'pnl': round(pnl, 6), 'pnl_total': round(st['pnl_realized'], 6)
                    }
            else:
                # SHORT (опционально)
                pos.best = min(pos.best, bar.close)
                trail = pos.best * (1 + self.cfg.TRAIL_PCT)
                tp = pos.entry * (1 - self.cfg.TP_PCT)
                sl = pos.entry * (1 + self.cfg.SL_PCT)
                exit_price = None
                reason = None
                if bar.close >= sl:
                    exit_price = bar.close
                    reason = 'SL'
                elif bar.close <= tp:
                    exit_price = bar.close
                    reason = 'TP'
                elif bar.close >= trail:
                    exit_price = bar.close
                    reason = 'TRAIL'
                elif now - pos.ts_open >= self.cfg.TTL_SEC:
                    exit_price = bar.close
                    reason = 'TTL'
                if exit_price is not None:
                    pnl = (pos.entry - exit_price) * pos.size
                    st['pnl_realized'] += pnl
                    st['pos'] = None
                    return {
                        'type': 'exit', 'symbol': sym, 'side': pos.side,
                        'price': round(exit_price, 10), 'reason': reason,
                        'pnl': round(pnl, 6), 'pnl_total': round(st['pnl_realized'], 6)
                    }

        # ——— Входы (когда позиции нет) ———
        mom3 = self._momentum(sym, 3)
        # mom10 можно использовать как фильтр тренда
        mom10 = self._momentum(sym, 10)

        enter_long = (
            pos is None and
            mom3 > self.cfg.MOM3_TH and
            vol_z > self.cfg.VOL_Z_TH and
            bar.close > ema_p
        )

        enter_short = (
            self.cfg.allow_short and pos is None and
            mom3 < -self.cfg.MOM3_TH and
            vol_z > self.cfg.VOL_Z_TH and
            bar.close < ema_p
        )

        if enter_long:
            size = self._target_size(bar.close)
            st['pos'] = Position(side='long', entry=bar.close, size=size, best=bar.close, ts_open=now)
            return {
                'type': 'entry', 'symbol': sym, 'side': 'long', 'price': round(bar.close, 10),
                'size': round(size, 6), 'mom3': round(mom3, 6), 'vol_z': round(vol_z, 3)
            }
        if enter_short:
            size = self._target_size(bar.close)
            st['pos'] = Position(side='short', entry=bar.close, size=size, best=bar.close, ts_open=now)
            return {
                'type': 'entry', 'symbol': sym, 'side': 'short', 'price': round(bar.close, 10),
                'size': round(size, 6), 'mom3': round(mom3, 6), 'vol_z': round(vol_z, 3)
            }
        
        # если ничего не сделали — можно вернуть диагностический снапшот раз в N секунд
        return None

    def snapshot(self, sym: str) -> dict:
        self._ensure(sym)
        st = self.state[sym]
        pos = st['pos']
        return {
            'symbol': sym,
            'ema_price': None if st['ema_price'].v is None else round(st['ema_price'].v, 10),
            'pnl_total': round(st['pnl_realized'], 6),
            'pos': None if pos is None else {
                'side': pos.side, 'entry': pos.entry, 'size': pos.size, 'best': pos.best,
                'age_sec': int(time.time() - pos.ts_open)
            }
        }