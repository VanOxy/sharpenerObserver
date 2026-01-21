"""
backfill.py — минимальный, но практичный модуль для sharpenerObserver

Что он делает:
1) Тянет историю свечей с Binance (Futures USDT) по REST (1s и/или 1m)
2) Строит базовые фичи (доходности, волатильности, роллинги)
3) Считает статистики нормализации (ZScore и Robust)
4) Подготавливает кольцевые буферы (ring buffers) под онлайн-стрим

Зависимости: pip install requests pandas numpy

ВНИМАНИЕ: Binance официально не даёт 1s REST для обычных контрактов, но даёт
continuous kline 1s по фьючам PERPETUAL. Если вам нужны именно 1s, используйте
continuous endpoint. Для обычных символов, используйте 1m и/или стройте 1s из
aggTrade/stream. Здесь реализованы оба варианта.
"""
from __future__ import annotations

import time
import math
import typing as T
from dataclasses import dataclass

import requests
import pandas as pd
import numpy as np

BINANCE_FAPI = "https://fapi.binance.com"

# ------------------------------ Утилиты времени ------------------------------ #

def now_ms() -> int:
    return int(time.time() * 1000)


def to_ms(ts: T.Union[int, float, pd.Timestamp, str, None]) -> T.Optional[int]:
    if ts is None:
        return None
    if isinstance(ts, (int, np.integer)):
        return int(ts)
    if isinstance(ts, float):
        return int(ts)
    if isinstance(ts, pd.Timestamp):
        return int(ts.value // 1_000_000)
    if isinstance(ts, str):
        return int(pd.Timestamp(ts).value // 1_000_000)
    raise TypeError(f"Unsupported ts type: {type(ts)}")


# ------------------------------- HTTP & Retry ------------------------------- #

class Http:
    def __init__(self, base=BINANCE_FAPI, timeout=10, max_retries=3, backoff=0.5):
        self.base = base
        self.s = requests.Session()
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff = backoff

    def get(self, path: str, params: dict) -> requests.Response:
        url = self.base + path
        last_exc = None
        for i in range(self.max_retries):
            try:
                r = self.s.get(url, params=params, timeout=self.timeout)
                if r.status_code == 429:
                    # rate limit
                    time.sleep(self.backoff * (2 ** i))
                    continue
                r.raise_for_status()
                return r
            except Exception as e:
                last_exc = e
                time.sleep(self.backoff * (2 ** i))
        raise last_exc


# ---------------------------- Загрузка свечей ------------------------------- #

# Обычные фьючерсные свечи (min granular = 1m)
# GET /fapi/v1/klines?symbol=BTCUSDT&interval=1m&endTime=...&limit=1000

def fetch_klines_futures(symbol: str, interval: str, start_ms: int | None, end_ms: int | None, limit: int = 1000, http: Http | None = None) -> pd.DataFrame:
    http = http or Http()
    params = {"symbol": symbol.upper(), "interval": interval, "limit": limit}
    if start_ms is not None:
        params["startTime"] = int(start_ms)
    if end_ms is not None:
        params["endTime"] = int(end_ms)
    resp = http.get("/fapi/v1/klines", params)
    data = resp.json()
    cols = [
        "open_time","open","high","low","close","volume",
        "close_time","quote_asset_volume","number_of_trades",
        "taker_buy_base","taker_buy_quote","ignore"
    ]
    df = pd.DataFrame(data, columns=cols)
    if df.empty:
        return df
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
    num_cols = ["open","high","low","close","volume","quote_asset_volume","number_of_trades","taker_buy_base","taker_buy_quote"]
    df[num_cols] = df[num_cols].astype(float)
    df.set_index("open_time", inplace=True)
    return df


# Непрерывные фьючи (continuous contracts) поддерживают 1s
# GET /fapi/v1/continuousKlines?pair=BTCUSDT&contractType=PERPETUAL&interval=1s

def fetch_continuous_klines(pair: str, contract_type: str, interval: str, start_ms: int | None, end_ms: int | None, limit: int = 1000, http: Http | None = None) -> pd.DataFrame:
    http = http or Http()
    params = {
        "pair": pair.upper(),
        "contractType": contract_type,  # e.g. PERPETUAL
        "interval": interval,
        "limit": limit,
    }
    if start_ms is not None:
        params["startTime"] = int(start_ms)
    if end_ms is not None:
        params["endTime"] = int(end_ms)
    resp = http.get("/fapi/v1/continuousKlines", params)
    data = resp.json()
    cols = [
        "open_time","open","high","low","close","volume",
        "close_time","quote_asset_volume","number_of_trades",
        "taker_buy_base","taker_buy_quote","ignore"
    ]
    df = pd.DataFrame(data, columns=cols)
    if df.empty:
        return df
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
    num_cols = ["open","high","low","close","volume","quote_asset_volume","number_of_trades","taker_buy_base","taker_buy_quote"]
    df[num_cols] = df[num_cols].astype(float)
    df.set_index("open_time", inplace=True)
    return df


def backfill_klines(symbol: str, *, use_continuous_1s: bool = True, hours_1s: int = 3, hours_1m: int = 48, contract_type: str = "PERPETUAL", end_time: T.Union[int, str, pd.Timestamp, None] = None) -> dict:
    """
    Тянет бэктилл 1s (continuous) и 1m (обычные фьючи) до момента end_time.
    Возвращает dict с df_1s и df_1m.
    """
    end_ms = to_ms(end_time) or now_ms()

    # 1m обычные фьючи
    start_1m = end_ms - hours_1m * 60 * 60 * 1000
    df_1m_parts = []
    cur_end = end_ms
    while cur_end > start_1m:
        chunk_start = max(start_1m, cur_end - 1000 * 60_000)  # 1000 минут назад макс батч
        part = fetch_klines_futures(symbol, "1m", start_ms=chunk_start, end_ms=cur_end, limit=1000)
        if part.empty:
            break
        df_1m_parts.append(part)
        first_ts = int(part.index[0].value // 1_000_000)
        if first_ts <= start_1m:
            break
        cur_end = first_ts - 1
    df_1m = pd.concat(df_1m_parts).sort_index().drop_duplicates()

    # 1s continuous (если нужен)
    if use_continuous_1s:
        start_1s = end_ms - hours_1s * 60 * 60 * 1000
        df_1s_parts = []
        cur_end = end_ms
        while cur_end > start_1s:
            chunk_start = max(start_1s, cur_end - 1000 * 1000)  # 1000 секунд батч
            part = fetch_continuous_klines(pair=symbol, contract_type=contract_type, interval="1s", start_ms=chunk_start, end_ms=cur_end, limit=1000)
            if part.empty:
                break
            df_1s_parts.append(part)
            first_ts = int(part.index[0].value // 1_000_000)
            if first_ts <= start_1s:
                break
            cur_end = first_ts - 1
        df_1s = pd.concat(df_1s_parts).sort_index().drop_duplicates()
    else:
        df_1s = pd.DataFrame()

    return {"df_1m": df_1m, "df_1s": df_1s}


# --------------------------- Фичи и нормализация ---------------------------- #

def add_basic_features(df: pd.DataFrame, prefix: str) -> pd.DataFrame:
    df = df.copy()
    # Доходности
    df[f"{prefix}_ret"] = df["close"].pct_change()
    df[f"{prefix}_logret"] = np.log(df["close"]).diff()
    # Волатильность (роллинги)
    df[f"{prefix}_ret_std_60"] = df[f"{prefix}_ret"].rolling(60, min_periods=10).std()
    df[f"{prefix}_ret_std_300"] = df[f"{prefix}_ret"].rolling(300, min_periods=30).std()
    # SMA/EMA
    df[f"{prefix}_sma_20"] = df["close"].rolling(20, min_periods=5).mean()
    df[f"{prefix}_ema_20"] = df["close"].ewm(span=20, adjust=False).mean()
    # zscore по цене (на окне)
    roll_mean = df["close"].rolling(100, min_periods=20).mean()
    roll_std = df["close"].rolling(100, min_periods=20).std()
    df[f"{prefix}_z_100"] = (df["close"] - roll_mean) / (roll_std + 1e-12)
    return df


def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    ma_up = up.ewm(alpha=1/period, adjust=False).mean()
    ma_down = down.ewm(alpha=1/period, adjust=False).mean()
    rs = ma_up / (ma_down + 1e-12)
    return 100 - (100 / (1 + rs))


def add_1m_features(df_1m: pd.DataFrame) -> pd.DataFrame:
    df = df_1m.copy()
    df = add_basic_features(df, prefix="m1")
    df["m1_rsi_14"] = rsi(df["close"], 14)
    # ATR по 1m
    tr = np.maximum(df["high"] - df["low"], np.maximum((df["high"] - df["close"].shift()).abs(), (df["low"] - df["close"].shift()).abs()))
    df["m1_atr_14"] = tr.rolling(14, min_periods=5).mean()
    return df


def add_1s_features(df_1s: pd.DataFrame) -> pd.DataFrame:
    if df_1s.empty:
        return df_1s
    df = df_1s.copy()
    df = add_basic_features(df, prefix="s1")
    return df


# --------------------------- Нормализации (scalers) ------------------------- #

@dataclass
class ZScoreStats:
    mean: float
    std: float

    def transform(self, x: np.ndarray) -> np.ndarray:
        return (x - self.mean) / (self.std + 1e-12)


@dataclass
class RobustStats:
    median: float
    iqr: float  # Q3 - Q1

    def transform(self, x: np.ndarray) -> np.ndarray:
        return (x - self.median) / (self.iqr + 1e-12)


def fit_zscore(series: pd.Series) -> ZScoreStats:
    return ZScoreStats(mean=float(series.mean()), std=float(series.std(ddof=0)))


def fit_robust(series: pd.Series) -> RobustStats:
    q1 = float(series.quantile(0.25))
    q3 = float(series.quantile(0.75))
    return RobustStats(median=float(series.median()), iqr=(q3 - q1))


def fit_scalers(df: pd.DataFrame, cols: list[str], method: str = "zscore") -> dict[str, T.Union[ZScoreStats, RobustStats]]:
    scalers = {}
    for c in cols:
        s = df[c].dropna()
        if s.empty:
            continue
        if method == "zscore":
            scalers[c] = fit_zscore(s)
        elif method == "robust":
            scalers[c] = fit_robust(s)
        else:
            raise ValueError("Unknown method")
    return scalers


def apply_scalers(df: pd.DataFrame, scalers: dict, suffix: str = "_n") -> pd.DataFrame:
    out = df.copy()
    for c, st in scalers.items():
        if c in out:
            out[c + suffix] = st.transform(out[c].to_numpy())
    return out


# ------------------------------- Ring Buffer -------------------------------- #

class RingBuffer:
    """Простой кольцевой буфер фиксированной длины для онлайн-фич.
    Хранит numpy-массив shape=(capacity, dim)."""
    def __init__(self, capacity: int, dim: int):
        self.capacity = int(capacity)
        self.dim = int(dim)
        self.data = np.zeros((self.capacity, self.dim), dtype=np.float32)
        self.size = 0
        self.ptr = 0

    def push(self, x: np.ndarray):
        x = np.asarray(x, dtype=np.float32)
        assert x.shape[-1] == self.dim, f"dim mismatch: {x.shape[-1]} vs {self.dim}"
        self.data[self.ptr] = x
        self.ptr = (self.ptr + 1) % self.capacity
        self.size = min(self.size + 1, self.capacity)

    def to_array(self) -> np.ndarray:
        if self.size < self.capacity:
            return self.data[:self.size]
        # вернуть в хронологическом порядке
        idx = np.arange(self.ptr, self.ptr + self.capacity) % self.capacity
        return self.data[idx]

    def is_full(self) -> bool:
        return self.size == self.capacity


# ----------------------------- Главный пайплайн ----------------------------- #

def build_feature_frames(symbol: str, *, end_time: T.Union[int, str, pd.Timestamp, None] = None, hours_1m: int = 48, hours_1s: int = 3, use_continuous_1s: bool = True, contract_type: str = "PERPETUAL") -> dict:
    raw = backfill_klines(symbol, use_continuous_1s=use_continuous_1s, hours_1s=hours_1s, hours_1m=hours_1m, contract_type=contract_type, end_time=end_time)
    df_1m = add_1m_features(raw["df_1m"]).sort_index()
    df_1s = add_1s_features(raw["df_1s"]).sort_index()

    # выровнять 1m фичи на 1s, если есть 1s
    if not df_1s.empty:
        df_1m_on_1s = df_1m.reindex(df_1s.index, method="ffill")
    else:
        df_1m_on_1s = df_1m

    return {"df_1m": df_1m, "df_1s": df_1s, "df_1m_on_1s": df_1m_on_1s}


def fit_normalizers(frames: dict, method: str = "zscore") -> dict:
    # выберем, что нормализуем (пример)
    cols_1m = [c for c in frames["df_1m"].columns if any(k in c for k in ["ret","logret","std","atr","rsi","z_"])]
    scalers_1m = fit_scalers(frames["df_1m"], cols_1m, method=method)

    if not frames["df_1s"].empty:
        cols_1s = [c for c in frames["df_1s"].columns if any(k in c for k in ["ret","logret","std","z_"])]
        scalers_1s = fit_scalers(frames["df_1s"], cols_1s, method=method)
    else:
        scalers_1s = {}

    return {"m1": scalers_1m, "s1": scalers_1s}


def prepare_ring_buffers(frames: dict, scalers: dict, capacity_1s: int = 3600, capacity_1m: int = 240) -> dict:
    # применим нормализацию, сформируем буферы под онлайн инференс
    df_m1_n = apply_scalers(frames["df_1m"], scalers["m1"]) if scalers.get("m1") else frames["df_1m"].copy()
    if not frames["df_1s"].empty:
        df_s1_n = apply_scalers(frames["df_1s"], scalers["s1"]) if scalers.get("s1") else frames["df_1s"].copy()
        # Совмещённый набор фич на 1s таймлайне (пример: берём нормализованные 1s + ffill 1m)
        df_m1_on_1s = frames["df_1m_on_1s"].copy()
        df_m1_on_1s_n = apply_scalers(df_m1_on_1s, scalers["m1"]) if scalers.get("m1") else df_m1_on_1s
        # Выберем подмножество столбцов для буфера (пример)
        feat_s1 = pd.concat([
            df_s1_n[[c for c in df_s1_n.columns if c.endswith("_n") or c.startswith("s1_")]].fillna(0.0),
            df_m1_on_1s_n[[c for c in df_m1_on_1s_n.columns if c.endswith("_n") or c.startswith("m1_")]].fillna(0.0)
        ], axis=1)
        dim_s1 = feat_s1.shape[1]
        rb_s1 = RingBuffer(capacity_1s, dim_s1)
        for _, row in feat_s1.tail(capacity_1s).iterrows():
            rb_s1.push(row.to_numpy())
    else:
        rb_s1 = None

    # Буфер 1m (нормализованные 1m фичи)
    feat_m1 = df_m1_n[[c for c in df_m1_n.columns if c.endswith("_n") or c.startswith("m1_")]].fillna(0.0)
    dim_m1 = feat_m1.shape[1]
    rb_m1 = RingBuffer(capacity_1m, dim_m1)
    for _, row in feat_m1.tail(capacity_1m).iterrows():
        rb_m1.push(row.to_numpy())

    return {"rb_1s": rb_s1, "rb_1m": rb_m1, "feat_cols_1s": list(feat_s1.columns) if frames.get("df_1s") is not None and not frames["df_1s"].empty else [], "feat_cols_1m": list(feat_m1.columns)}


# ------------------------------- Пример использования ------------------------ #
if __name__ == "__main__":
    symbol = "BTCUSDT"
    frames = build_feature_frames(symbol, end_time=None, hours_1m=48, hours_1s=3, use_continuous_1s=True)
    scalers = fit_normalizers(frames, method="robust")  # или "zscore"
    bufs = prepare_ring_buffers(frames, scalers)

    print("1m df shape:", frames["df_1m"].shape)
    print("1s df shape:", frames["df_1s"].shape)
    if bufs["rb_1s"] is not None:
        print("RingBuffer 1s size:", bufs["rb_1s"].size, "dim:", bufs["rb_1s"].dim)
    print("RingBuffer 1m size:", bufs["rb_1m"].size, "dim:", bufs["rb_1m"].dim)
