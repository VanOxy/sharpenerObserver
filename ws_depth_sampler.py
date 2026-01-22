# file: depth_sampler.py
from typing import Dict, List, Optional, Tuple
import math
import numpy as np
from time import perf_counter

def _lap(t0, tag, tb):
    t1 = perf_counter()
    tb[tag] = tb.get(tag, 0.0) + (t1 - t0) * 1000.0
    return t1

class DepthSampler:
    """
    Сжатие DOM:
      - сырой top-N по каждой стороне (px, qty)
      - «хвост» уровней агрегируется лог-биннингом в bps от mid (по qty)
      - быстрые фичи: spread, microprice, imbalance@k

    Ожидаемый формат dom_sym:
      {
        "symbol": "BTCUSDT",
        "mid": float,
        "bids": [{"px": float, "qty": float, ...}, ...]  # сорт. по убыванию px
        "asks": [{"px": float, "qty": float, ...}, ...]  # сорт. по возрастанию px
      }
    Поле "rel" (относит. отклонение от mid) опционально; если нет — считаем.
    """

    def __init__(
        self,
        top_n: int = 40,
        tail_bins: int = 32,
        tail_max_bps: float = 50.0,  # расстояние хвоста до ±50 bps
        imb_k: Optional[int] = None, # для imbalance@k (по умолчанию = min(10, top_n))
        dtype=np.float32,
    ):
        self.top_n = int(top_n)
        self.tail_bins = int(tail_bins)
        self.tail_max_bps = float(tail_max_bps)
        self.imb_k = imb_k if imb_k is not None else min(10, self.top_n)
        self.dtype = dtype
        # ширина одного бина в bps
        self._bin_w = self.tail_max_bps / self.tail_bins if self.tail_bins > 0 else 1.0

    def _ensure_level_rel(self, px: float, mid: float, level: Dict) -> float:
        # используем level["rel"] если есть; иначе считаем (px-mid)/mid
        if "rel" in level and isinstance(level["rel"], (int, float)):
            return float(level["rel"])
        if mid <= 0:
            return 0.0
        return (px - mid) / mid

    def _top_arrays(self, side_levels: List[Dict], take: int, descending: bool) -> Tuple[np.ndarray, np.ndarray]:
        # предполагаем, что менеджер уже отсортировал:
        # bids: по убыванию px; asks: по возрастанию px
        px = np.zeros((take,), dtype=self.dtype)
        qty = np.zeros((take,), dtype=self.dtype)
        n = min(len(side_levels), take)
        if n > 0:
            for i in range(n):
                px[i]  = float(side_levels[i].get("px", 0.0))
                qty[i] = float(side_levels[i].get("qty", 0.0))
        return px, qty

    def _tail_bins_qty(
        self, side_levels: List[Dict], start_idx: int, mid: float) -> np.ndarray:
        """
        Агрегирует qty в tail_bins по |rel| в bps от mid.
        sign: -1 для bid, +1 для ask (для вычисления rel берём модуль, знак не важен).
        """
        bins = np.zeros((self.tail_bins,), dtype=self.dtype)
        if self.tail_bins == 0:
            return bins

        for i in range(start_idx, len(side_levels)):
            lv = side_levels[i]
            px = float(lv.get("px", 0.0))
            qty = float(lv.get("qty", 0.0))
            if qty <= 0 or mid <= 0 or px <= 0:
                continue
            rel = self._ensure_level_rel(px, mid, lv)  # ~ (px-mid)/mid
            rel_bps = abs(rel) * 1e4  # → bps
            if rel_bps <= 0:
                idx = 0
            elif rel_bps >= self.tail_max_bps:
                idx = self.tail_bins - 1
            else:
                idx = int(math.floor(rel_bps / self._bin_w))
                idx = max(0, min(idx, self.tail_bins - 1))
            bins[idx] += qty
        return bins

    def _fast_feats(
        self,
        mid: float,
        best_bid_px: float,
        best_bid_qty: float,
        best_ask_px: float,
        best_ask_qty: float,
        bid_qty_k: float,
        ask_qty_k: float,
    ) -> np.ndarray:
        spread = max(0.0, best_ask_px - best_bid_px)
        # microprice (для top-of-book)
        den = best_bid_qty + best_ask_qty
        if den > 0:
            micro = (best_ask_px * best_bid_qty + best_bid_px * best_ask_qty) / den
        else:
            micro = mid if mid > 0 else (best_bid_px + best_ask_px) * 0.5

        den_k = bid_qty_k + ask_qty_k
        imb_k = (bid_qty_k - ask_qty_k) / den_k if den_k > 0 else 0.0

        return np.asarray(
            [mid, spread, micro, imb_k, bid_qty_k, ask_qty_k],
            dtype=self.dtype
        )

    def compress(self, dom_sym: Dict, tb: Optional[Dict[str, float]] = None) -> Optional[Dict]:
        """Возвращает словарь с массивами и фичами или None, если DOM невалиден.
            Если передан tb — аккумулируем туда время по этапам.
        """
        if not dom_sym:
            return None
        #debug
        t = perf_counter() if tb is not None else None

        mid = float(dom_sym.get("mid", 0.0))
        bids = dom_sym.get("bids") or []
        asks = dom_sym.get("asks") or []
        if mid <= 0 or not bids or not asks:
            return None

        # Top-N
        if tb is not None: t = _lap(t, "ds_pre_top", tb)
        bid_px, bid_qty = self._top_arrays(bids, self.top_n, descending=True)
        if tb is not None: t = _lap(t, "ds_top_bids", tb)
        ask_px, ask_qty = self._top_arrays(asks, self.top_n, descending=False)
        if tb is not None: t = _lap(t, "ds_top_asks", tb)

        # Хвосты
        tail_bid = self._tail_bins_qty(bids, self.top_n, mid)
        if tb is not None: t = _lap(t, "ds_tail_bids", tb)
        tail_ask = self._tail_bins_qty(asks, self.top_n, mid)
        if tb is not None: t = _lap(t, "ds_tail_asks", tb)

        # Фичи (imbalance по первым k уровням)
        k = min(self.imb_k, self.top_n)
        bid_qty_k = float(bid_qty[:k].sum())
        ask_qty_k = float(ask_qty[:k].sum())
        best_bid_px = float(bid_px[0]) if bid_px.size > 0 else 0.0
        best_ask_px = float(ask_px[0]) if ask_px.size > 0 else 0.0
        best_bid_qty = float(bid_qty[0]) if bid_qty.size > 0 else 0.0
        best_ask_qty = float(ask_qty[0]) if ask_qty.size > 0 else 0.0
        feats = self._fast_feats(
            mid, best_bid_px, best_bid_qty, best_ask_px, best_ask_qty, bid_qty_k, ask_qty_k
        )
        if tb is not None: _lap(t, "ds_feats", tb)

        return {
            "top_bid_px": bid_px,
            "top_bid_qty": bid_qty,
            "top_ask_px": ask_px,
            "top_ask_qty": ask_qty,
            "tail_bid_qty": tail_bid,
            "tail_ask_qty": tail_ask,
            "depth_feats": feats,  # [mid, spread, microprice, imb@k, sum_bid_k, sum_ask_k]
        }
