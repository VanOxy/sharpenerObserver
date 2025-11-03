# file: snapshot_packer.py
from typing import Dict, List, Tuple, Optional
import numpy as np
from ws_depth_sampler import DepthSampler

def _bar_to_vec(bar) -> List[float]:
    """Ожидается объект с атрибутами .o .h .l .c .v (как у твоего Bar1s).
       Если вместо этого dict — попробуем ключи 'o','h','l','c','v'.
    """
    try:
        o, h, l, c, v = float(bar.o), float(bar.h), float(bar.l), float(bar.c), float(bar.v)
    except AttributeError:
        o, h, l, c, v = [float(bar.get(k, 0.0)) for k in ("o","h","l","c","v")]
    return [o, h, l, c, v]

class SnapshotPacker:
    """
    Собирает пересечение bars/dom/feats → фиксированный пакет в модель.
    Делает паддинг до S_cap и маску (1 — валидный слот, 0 — паддинг).

    Выход (dict):
      - symbols: List[str] длиной ≤ S_cap (в порядке пакования)
      - mask: (S_cap,) float32
      - bars: (S_cap, 5)   [o,h,l,c,v]
      - depth_top_px:  (S_cap, 2, top_n)    [side=0:bid, 1:ask]
      - depth_top_qty: (S_cap, 2, top_n)
      - depth_tail_qty:(S_cap, 2, tail_bins)
      - depth_feats:   (S_cap, 6)           [mid,spread,micro,imb@k,sumBidK,sumAskK]
      - extra_feats:   (S_cap, M)           (по выбранным ключам из feats; опц.)
    """

    def __init__(
        self,
        sampler: Optional[DepthSampler] = None,
        extra_keys: Optional[List[str]] = None,
        dtype=np.float32,
    ):
        self.sampler = sampler or DepthSampler()
        # Можно выбрать ключи, которые уже считает твой DepthBooksManager.get_all_features()
        self.extra_keys = extra_keys or ["sum_bid_n_usd", "sum_ask_n_usd"]
        self.dtype = dtype

    def pack(
        self,
        bars: Dict[str, object],
        dom_all: Dict[str, dict],
        feats: Dict[str, dict],
        S_cap: int,
        symbols_order_hint: Optional[List[str]] = None,
    ) -> Dict:
        # пересечение доступных ключей
        sym_set = set(bars.keys()) & set(dom_all.keys()) & set(feats.keys())
        # порядок: либо по hint, либо сортированный
        if symbols_order_hint:
            symbols = [s for s in symbols_order_hint if s in sym_set]
            # добавим недостающие по алфавиту
            leftover = sorted(sym_set - set(symbols))
            symbols.extend(leftover)
        else:
            symbols = sorted(sym_set)

        # фильтруем невалидные DOM (mid<=0, пустые стороны)
        valid_syms = []
        for s in symbols:
            d = dom_all.get(s) or {}
            if not d:
                continue
            if float(d.get("mid", 0.0)) <= 0.0:
                continue
            if not (d.get("bids") and d.get("asks")):
                continue
            # bar sanity
            b = bars.get(s)
            if b is None:
                continue
            try:
                if (getattr(b, "o") <= 0) or (getattr(b, "c") <= 0):
                    continue
            except Exception:
                bb = {k: float(b.get(k, 0.0)) for k in ("o","c")}
                if bb["o"] <= 0 or bb["c"] <= 0:
                    continue
            valid_syms.append(s)

        # ограничиваем по S_cap
        used = valid_syms[:S_cap]
        S = len(used)
        top_n = self.sampler.top_n
        tail_bins = self.sampler.tail_bins

        # аллокации
        mask            = np.zeros((S_cap,), dtype=self.dtype)
        bars_mat        = np.zeros((S_cap, 5), dtype=self.dtype)
        depth_top_px    = np.zeros((S_cap, 2, top_n), dtype=self.dtype)   # [bid/ask, N]
        depth_top_qty   = np.zeros((S_cap, 2, top_n), dtype=self.dtype)
        depth_tail_qty  = np.zeros((S_cap, 2, tail_bins), dtype=self.dtype)
        depth_feats     = np.zeros((S_cap, 6), dtype=self.dtype)
        extra_mat       = np.zeros((S_cap, len(self.extra_keys)), dtype=self.dtype)

        out_symbols = []

        for i, s in enumerate(used):
            out_symbols.append(s)
            mask[i] = 1.0

            # bars
            bars_mat[i, :] = np.asarray(_bar_to_vec(bars[s]), dtype=self.dtype)

            # depth compress
            comp = self.sampler.compress(dom_all[s])
            if comp is None:
                # маска останется 1, но данные нулевые — при желании можно занулить mask[i]
                continue

            depth_top_px[i, 0, :] = comp["top_bid_px"]
            depth_top_px[i, 1, :] = comp["top_ask_px"]
            depth_top_qty[i, 0, :] = comp["top_bid_qty"]
            depth_top_qty[i, 1, :] = comp["top_ask_qty"]
            if tail_bins > 0:
                depth_tail_qty[i, 0, :] = comp["tail_bid_qty"]
                depth_tail_qty[i, 1, :] = comp["tail_ask_qty"]
            depth_feats[i, :] = comp["depth_feats"]

            # extra feats (по желанию, берём только выбранные ключи)
            f = feats.get(s) or {}
            for j, k in enumerate(self.extra_keys):
                v = f.get(k, 0.0)
                try:
                    extra_mat[i, j] = float(v)
                except Exception:
                    extra_mat[i, j] = 0.0

        return {
            "symbols": out_symbols,     # длина S (≤S_cap)
            "mask": mask,               # (S_cap,)
            "bars": bars_mat,           # (S_cap, 5)
            "depth_top_px": depth_top_px,     # (S_cap, 2, top_n)
            "depth_top_qty": depth_top_qty,   # (S_cap, 2, top_n)
            "depth_tail_qty": depth_tail_qty, # (S_cap, 2, tail_bins)
            "depth_feats": depth_feats,       # (S_cap, 6)
            "extra_feats": extra_mat,         # (S_cap, len(extra_keys))
        }
