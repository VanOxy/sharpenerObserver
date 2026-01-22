# file: snapshot_packer.py
from typing import Dict, List, Optional
from dataclasses import dataclass
import numpy as np
from ws_depth_sampler import DepthSampler
from time import perf_counter

def _bar_to_vec(bar) -> List[float]:
    """Ожидается объект с атрибутами .o .h .l .c .v (как у твоего Bar1s).
       Если вместо этого dict — попробуем ключи 'o','h','l','c','v'.
    """
    try:
        o, h, l, c, v = float(bar.o), float(bar.h), float(bar.l), float(bar.c), float(bar.v)
    except AttributeError:
        o, h, l, c, v = [float(bar.get(k, 0.0)) for k in ("o","h","l","c","v")]
    return [o, h, l, c, v]

def _lap(t0, tag, tb):
    if tb is None:  # быстрый ран без профайла
        return perf_counter()
    t1 = perf_counter()
    tb[tag] = tb.get(tag, 0.0) + (t1 - t0) * 1000.0
    return t1

@dataclass
class _Workspace:
    S_cap: int
    top_n: int
    tail_bins: int
    dtype: np.dtype
    # буферы:
    mask: np.ndarray
    bars_mat: np.ndarray
    depth_top_px: np.ndarray
    depth_top_qty: np.ndarray
    depth_tail_qty: np.ndarray
    depth_feats: np.ndarray
    extra_mat: np.ndarray

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
        self._workspace: Optional[_Workspace] = None  # лениво аллоцируем

     # -------- workspace lifecycle --------
    def alloc_workspace(self, S_cap: int) -> None:
        """Единовременное выделение всех матриц под текущие top_n/tail_bins."""
        tn = self.sampler.top_n
        tbins = self.sampler.tail_bins
        dt = self.dtype

        self._workspace = _Workspace(
            S_cap=S_cap, top_n=tn, tail_bins=tbins, dtype=dt,
            mask=np.zeros((S_cap,), dtype=dt),
            bars_mat=np.zeros((S_cap, 5), dtype=dt),
            depth_top_px=np.zeros((S_cap, 2, tn), dtype=dt),
            depth_top_qty=np.zeros((S_cap, 2, tn), dtype=dt),
            depth_tail_qty=np.zeros((S_cap, 2, tbins), dtype=dt),
            depth_feats=np.zeros((S_cap, 6), dtype=dt),
            extra_mat=np.zeros((S_cap, len(self.extra_keys)), dtype=dt),
        )

    def _ensure_workspace(self, S_cap: int) -> None:
        """Ре-аллокация только если изменились S_cap/top_n/tail_bins/dtype."""
        need_new = (
            self._workspace is None
            or self._workspace.S_cap != S_cap
            or self._workspace.top_n != self.sampler.top_n
            or self._workspace.tail_bins != self.sampler.tail_bins
            or self._workspace.dtype != self.dtype
        )
        if need_new:
            self.alloc_workspace(S_cap)

    # -------- основная логика --------
    def pack_into(
        self,
        bars: Dict[str, object],
        dom_all: Dict[str, dict],
        feats: Dict[str, dict],
        S_cap: int,
        symbols_order_hint: Optional[List[str]] = None,
        debug_timings: bool = False
    ) -> Dict:
        self._ensure_workspace(S_cap)
        workspace = self._workspace  # type: ignore
        # 0) debug
        tb = {} if debug_timings else None
        if tb is not None: t = perf_counter()

        # 1) пересечение ключей
        sym_set = set(bars.keys()) & set(dom_all.keys()) & set(feats.keys())
        if tb is not None: t = _lap(t, "pack_sym_set", tb)
        
       # 2) порядок символов
        if symbols_order_hint:
            symbols = [s for s in symbols_order_hint if s in sym_set]
            symbols.extend(sorted(sym_set - set(symbols)))  # добавим недостающие по алфавиту
        else:
            symbols = sorted(sym_set)
        if tb is not None: t = _lap(t, "pack_order", tb)

        # 3) валидация DOM/баров
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
                o = float(b.get("o", 0.0))
                c = float(b.get("c", 0.0))
                if o <= 0 or c <= 0:
                    continue
            valid_syms.append(s)

        if tb is not None: t = _lap(t, "pack_validate", tb)

        # 4) ограничение по S_cap и подготовка размеров
        used = valid_syms[:S_cap]
        top_n = self.sampler.top_n
        tail_bins = self.sampler.tail_bins

        # # 5) аллокации
        # mask            = np.zeros((S_cap,), dtype=self.dtype)
        # bars_mat        = np.zeros((S_cap, 5), dtype=self.dtype)
        # depth_top_px    = np.zeros((S_cap, 2, top_n), dtype=self.dtype)   # [bid/ask, N]
        # depth_top_qty   = np.zeros((S_cap, 2, top_n), dtype=self.dtype)
        # depth_tail_qty  = np.zeros((S_cap, 2, tail_bins), dtype=self.dtype)
        # depth_feats     = np.zeros((S_cap, 6), dtype=self.dtype)
        # extra_mat       = np.zeros((S_cap, len(self.extra_keys)), dtype=self.dtype)
        # if tb is not None: t = _lap(t, "pack_alloc", tb)

        # 5) аллокации - reset только маски (остальные матрицы мы перезапишем строками i< len(used))
        workspace.mask[:] = 0.0
        t = _lap(t, "pack_ws_reset", tb)

        # 6) основной цикл
        out_symbols: List[str] = []
        for i, s in enumerate(used):
            out_symbols.append(s)
            workspace.mask[i] = 1.0

            # 6a) бары → вектор
            t_sym = perf_counter() if tb is not None else None
            workspace.bars_mat[i, :] = np.asarray(_bar_to_vec(bars[s]), dtype=self.dtype)
            if tb is not None: t_sym = _lap(t_sym, "pack_loop_bar", tb)     #debug

            # 6b) depth compress
            comp = self.sampler.compress(dom_all[s], tb=tb)
            if comp is None:
                # маска останется 1, но данные нулевые — при желании можно занулить mask[i]
                continue
            if tb is not None: t_sym = _lap(t_sym, "pack_loop_compress", tb)

            # 6c) запись результатов семплера
            workspace.depth_top_px[i, 0, :] = comp["top_bid_px"]
            workspace.depth_top_px[i, 1, :] = comp["top_ask_px"]
            workspace.depth_top_qty[i, 0, :] = comp["top_bid_qty"]
            workspace.depth_top_qty[i, 1, :] = comp["top_ask_qty"]
            #хвосты
            if tail_bins > 0:
                workspace.depth_tail_qty[i, 0, :] = comp["tail_bid_qty"]
                workspace.depth_tail_qty[i, 1, :] = comp["tail_ask_qty"]
            # быстрые фичи
            workspace.depth_feats[i, :] = comp["depth_feats"]
            if tb is not None: t_sym = _lap(t_sym, "pack_loop_write", tb)

            # 6d) доп. фичи из feats
            f = feats.get(s) or {}
            for j, k in enumerate(self.extra_keys):
                v = f.get(k, 0.0)
                try:
                    workspace.extra_mat[i, j] = float(v)
                except Exception:
                    workspace.extra_mat[i, j] = 0.0
            if tb is not None: _lap(t_sym, "pack_loop_extras", tb)

        # 7) собрать выход
        out = {
            "symbols": out_symbols,              # длина ≤ S_cap
            "mask": workspace.mask,                        # (S_cap,)
            "bars": workspace.bars_mat,                    # (S_cap, 5)
            "depth_top_px": workspace.depth_top_px,        # (S_cap, 2, top_n)
            "depth_top_qty": workspace.depth_top_qty,      # (S_cap, 2, top_n)
            "depth_tail_qty": workspace.depth_tail_qty,    # (S_cap, 2, tail_bins)
            "depth_feats": workspace.depth_feats,          # (S_cap, 6)
            "extra_feats": workspace.extra_mat,            # (S_cap, len(extra_keys))
        }
        if tb is not None:
            out["timings"] = {k: round(v, 3) for k, v in tb.items()}
        return out
    
    # для совместимости с твоими вызовами
    def pack(
        self,
        bars: Dict[str, object],
        dom_all: Dict[str, dict],
        feats: Dict[str, dict],
        S_cap: int,
        symbols_order_hint: Optional[List[str]] = None,
        debug_timings: bool = False
    ) -> Dict:
        return self.pack_into(
            bars=bars, dom_all=dom_all, feats=feats,
            S_cap=S_cap, symbols_order_hint=symbols_order_hint,
            debug_timings=debug_timings
        )
