# file: snapshot_packer.py
from typing import Dict, List, Optional
from dataclasses import dataclass
import numpy as np
from ws_depth_sampler import DepthSampler
from time import perf_counter
from tools.profiling import StepProfiler    #profiling

# Импортируем параметры из менеджера, чтобы знать размеры
from ws_depth_manager import AI_TOP_N, AI_TAIL_BINS

def _bar_to_vec(bar) -> List[float]:
    """Ожидается объект с атрибутами .o .h .l .c .v (как у Bar1s).
       Если вместо этого dict — попробуем ключи 'o','h','l','c','v'.
    """
    try:
        o, h, l, c, v = float(bar.o), float(bar.h), float(bar.l), float(bar.c), float(bar.v)
    except AttributeError:
        o, h, l, c, v = [float(bar.get(k, 0.0)) for k in ("o","h","l","c","v")]
    return [o, h, l, c, v]

@dataclass
class _Workspace:
    S_cap: int
    #top_n: int
    #tail_bins: int
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

    def __init__(self, extra_keys: Optional[List[str]] = None, dtype=np.float32):
        # Можно выбрать ключи, которые уже считает твой DepthBooksManager.get_all_features()
        self.extra_keys = extra_keys or ["sum_bid_n_usd", "sum_ask_n_usd", "cum_imbalance_n_usd"]
        self.dtype = dtype
        self._workspace: Optional[_Workspace] = None  # лениво аллоцируем

     # -------- workspace lifecycle --------
    def alloc_workspace(self, S_cap: int) -> None:
        """Единовременное выделение всех матриц под текущие top_n/tail_bins."""
        self._workspace = _Workspace(
            S_cap=S_cap, dtype=self.dtype,
            mask=np.zeros((S_cap,), dtype=self.dtype),
            bars_mat=np.zeros((S_cap, 6), dtype=self.dtype),

            # Размеры берутся напрямую из настроек
            depth_top_px=np.zeros((S_cap, 2, AI_TOP_N), dtype=self.dtype),
            depth_top_qty=np.zeros((S_cap, 2, AI_TOP_N), dtype=self.dtype),
            depth_tail_qty=np.zeros((S_cap, 2, AI_TAIL_BINS), dtype=self.dtype),
            depth_feats=np.zeros((S_cap, 6), dtype=self.dtype),
            extra_mat=np.zeros((S_cap, len(self.extra_keys)), dtype=self.dtype),
        )
    """
    def _ensure_workspace(self, S_cap: int) -> None:
        #Ре-аллокация только если изменились S_cap/top_n/tail_bins/dtype.
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

        # === profiling/debug ===
        tb = {} if debug_timings else None
        prof = StepProfiler(tb)
        t0 = perf_counter()
        #========================

        # 1) пересечение ключей
        sym_set = set(bars.keys()) & set(dom_all.keys()) & set(feats.keys())
        t0 = prof.lap(t0, "pack_sym_set")    #profiling
        
       # 2) порядок символов
        if symbols_order_hint:
            symbols = [s for s in symbols_order_hint if s in sym_set]
            symbols.extend(sorted(sym_set - set(symbols)))  # добавим недостающие по алфавиту
        else:
            symbols = sorted(sym_set)
        t0 = prof.lap(t0, "pack_order")     #profiling

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

        t0 = prof.lap(t0, "pack_validate")      #profiling

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
        t0 = prof.lap(t0, "pack_ws_reset")      #profiling

        # 6) основной цикл
        out_symbols: List[str] = []
        for i, s in enumerate(used):
            out_symbols.append(s)
            workspace.mask[i] = 1.0

            # 6a) бары → вектор
            t_sym = perf_counter() if tb is not None else None
            workspace.bars_mat[i, :] = np.asarray(_bar_to_vec(bars[s]), dtype=self.dtype)
            t0 = prof.lap(t0, "pack_loop_bar")      #profiling

            # 6b) depth compress
            comp = self.sampler.compress(dom_all[s], tb=tb)
            if comp is None:
                # маска останется 1, но данные нулевые — при желании можно занулить mask[i]
                continue
            t0 = prof.lap(t0, "pack_loop_compress")   #profiling

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
            t0 = prof.lap(t0, "pack_loop_write")      #profiling

            # 6d) доп. фичи из feats
            f = feats.get(s) or {}
            for j, k in enumerate(self.extra_keys):
                v = f.get(k, 0.0)
                try:
                    workspace.extra_mat[i, j] = float(v)
                except Exception:
                    workspace.extra_mat[i, j] = 0.0
            t0 = prof.lap(t0, "pack_loop_extras")    #profiling

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
    """
    def pack(self, bars: Dict, ai_data: Dict, S_cap: int, debug_timings=False):
        if self._workspace is None or self._workspace.S_cap != S_cap:
            self.alloc_workspace(S_cap)
        workspace = self._workspace
        
        # Очистка маски
        workspace.mask[:] = 0.0

        # Приводим всё к верхнему регистру на лету, чтобы точно поймать пересечение
        bars_up = {k.upper(): v for k, v in bars.items()}
        ai_up = {k.upper(): v for k, v in ai_data.items()}
        
        # Пересечение символов
        valid_syms = sorted(list(set(bars_up.keys()) & set(ai_up.keys())))
        used = valid_syms[:S_cap]

        for i, s in enumerate(used):
            workspace.mask[i] = 1.0
            
            # 1. Bars
            b = bars_up[s]
            # Быстрая конвертация (предполагаем, что b - объект или dict)
            try:
                vec = [float(b.o), float(b.h), float(b.l), float(b.c), float(b.v), float(b.n)]
            except AttributeError:
                # Если вдруг пришел словарь (резервный вариант)
                vec = [float(b.get(k,0)) for k in 'ohlcv']
                vec.append(float(b.get('n', 0.0)))
            workspace.bars_mat[i] = vec

            # 2. AI Data (просто копируем массивы!)
            d = ai_up[s]
            workspace.depth_top_px[i, 0, :] = d['top_bid_px']
            workspace.depth_top_px[i, 1, :] = d['top_ask_px']
            workspace.depth_top_qty[i, 0, :] = d['top_bid_qty']
            workspace.depth_top_qty[i, 1, :] = d['top_ask_qty']
            
            workspace.depth_tail_qty[i, 0, :] = d['tail_bid_qty']
            workspace.depth_tail_qty[i, 1, :] = d['tail_ask_qty']
            
            workspace.depth_feats[i, :] = d['depth_feats']

            # 3. Extra feats
            extras = d.get('extra_feats', {})
            for j, k in enumerate(self.extra_keys):
                workspace.extra_mat[i, j] = extras.get(k, 0.0)

        return {
            "symbols": used,
            "mask": workspace.mask,
            "bars": workspace.bars_mat,
            "depth_top_px": workspace.depth_top_px,
            "depth_top_qty": workspace.depth_top_qty,
            "depth_tail_qty": workspace.depth_tail_qty,
            "depth_feats": workspace.depth_feats,
            "extra_feats": workspace.extra_mat
        }