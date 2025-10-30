from typing import Tuple

class ActionCodec:
    """Discrete:
       0 = HOLD
       1..(S_cap*2*K) = (sym, side, size_level)
    """
    def __init__(self, S_cap: int, K: int):
        self.S_cap = S_cap
        self.K = K
        self.n_actions = 1 + S_cap * 2 * K

    def decode(self, a: int) -> Tuple[str, int, int]:
        # returns (kind, sym_idx, payload)
        if a == 0:
            return ("hold", -1, -1)
        i = a - 1
        sym_idx = i // (2 * self.K)
        rem = i % (2 * self.K)
        side_idx = rem // self.K           # 0=buy, 1=sell
        size_lvl = rem % self.K            # 0..K-1
        return ("trade", sym_idx, side_idx * self.K + size_lvl)

    def encode(self, sym_idx: int, side_idx: int, size_lvl: int) -> int:
        return 1 + sym_idx * (2 * self.K) + side_idx * self.K + size_lvl
