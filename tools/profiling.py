# =============================
# file: profiling.py
# =============================
from time import perf_counter
from typing import Optional, Dict

class StepProfiler:
    __slots__ = ("tb",)

    def __init__(self, tb: Optional[Dict[str, float]] = None):
        self.tb = tb

    def lap(self, t0: float, tag: str) -> float:
        """
        Measure elapsed time since t0, accumulate into tb[tag] (ms),
        return new t0.
        """
        if self.tb is None:
            return perf_counter()

        t1 = perf_counter()
        self.tb[tag] = self.tb.get(tag, 0.0) + (t1 - t0) * 1000.0
        return t1
    
    def reset(self):
        self.tb.clear()
