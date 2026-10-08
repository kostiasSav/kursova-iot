"""Пікова пам'ять процесу — однаково на Windows, macOS і Linux."""

from __future__ import annotations

import sys


def peak_rss_mb() -> float:
    """Найбільший обсяг оперативної пам'яті, який цей процес займав досі, МБ."""
    try:
        import resource                       # macOS і Linux
    except ImportError:                       # Windows: через psutil
        try:
            import psutil
        except ImportError:
            return float("nan")
        mem = psutil.Process().memory_info()
        return getattr(mem, "peak_wset", mem.rss) / 1e6
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # macOS повідомляє байти, Linux — кілобайти
    return peak / 1e6 if sys.platform == "darwin" else peak / 1e3
