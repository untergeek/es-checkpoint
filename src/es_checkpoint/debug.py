"""Debug utilities for es-checkpoint.

Provides a decorator for logging function entry and exit with tiered debugging.
"""

import typing as t
from functools import wraps
from tiered_debug import TieredDebug

debug = TieredDebug()


def begin_end(begin: t.Optional[int] = 2, end: t.Optional[int] = 3) -> t.Callable:
    """Logs function entry and exit at specified debug levels.

    Args:
        begin: Debug level for entry logging (1-5, default: 2).
        end: Debug level for exit logging (1-5, default: 3).

    Returns:
        t.Callable: Decorated function with entry/exit logging.

    Examples:
        >>> from unittest.mock import Mock
        >>> debug.lv2 = Mock()
        >>> debug.lv3 = Mock()
        >>> debug.stacklevel = 0
        >>> @begin_end(begin=2, end=3)
        ... def test_func():
        ...     pass
        >>> test_func()
        >>> debug.lv2.called
        True
        >>> debug.lv3.called
        True
    """
    mmap = {
        1: debug.lv1,
        2: debug.lv2,
        3: debug.lv3,
        4: debug.lv4,
        5: debug.lv5,
    }

    def decorator(func: t.Callable) -> t.Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            common = f"CALL: {func.__name__}()"
            mmap[begin](f"BEGIN {common}", stklvl=debug.stacklevel + 1)
            result = func(*args, **kwargs)
            mmap[end](f"END {common}", stklvl=debug.stacklevel + 1)
            return result

        return wrapper

    return decorator
