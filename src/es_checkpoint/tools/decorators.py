"""Decorators for es-checkpoint exception handling.

This module provides the try_except decorator for managing exceptions in the
es-checkpoint module, used with handlers from tools.handlers.
"""

# pylint: disable=R0913,R0917,W0718
import typing as t
import logging
from .utils import bind_args, has_arg, map_args
from ..debug import debug

logger = logging.getLogger(__name__)


def try_except(
    exceptions: t.Optional[t.Any] = Exception,
    handler: t.Optional[t.Callable] = None,
    re_raise: t.Optional[bool] = False,
    default: t.Optional[t.Any] = None,
    use: t.Optional[t.Type[Exception]] = None,
    use_map: t.Optional[t.Dict] = None,
    msg: t.Optional[str] = None,
) -> t.Callable:
    """Wraps a function with a try/except block for exception handling.

    Catches specified exceptions, logs them, and optionally calls a handler,
    re-raises, or returns a default value. Supports custom exception mapping.

    Args:
        exceptions: Exception(s) to catch (default: Exception).
        handler: Function to handle the exception (default: None).
        re_raise: Whether to re-raise the exception (default: False).
        default: Value to return if not re-raised (default: None).
        use: Custom exception class to raise (default: None).
        use_map: Dict mapping function args to exception attributes (default: None).
        msg: Custom message for logging and exception (default: None).

    Returns:
        t.Callable: Decorated function with try/except logic.

    Examples:
        >>> @try_except(exceptions=ValueError, default="Failed")
        ... def divide(a: int, b: int) -> float:
        ...     return a / b
        >>> divide(10, 0)
        'Failed'
        >>> @try_except(exceptions=ValueError, msg="Division error")
        ... def divide_log(a: int, b: int) -> float:
        ...     return a / b
        >>> try:
        ...     divide_log(10, 0)
        ... except ValueError as e:
        ...     print(str(e).startswith("Division error"))
        True
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            fn_args = bind_args(func, *args, **kwargs)
            use_args, use_kwargs = map_args(fn_args, use_map)
            try:
                debug.lv4(f"TRY: Calling {func.__name__}")
                debug.lv5(f"With args: {args}, kwargs: {kwargs}")
                return func(*args, **kwargs)
            except exceptions as exc:
                message = f"{exc.__name__} exception in {func.__name__}: {exc}"
                if msg:
                    message = f"{msg}. {message}"
                if handler:
                    handler(exc, fn_args)
                else:
                    logger.error(message)
                if use:
                    if has_arg(use.__init__, "errors"):
                        if "errors" in use_kwargs:
                            use_kwargs["errors"] += (exc,)
                        else:
                            use_kwargs["errors"] = exc
                    raise use(message, *use_args, **use_kwargs) from exc
                if re_raise:
                    raise
                return default

        return wrapper

    return decorator
