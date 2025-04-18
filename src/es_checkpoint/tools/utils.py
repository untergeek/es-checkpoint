"""Utilities for es-checkpoint module.

Provides helper functions for argument handling, logging, and tracker management.
"""

import typing as t
import inspect
from functools import lru_cache
from ..debug import debug, begin_end
from ..exceptions import TrackerError

if t.TYPE_CHECKING:
    from ..job import Job
    from ..task import Task
    from ..step import Step


@begin_end()
def ender(obj: t.Union["Job", "Task", "Step"], msg: t.Optional[str] = None) -> None:
    """Finalizes a tracker object, marking success or failure.

    Sets completion status, logs results, and calls type-specific methods (e.g.,
    dump for Task, save_to_task for Step).

    Args:
        obj: Tracker object (Job, Task, or Step).
        msg: Optional custom log message (default: None).

    Raises:
        TrackerError: If tracker type is unsupported or methods fail.

    Examples:
        >>> from unittest.mock import Mock
        >>> tracker = Mock(success=True, end=lambda **kw: None, dump=lambda: None)
        >>> tracker.__class__.__name__ = "Task"
        >>> ender(tracker, "Custom message")
        >>> tracker.end.called
        True
        >>> tracker.dump.called
        True
    """
    if not obj.success:
        err = True
        log = "Check application logs for detailed report"
    else:
        err = False
        log = "DONE"
    if msg:
        log = msg
    tracker_type = obj.__class__.__name__.lower()
    try:
        if tracker_type == "task":
            obj.dump()
        elif tracker_type == "step":
            obj.save_to_task()
        obj.end(completed=obj.success, errors=err, logmsg=log)
    except AttributeError as e:
        raise TrackerError(
            f"Invalid tracker type: {tracker_type}", obj, tracker_type
        ) from e


@lru_cache(maxsize=128)
@begin_end()
def get_sig(fn: t.Callable) -> inspect.Signature:
    """Gets the signature of a function.

    Caches the result for performance, useful for checking parameters.

    Args:
        fn: Function to inspect.

    Returns:
        inspect.Signature: Function's signature.

    Examples:
        >>> def example(a: int, b: str = "test") -> None:
        ...     pass
        >>> sig = get_sig(example)
        >>> str(sig)
        '(a: int, b: str = "test") -> None'
    """
    sig = inspect.signature(fn)
    debug.lv5(f"Return value = {sig!r}")
    return sig


@lru_cache(maxsize=128)
@begin_end()
def bind_args(fn: t.Callable, *args, **kwargs) -> t.Dict[str, t.Any]:
    """Binds function arguments to parameter names.

    Returns a dictionary mapping parameter names to values, excluding 'self'.

    Args:
        fn: Function to bind arguments for.
        args: Positional arguments.
        kwargs: Keyword arguments.

    Returns:
        t.Dict[str, t.Any]: Dictionary of parameter names to values.

    Examples:
        >>> def example(a: int, b: str) -> None:
        ...     pass
        >>> bind_args(example, 1, b="test")
        {'a': 1, 'b': 'test'}
    """
    sig = get_sig(fn)
    fn_args = sig.bind(*args, **kwargs).arguments
    if "self" in fn_args:
        del fn_args["self"]
    debug.lv5(f"Return value = {fn_args!r}")
    return fn_args


@lru_cache(maxsize=128)
@begin_end()
def has_arg(fn: t.Callable, arg: str) -> bool:
    """Checks if a function has a specific parameter.

    Caches the result for performance, useful for validating signatures.

    Args:
        fn: Function to check.
        arg: Parameter name to look for.

    Returns:
        bool: True if parameter exists, False otherwise.

    Examples:
        >>> def example(a: int, b: str = "test") -> None:
        ...     pass
        >>> has_arg(example, "a")
        True
        >>> has_arg(example, "c")
        False
    """
    sig = get_sig(fn)
    result = arg in sig.parameters
    debug.lv5(f"Return value = {result}")
    return result


@begin_end()
def name_or_index(argmap: t.Dict, fn_args: t.Dict) -> t.Dict:
    """Swaps 'index' and 'name' in argmap based on function signature.

    If 'index' is in argmap but not fn_args, and 'name' is in fn_args, swaps them,
    and vice versa.

    Args:
        argmap: Dictionary of argument names to values.
        fn_args: Dictionary of function signature arguments.

    Returns:
        t.Dict: Modified argument map.

    Examples:
        >>> argmap = {"index": "test_idx"}
        >>> fn_args = {"name": None}
        >>> name_or_index(argmap, fn_args)
        {'name': 'test_idx'}
        >>> argmap = {"index": "test_idx", "name": "test_name"}
        >>> name_or_index(argmap, fn_args)
        {'index': 'test_idx', 'name': 'test_name'}
    """
    if "index" in argmap and "index" not in fn_args and "name" in fn_args:
        debug.lv5("Found index in argmap, not in fn_args, found name")
        debug.lv5("Swapping name with index in argmap")
        argmap["name"] = argmap["index"]
        del argmap["index"]
    elif "name" in argmap and "name" not in fn_args and "index" in fn_args:
        debug.lv5("Found name in argmap, not in fn_args, found index")
        debug.lv5("Swapping index with name in argmap")
        argmap["index"] = argmap["name"]
        del argmap["name"]
    debug.lv5(f"Return value = {argmap!r}")
    return argmap


@begin_end()
def positional_args(argmap: t.Dict[str, t.Any], fn_args: t.Dict[str, t.Any]) -> t.Tuple:
    """Extracts ordered positional arguments from a function call.

    Uses argmap to map argument names to values, prioritizing 'value' keys.

    Args:
        argmap: Dictionary mapping argument names to metadata.
        fn_args: Dictionary of function arguments.

    Returns:
        t.Tuple: Ordered positional arguments.

    Examples:
        >>> argmap = {"a": {"position": 1, "value": 10}, "b": {"position": 2}}
        >>> fn_args = {"a": 1, "b": "test"}
        >>> positional_args(argmap, fn_args)
        (10, 'test')
    """
    retval: t.Tuple = ()
    if argmap:
        ordered = dict(sorted(argmap.items(), key=lambda item: item[1]))
        for key in ordered:
            if ordered[key].get("value", None) is not None:
                retval += (ordered[key]["value"],)
            else:
                retval += (fn_args[key],)
    debug.lv5(f"Return value = {retval!r}")
    return retval


@begin_end()
def keyword_args(argmap: t.Dict[str, t.Any], fn_args: t.Dict[str, t.Any]) -> t.Dict:
    """Extracts keyword arguments from a function call.

    Uses argmap to map argument names to values, prioritizing 'value' keys.

    Args:
        argmap: Dictionary mapping argument names to metadata.
        fn_args: Dictionary of function arguments.

    Returns:
        t.Dict: Keyword arguments.

    Examples:
        >>> argmap = {"a": {"attr": "x", "value": 10}, "b": {"attr": "y"}}
        >>> fn_args = {"a": 1, "b": "test"}
        >>> keyword_args(argmap, fn_args)
        {'x': 10, 'y': 'test'}
    """
    retval = {}
    if argmap:
        for k, v in argmap.items():
            if v.get("value", None) is not None:
                retval[argmap[k]["attr"]] = v["value"]
            else:
                retval[argmap[k]["attr"]] = fn_args[k]
    debug.lv5(f"Return value = {retval!r}")
    return retval


@begin_end()
def map_args(
    fn_args: t.Dict[str, t.Any], argmap: t.Dict[str, t.Any]
) -> t.Tuple[t.Tuple, t.Dict]:
    """Maps function arguments to positional and keyword arguments.

    Processes fn_args and argmap to separate positional and keyword arguments,
    handling 'index'/'name' swaps.

    Args:
        fn_args: Dictionary of function arguments.
        argmap: Dictionary mapping argument names to metadata.

    Returns:
        t.Tuple[t.Tuple, t.Dict]: Positional and keyword arguments.

    Examples:
        >>> fn_args = {"index": "test_idx", "b": "test"}
        >>> argmap = {"index": {"position": 1}, "b": {"attr": "y"}}
        >>> args, kwargs = map_args(fn_args, argmap)
        >>> args, kwargs
        (('test_idx',), {'y': 'test'})
    """
    args = ()
    kwargs: t.Dict[str, t.Any] = {}
    if not argmap:
        debug.lv3("No argmap provided, returning args and kwargs")
        return args, kwargs
    argmap = name_or_index(argmap, fn_args)
    intersection = list(set(fn_args.keys()).intersection(argmap.keys()))
    for key in list(fn_args):
        if key not in intersection:
            del fn_args[key]
    ordered = {}
    unordered = {}
    for key in argmap:
        position = argmap[key].get("position", 0)
        if position > 0:
            ordered[key] = argmap[key]
        else:
            unordered[key] = argmap[key]
    args = positional_args(ordered, fn_args)
    kwargs = keyword_args(unordered, fn_args)
    debug.lv5(f"Return value = {(args, kwargs)}")
    return args, kwargs


def two_values(name1: str, val1: t.Any, name2: str, val2: t.Any) -> str:
    """Formats two values into a string."""
    if not val1 and not val2:
        return "Unknown"
    if val1 and val2:
        return f"Both {name1} \"{val1}\" and {name2} \"{val2}\" found: IRREGULAR"
    return f"{val1 if val1 else ''}{val2 if val2 else ''}"
