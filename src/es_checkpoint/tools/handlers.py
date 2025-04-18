"""Exception handlers for es-checkpoint module.

Provides handlers for use with the try_except decorator, managing Elasticsearch
and tracker-related exceptions.
"""

import typing as t
import logging
from elasticsearch8.exceptions import (
    ApiError,
    BadRequestError,
    NotFoundError,
    TransportError,
)
from ..debug import debug, begin_end
from ..exceptions import (
    ClientError,
    FatalError,
    MissingIndex,
    MissingDocument,
    TrackerError,
)
from .utils import ender, two_values

logger = logging.getLogger(__name__)

if t.TYPE_CHECKING:
    from ..job import Job
    from ..task import Task
    from ..step import Step


@begin_end()
def es_response(exception: Exception) -> None:
    """Handles Elasticsearch API exceptions, excluding NotFoundError.

    Raises ClientError with a message and the original exception, unless the
    exception is a NotFoundError, which is skipped.

    Args:
        exception: Exception raised by the Elasticsearch API.

    Raises:
        ClientError: For non-NotFoundError exceptions with details.

    Examples:
        >>> from elasticsearch8.exceptions import ApiError
        >>> try:
        ...     es_response(ApiError("API failure"))
        ... except ClientError as e:
        ...     print(str(e).startswith("The exception type was ApiError"))
        True
        >>> es_response(NotFoundError("Not found"))  # No exception raised
    """
    if isinstance(exception, NotFoundError):
        debug.lv3("NotFoundError detected, skipping")
        return
    msg = (
        f"The exception type was {exception.__class__.__name__}, "
        f"with error message: {exception.args[0]}"
    )
    if isinstance(exception, (ApiError, TransportError, BadRequestError)):
        pass
    else:
        logger.warning("Other exception detected")
    debug.lv5(msg)
    raise ClientError(msg, errors=exception)


@begin_end()
def missing_handler(exception: Exception, fn_args: t.Dict[str, t.Any]) -> None:
    """Handles NotFoundError and MissingIndex/MissingDocument exceptions.

    Processes Elasticsearch NotFoundError or local MissingIndex/MissingDocument
    exceptions, raising appropriate exceptions with metadata.

    Args:
        exception: Exception raised.
        fn_args: Function arguments as a dictionary.

    Raises:
        MissingIndex: For NotFoundError or MissingIndex exceptions.
        MissingDocument: For MissingDocument exceptions.
        Exception: For unexpected exceptions.

    Examples:
        >>> fn_args = {"index": "test_idx", "name": None, "job_id": None,
        ...            "task_id": "123", "stepname": None}
        >>> try:
        ...     missing_handler(MissingDocument("Doc gone"), fn_args)
        ... except MissingDocument as e:
        ...     print(e.index)
        test_idx
        >>> try:
        ...     missing_handler(NotFoundError("Not found"), fn_args)
        ... except MissingIndex as e:
        ...     print(e.index)
        test_idx
    """
    errors = exception.errors if hasattr(exception, "errors") else ""
    name = fn_args.get("name") or None
    index = fn_args.get("index") or None
    job_id = fn_args.get("job_id") or None
    task_id = fn_args.get("task_id") or None
    stepname = fn_args.get("stepname") or None
    m_name = two_values("name", name, "index", index)
    m_id = two_values("job_id", job_id, "task_id", task_id)
    if not stepname:
        stepname = "Unknown"
    msg = (
        f"The exception type was {exception.__class__.__name__}, "
        f"with error message: {exception.args[0]}"
    )
    logger.error(msg)
    if isinstance(exception, (NotFoundError, MissingIndex)):
        raise MissingIndex(msg, errors=errors, index=m_name)
    if isinstance(exception, MissingDocument):
        raise MissingDocument(msg, errors=errors, index=m_name, doc_id=m_id)
    logger.warning("Unexpected exception detected")
    raise exception


@begin_end()
def tracker_handler(exception: Exception, fn_args: t.Dict[str, t.Any]) -> None:
    """Handles exceptions for Job, Task, or Step trackers.

    Marks the tracker as failed, logs the error, and raises a TrackerError.

    Args:
        exception: Exception raised.
        fn_args: Function arguments as a dictionary.

    Raises:
        ValueError: If no valid tracker is found in fn_args.
        TrackerError: For non-FatalError exceptions with tracker details.
        FatalError: Re-raised if the exception is a FatalError.

    Examples:
        >>> from unittest.mock import Mock
        >>> tracker = Mock(success=True, end=lambda **kw: None)
        >>> fn_args = {"task": tracker}
        >>> try:
        ...     tracker_handler(ValueError("Task error"), fn_args)
        ... except TrackerError as e:
        ...     print(e.tracker_type)
        task
        >>> fn_args = {}
        >>> try:
        ...     tracker_handler(ValueError("No tracker"), fn_args)
        ... except ValueError as e:
        ...     print(str(e))
        No step, task, or job object found in fn_args, or were None values
    """
    exname = exception.__class__.__name__
    msg = (
        f"The triggering exception type was {exname}, "
        f"with error message: {exception.args[0]}"
    )
    logger.critical(msg, stacklevel=2)
    if "step" in fn_args and fn_args["step"] is not None:
        tracker: t.Union["Job", "Task", "Step"] = fn_args["step"]
        tracker_type = "step"
    elif "task" in fn_args and fn_args["task"] is not None:
        tracker = fn_args["task"]
        tracker_type = "task"
    elif "job" in fn_args and fn_args["job"] is not None:
        tracker = fn_args["job"]
        tracker_type = "job"
    else:
        _ = "No step, task, or job object found in fn_args, or were None values"
        logger.error(_)
        raise ValueError(_)
    tracker.success = False

    ender(tracker)
    debug.lv3("Exiting function, raising exception")
    debug.lv5(f"Exception: {exception}")
    if isinstance(exception, FatalError):
        raise exception
    raise TrackerError(msg, tracker, tracker_type)
