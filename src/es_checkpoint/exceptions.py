"""es-checkpoint Exceptions.

This module defines custom exceptions for the es-checkpoint module, used to handle
errors in Job, Task, and Step trackers, as well as Elasticsearch API responses.
"""

# pylint: disable=R0913,R0917,W0718
import typing as t
from dataclasses import dataclass
import logging

if t.TYPE_CHECKING:
    from .job import Job
    from .task import Task
    from .step import Step

Exceptions = t.Union[Exception, t.Tuple[Exception, ...]]

logger = logging.getLogger(__name__)


@dataclass
class TrackerMeta:
    """Metadata extracted from a Tracker object.

    Attributes:
        dry_run (bool): Indicates if the tracker is in dry run mode.
        stub (str): Extended name of the tracker.
        tracking_index (str): Name of the tracking index in Elasticsearch.
        doc_id (t.Optional[str]): Document ID in the tracking index, if set.
        start_time (t.Optional[str]): Start time of the tracker, if set.

    Examples:
        >>> meta = TrackerMeta(dry_run=True, stub="test",
        tracking_index="es-checkpoint")
        >>> print(meta.dry_run)
        True
    """

    dry_run: bool
    stub: str
    tracking_index: str
    doc_id: t.Optional[str] = None
    start_time: t.Optional[str] = None


def get_tracker_meta(tracker: t.Union['Job', 'Task', 'Step']) -> TrackerMeta:
    """Extracts metadata from a Tracker object.

    Args:
        tracker: The Tracker object (Job, Task, or Step).

    Returns:
        TrackerMeta: Metadata containing dry_run, stub, tracking_index, doc_id,
            and start_time.

    Examples:
        >>> from unittest.mock import Mock
        >>> tracker = Mock(dry_run=True, stub="test", tracking_index="idx",
        ...                doc_id="123", start_time="2023-01-01T00:00:00Z")
        >>> meta = get_tracker_meta(tracker)
        >>> meta.stub
        'test'

    """
    return TrackerMeta(
        dry_run=tracker.dry_run,
        stub=tracker.stub,
        tracking_index=tracker.tracking_index,
        doc_id=tracker.doc_id,
        start_time=tracker.start_time,
    )


class CheckpointError(Exception):
    """Base exception for all custom exceptions in es-checkpoint.

    Examples:
        >>> try:
        ...     raise CheckpointError("Base error")
        ... except CheckpointError as e:
        ...     print(str(e))
        Base error
    """

    def __repr__(self) -> str:
        """Returns a string representation of the exception.

        Returns:
            str: String representation of the exception.

        Examples:
            >>> repr(CheckpointError("Test error"))
            "CheckpointError('Test error')"
        """
        return f"{self.__class__.__name__}({self.args[0]!r})"


class FatalError(CheckpointError):
    """Exception raised for fatal errors that halt execution.

    Examples:
        >>> try:
        ...     raise FatalError("Critical failure")
        ... except FatalError as e:
        ...     print(str(e))
        Critical failure
    """

    def __repr__(self) -> str:
        """Returns a string representation of the exception.

        Returns:
            str: String representation of the exception.

        Examples:
            >>> repr(FatalError("Critical failure"))
            "FatalError('Critical failure')"
        """
        return f"{self.__class__.__name__}({self.args[0]!r})"


class NonFatalError(CheckpointError):
    """Exception raised for non-fatal errors that allow continuation.

    Examples:
        >>> try:
        ...     raise NonFatalError("Minor issue")
        ... except NonFatalError as e:
        ...     print(str(e))
        Minor issue
    """

    def __repr__(self) -> str:
        """Returns a string representation of the exception.

        Returns:
            str: String representation of the exception.

        Examples:
            >>> repr(NonFatalError("Minor issue"))
            "NonFatalError('Minor issue')"
        """
        return f"{self.__class__.__name__}({self.args[0]!r})"


class TrackerError(CheckpointError):
    """Exception raised at the Tracker level (Job, Task, or Step).

    Args:
        message: The error message.
        tracker: The Tracker object (Job, Task, or Step).
        tracker_type: The type of tracker ('job', 'task', or 'step').

    Attributes:
        message (str): The error message.
        meta (TrackerMeta): Metadata from the Tracker object.
        tracker_type (str): The type of tracker.

    Examples:
        >>> from unittest.mock import Mock
        >>> tracker = Mock(dry_run=False, stub="test_task", tracking_index="idx",
        ...                doc_id=None, start_time=None)
        >>> try:
        ...     raise TrackerError("Task failed", tracker, "task")
        ... except TrackerError as e:
        ...     print(e.tracker_type)
        task
        >>> print(str(e))  # doctest: +ELLIPSIS
        Task failed (TrackerMeta(...))
    """

    def __init__(
        self,
        message: str,
        tracker: t.Union['Job', 'Task', 'Step'],
        tracker_type: str,
    ):
        super().__init__(message)
        self.message = message
        self.meta = get_tracker_meta(tracker)
        self.tracker_type = tracker_type

    def __repr__(self) -> str:
        """Returns a string representation of the exception.

        Returns:
            str: String representation including message, tracker_type, and meta.

        Examples:
            >>> from unittest.mock import Mock
            >>> tracker = Mock(dry_run=False, stub="test", tracking_index="idx",
            ...                doc_id=None, start_time=None)
            >>> err = TrackerError("Error", tracker, "task")
            >>> repr(err)  # doctest: +ELLIPSIS
            "TrackerError('Error', tracker_type='task', meta=TrackerMeta(...))"
        """
        parts = [repr(self.message), f"tracker_type={repr(self.tracker_type)}"]
        if self.meta:
            parts.append(f"meta={self.meta!r}")
        return f"{self.__class__.__name__}({', '.join(parts)})"

    def __str__(self) -> str:
        """Returns a string representation of the error message and metadata.

        Returns:
            str: String with the error message and metadata.

        Examples:
            >>> from unittest.mock import Mock
            >>> tracker = Mock(dry_run=False, stub="test", tracking_index="idx",
            ...                doc_id=None, start_time=None)
            >>> err = TrackerError("Error", tracker, "task")
            >>> str(err)  # doctest: +ELLIPSIS
            'Error (TrackerMeta(...))'
        """
        return f"{self.message} ({self.meta})"


class EsResponse(Exception):
    """Exception for Elasticsearch API call error responses.

    Args:
        message: The error message.
        errors: A tuple of upstream exceptions that caused this error.

    Attributes:
        message (str): The error message.
        errors (tuple): Tuple of upstream exceptions.

    Examples:
        >>> try:
        ...     raise EsResponse("API error", (ValueError("Invalid input"),))
        ... except EsResponse as e:
        ...     print(len(e.errors))
        1
        >>> print(str(e))
        API error
    """

    def __init__(self, message: str, errors: Exceptions = ()):
        super().__init__(message)
        self.message = message
        self.errors = errors if isinstance(errors, tuple) else (errors,)

    def __repr__(self) -> str:
        """Returns a string representation of the exception.

        Returns:
            str: String representation including message and errors.

        Examples:
            >>> err = EsResponse("API error", (ValueError("Invalid"),))
            >>> repr(err)
            "EsResponse('API error', errors=ValueError('Invalid'))"
        """
        parts = [repr(self.message)]
        if self.errors:
            parts.append(f"errors={', '.join(map(repr, list(self.errors)))}")
        return f"{self.__class__.__name__}({', '.join(parts)})"

    def __str__(self) -> str:
        """Returns the error message as a string.

        Returns:
            str: The error message.

        Examples:
            >>> err = EsResponse("API error")
            >>> str(err)
            'API error'
        """
        return str(self.message)


class MissingError(EsResponse):
    """Exception for items not found in Elasticsearch.

    Args:
        message: The error message.
        errors: A tuple of upstream exceptions.

    Examples:
        >>> try:
        ...     raise MissingError("Not found")
        ... except MissingError as e:
        ...     print(str(e))
        Not found
    """

    def __repr__(self) -> str:
        """Returns a string representation of the exception.

        Returns:
            str: String representation including message and errors.

        Examples:
            >>> err = MissingError("Not found")
            >>> repr(err)
            "MissingError('Not found')"
        """
        parts = [repr(self.message)]
        if self.errors:
            parts.append(f"errors={', '.join(map(repr, list(self.errors)))}")
        return f"{self.__class__.__name__}({', '.join(parts)})"


class MissingIndex(MissingError):
    """Exception for missing Elasticsearch indices.

    Args:
        message: The error message.
        errors: A tuple of upstream exceptions.
        index: The name of the missing index, if known.

    Attributes:
        index (t.Optional[str]): The name of the missing index.

    Examples:
        >>> try:
        ...     raise MissingIndex("Index missing", index="my_index")
        ... except MissingIndex as e:
        ...     print(e.index)
        my_index
    """

    def __init__(
        self,
        message: str,
        errors: Exceptions = (),
        index: t.Optional[str] = None,
    ):
        super().__init__(message, errors)
        self.index = index
        self.message = message
        self.errors = errors if isinstance(errors, tuple) else (errors,)

    def __repr__(self) -> str:
        """Returns a string representation of the exception.

        Returns:
            str: String representation including message, index, and errors.

        Examples:
            >>> err = MissingIndex("Index missing", index="my_index")
            >>> repr(err)
            "MissingIndex('Index missing', index='my_index')"
        """
        parts = [repr(self.message)]
        if self.index:
            parts.append(f"index={repr(self.index)}")
        if self.errors:
            parts.append(f"errors={', '.join(map(repr, list(self.errors)))}")
        return f"{self.__class__.__name__}({', '.join(parts)})"


class MissingDocument(MissingError):
    """Exception for missing Elasticsearch documents.

    Args:
        message: The error message.
        errors: A tuple of upstream exceptions.
        index: The name of the index, if known.
        doc_id: The ID of the missing document, if known.

    Attributes:
        index (t.Optional[str]): The name of the index.
        doc_id (t.Optional[str]): The ID of the missing document.

    Examples:
        >>> try:
        ...     raise MissingDocument("Doc missing", index="idx", doc_id="123")
        ... except MissingDocument as e:
        ...     print(e.doc_id)
        123
    """

    def __init__(
        self,
        message: str,
        errors: Exceptions = (),
        index: t.Optional[str] = None,
        doc_id: t.Optional[str] = None,
    ):
        super().__init__(message, errors)
        self.index = index
        self.doc_id = doc_id
        self.message = message
        self.errors = errors if isinstance(errors, tuple) else (errors,)

    def __repr__(self) -> str:
        """Returns a string representation of the exception.

        Returns:
            str: String representation including message, index, doc_id, and errors.

        Examples:
            >>> err = MissingDocument("Doc missing", index="idx", doc_id="123")
            >>> repr(err)
            "MissingDocument('Doc missing', index='idx', doc_id='123')"
        """
        parts = [repr(self.message)]
        if self.index:
            parts.append(f"index={repr(self.index)}")
        if self.doc_id:
            parts.append(f"doc_id={repr(self.doc_id)}")
        if self.errors:
            parts.append(f"errors={', '.join(map(repr, list(self.errors)))}")
        return f"{self.__class__.__name__}({', '.join(parts)})"


class ClientError(EsResponse):
    """Exception for Elasticsearch client errors (excluding NotFoundError).

    Args:
        message: The error message.
        errors: A tuple of upstream exceptions.

    Examples:
        >>> try:
        ...     raise ClientError("Client error")
        ... except ClientError as e:
        ...     print(str(e))
        Client error
    """

    def __init__(self, message: str, errors: Exceptions = ()):
        super().__init__(message, errors)
        self.message = message
        self.errors = errors if isinstance(errors, tuple) else (errors,)

    def __repr__(self) -> str:
        """Returns a string representation of the exception.

        Returns:
            str: String representation including message and errors.

        Examples:
            >>> err = ClientError("Client error")
            >>> repr(err)
            "ClientError('Client error')"
        """
        parts = [repr(self.message)]
        if self.errors:
            parts.append(f"errors={', '.join(map(repr, list(self.errors)))}")
        return f"{self.__class__.__name__}({', '.join(parts)})"
