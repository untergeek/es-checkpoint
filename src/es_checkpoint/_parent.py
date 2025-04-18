"""Parent classes for es-checkpoint trackers.

Provides Trackable and TaskOrStep base classes for Job, Task, and Step trackers.
"""

# pylint: disable=C0115,R0902,W0107
from abc import ABC, abstractmethod
import logging
import typing as t
from .debug import debug, begin_end
from .exceptions import MissingDocument, FatalError
from .storage import StorageBackend
from .utils import now_iso8601, get_progress_doc

logger = logging.getLogger(__name__)

if t.TYPE_CHECKING:
    from .job import Job


class Trackable(ABC):
    # Common attributes for tracking documents
    ATTRLIST = ["start_time", "completed", "end_time", "errors", "logs"]

    def __init__(
        self,
        backend: StorageBackend,
        tracking_index: str,
        doc_id: t.Optional[str] = None,
    ):
        """Initializes a Trackable object for progress tracking.

        Args:
            backend: Storage backend for document operations.
            tracking_index: Name of the tracking index or container.
            doc_id: Document ID in the storage backend (default: None).

        Examples:
            >>> from unittest.mock import Mock
            >>> backend = Mock()
            >>> tracker = Trackable(backend, "es-checkpoint", "doc1")
            >>> tracker.tracking_index
            'es-checkpoint'
            >>> tracker.doc_id
            'doc1'
        """
        #: StorageBackend: Backend for document operations
        self.backend = backend
        #: Name of the tracking index
        self.tracking_index = tracking_index
        #: Document ID in the storage backend
        self.doc_id = doc_id
        #: Status from storage backend
        self.status: t.Dict = {}
        #: List of log messages
        self.logs: t.List = []
        #: Start time of tracking
        self.start_time: str = ""
        #: End time of tracking
        self.end_time: str = ""
        #: Completion status
        self.completed: bool = False
        #: Error status
        self.errors: bool = False
        #: Dry run flag
        self.dry_run: bool = False
        #: Previous run dry run flag
        self.prev_dry_run: bool = False
        #: Short description
        self.stub: str = "Trackable"

    @abstractmethod
    def get_history(self):
        """Retrieves tracking history."""
        pass

    @begin_end()
    def build_doc(self) -> t.Dict:
        """Builds a tracking document for the tracker.

        Returns:
            dict: Dictionary for the tracking index.

        Examples:
            >>> from unittest.mock import Mock
            >>> class TestTracker(Trackable):
            ...     def get_history(self): pass
            ...     def extra_fields(self): return {"test": "value"}
            >>> tracker = TestTracker(Mock(), "es-checkpoint")
            >>> tracker.start_time = "2023-01-01T00:00:00Z"
            >>> doc = tracker.build_doc()
            >>> doc["start_time"]
            '2023-01-01T00:00:00Z'
            >>> doc["test"]
            'value'
        """
        debug.lv3(f"Building tracking document for {self.stub}")
        doc = {attr: getattr(self, attr) for attr in self.ATTRLIST}
        doc.update(self.extra_fields())
        doc = self.prune_empty_keys(doc)
        debug.lv5(f"Return value = {doc!r}")
        return doc

    @abstractmethod
    def extra_fields(self) -> t.Dict:
        """Provides additional fields for the tracking document.

        Returns:
            dict: Dictionary of additional fields specific to the tracker.

        Examples:
            >>> from unittest.mock import Mock
            >>> tracker = Trackable(Mock(), "es-checkpoint")
            >>> tracker.extra_fields()
            {}
        """
        return {}

    @begin_end()
    def add_log(self, value):
        """Adds a timestamped log message."""
        self.logs.append(f"{now_iso8601()} {value}")

    @begin_end()
    def attr2status(self):
        """Populates status from attributes.

        Examples:
            >>> tracker = Trackable(Mock(), "es-checkpoint")
            >>> tracker.start_time = "2023-01-01T00:00:00Z"
            >>> tracker.attr2status()
            >>> tracker.status["start_time"]
            '2023-01-01T00:00:00Z'
        """
        debug.lv3(f"Setting self.status from attributes {self.ATTRLIST}")
        for key in self.ATTRLIST:
            self.status[key] = getattr(self, key)

    @begin_end()
    def begin(self):
        """Starts tracking by setting start time and status.

        Examples:
            >>> from unittest.mock import Mock
            >>> tracker = Trackable(Mock(), "es-checkpoint")
            >>> tracker.dry_run = True
            >>> tracker.record = Mock()
            >>> tracker.begin()
            >>> tracker.start_time != ""
            True
            >>> tracker.completed
            False
        """
        logger.info(f"Begin tracking: {self.stub}...")
        if self.dry_run:
            msg = "DRY-RUN: No changes will be made"
            logger.info(msg)
            self.add_log(msg)
        self.start_time = now_iso8601()
        self.completed = False
        self.record()

    @begin_end()
    def end(
        self,
        completed: bool = False,
        errors: bool = False,
        logmsg: t.Optional[str] = None,
    ):
        """Ends tracking by setting end time and status.

        Args:
            completed: Completion status (default: False).
            errors: Error status (default: False).
            logmsg: Optional log message (default: None).

        Examples:
            >>> tracker = Trackable(Mock(), "es-checkpoint")
            >>> tracker.record = Mock()
            >>> tracker.end(completed=True, logmsg="Done")
            >>> tracker.completed
            True
            >>> tracker.logs
            ['... Done']
        """
        self.end_time = now_iso8601()
        self.completed = completed
        self.errors = errors
        if logmsg:
            self.add_log(logmsg)
        self.record()
        debug.lv2(f"{self.stub} ended. Completed: {completed}")

    @begin_end()
    def finished(self) -> bool:
        """Checks if a prior run was completed.

        Returns:
            bool: True if previously completed, False otherwise.

        Examples:
            >>> tracker = Trackable(Mock(), "es-checkpoint")
            >>> tracker.completed = True
            >>> tracker.stub = "Test"
            >>> tracker.finished()
            True
            >>> tracker.start_time = "2023-01-01T00:00:00Z"
            >>> tracker.completed = False
            >>> tracker.finished()
            False
        """
        if self.completed:
            if self.dry_run:
                logger.info(f"DRY-RUN: Ignoring previous run of {self.stub}")
            else:
                debug.lv1(f"{self.stub} was completed previously.")
                debug.lv5("Return value = True")
                return True
        if self.start_time:
            self.report_history()
            logger.info(f"{self.stub} was not completed in a previous run.")
        debug.lv5("Return value = False")
        return False

    def fn_result(
        self,
        func: t.Callable,
        args: t.Optional[t.Tuple] = None,
        kwargs: t.Optional[t.Dict] = None,
    ) -> t.Any:
        """Calls a function and handles exceptions.

        Args:
            func: Function to call.
            args: Positional arguments (default: None).
            kwargs: Keyword arguments (default: None).

        Returns:
            t.Any: Function result or empty dict for MissingDocument.

        Raises:
            FatalError: For non-MissingDocument exceptions.

        Examples:
            >>> tracker = Trackable(Mock(), "es-checkpoint")
            >>> def test_func(): return {"result": "success"}
            >>> tracker.fn_result(test_func)
            {'result': 'success'}
            >>> def fail_func(): raise ValueError("Error")
            >>> try:
            ...     tracker.fn_result(fail_func)
            ... except FatalError:
            ...     print("Caught")
            Caught
        """
        try:
            if args is None:
                args = ()
            if kwargs is None:
                kwargs = {}
            result = func(*args, **kwargs)
        except MissingDocument:
            debug.lv5("No document found")
            result = {}
        except Exception as exc:
            debug.lv3("Exiting function, raising exception")
            debug.lv5(f"Exception: {exc}")
            msg = "Error in storage operation"
            logger.error(f"{msg}: {exc}")
            raise FatalError(msg, errors=exc) from exc
        debug.lv5(f"Return value = {result}")
        return result

    @begin_end()
    def null2attr(self):
        """Sets attributes to None.

        Examples:
            >>> tracker = Trackable(Mock(), "es-checkpoint")
            >>> tracker.start_time = "2023-01-01T00:00:00Z"
            >>> tracker.null2attr()
            >>> tracker.start_time
        """
        debug.lv3(f"Overriding attributes {self.ATTRLIST} with None")
        for key in self.ATTRLIST:
            setattr(self, key, None)

    @begin_end()
    def prune_empty_keys(self, doc: t.Dict) -> t.Dict:
        """Removes empty keys from a document.

        Args:
            doc: Dictionary to process.

        Returns:
            t.Dict: Dictionary with empty keys removed.

        Examples:
            >>> tracker = Trackable(Mock(), "es-checkpoint")
            >>> doc = {"a": 1, "b": "", "c": None, "d": []}
            >>> tracker.prune_empty_keys(doc)
            {'a': 1}
        """
        debug.lv3("Removing empty keys from doc")
        empty: t.List = [None, "", [], {}]
        doc = {key: value for key, value in doc.items() if value not in empty}
        debug.lv5(f"Post cleanup: {doc}")
        return doc

    @begin_end()
    def record(self):
        """Saves the current status to the storage backend.

        Examples:
            >>> from unittest.mock import Mock
            >>> backend = Mock()
            >>> backend.save.return_value = "doc1"
            >>> tracker = Trackable(backend, "es-checkpoint")
            >>> tracker.build_doc = Mock(return_value={"field": "value"})
            >>> tracker.record()
            >>> backend.save.called
            True
            >>> tracker.doc_id
            'doc1'
        """
        doc = self.build_doc()
        if self.doc_id:
            self.backend.save(self.tracking_index, self.doc_id, doc)
        else:
            self.doc_id = self.backend.save(self.tracking_index, None, doc)

    @begin_end()
    def report_history(self) -> None:
        """Logs the history of a prior run.

        Examples:
            >>> tracker = Trackable(Mock(), "es-checkpoint")
            >>> tracker.stub = "Test"
            >>> tracker.start_time = "2023-01-01T00:00:00Z"
            >>> tracker.errors = True
            >>> tracker.logs = ["Error occurred"]
            >>> tracker.report_history()  # Logs history to logger
        """
        prefix = f"The prior run of {self.stub}"
        if self.prev_dry_run:
            debug.lv1(f"{prefix} was a dry run.")
        if self.start_time:
            debug.lv1(f"{prefix} started at {self.start_time}")
        if self.completed:
            if self.end_time:
                debug.lv1(f"{prefix} completed at {self.end_time}")
            else:
                msg = "is marked completed but did not record an end time"
                logger.warning(f"{prefix} started at {self.start_time} and {msg}")
        if self.errors:
            logger.warning(f"{prefix} encountered errors.")
            if self.logs:
                logger.warning(f"{prefix} had log(s): {self.logs}")

    @begin_end()
    def status2attr(self):
        """Populates attributes from status.

        Examples:
            >>> tracker = Trackable(Mock(), "es-checkpoint")
            >>> tracker.status = {"start_time": "2023-01-01T00:00:00Z"}
            >>> tracker.status2attr()
            >>> tracker.start_time
            '2023-01-01T00:00:00Z'
        """
        debug.lv3(f"Setting attributes {self.ATTRLIST} from self.status")
        for key in self.ATTRLIST:
            setattr(self, key, self.status.get(key, None))


class TaskOrStep(Trackable):
    def __init__(self, job: "Job", index: str) -> None:
        """Initializes a TaskOrStep object for tracking.

        Args:
            job: Job object associated with this task or step.
            index: Index name associated with this task or step.

        Examples:
            >>> from unittest.mock import Mock
            >>> job = Mock(backend=Mock(), tracking_index="es-checkpoint", name="job1")
            >>> task = TaskOrStep(job, "test_idx")
            >>> task.index
            'test_idx'
            >>> task.job.name
            'job1'
        """
        debug.lv2("Initializing Task object")
        super().__init__(job.backend, job.tracking_index)
        self.job = job
        self.index = index
        self.stub = f"Job: {job.name}"
        self.task_id = ""
        self.stepname = ""
        debug.lv3("Task object initialized")

    @begin_end()
    def extra_fields(self) -> t.Dict:
        """Provides additional fields for the TaskOrStep tracking document.

        Returns:
            dict: Dictionary with job, dry_run, index, step, and task fields.

        Examples:
            >>> from unittest.mock import Mock
            >>> job = Mock(backend=Mock(), tracking_index="es-checkpoint", name="job1")
            >>> task = TaskOrStep(job, "test_idx")
            >>> task.task_id = "task1"
            >>> task.stepname = "step1"
            >>> fields = task.extra_fields()
            >>> fields["job"]
            'job1'
            >>> fields["task"]
            'task1'
            >>> fields["step"]
            'step1'
        """
        fields = {
            "job": self.job.name,
            "dry_run": self.dry_run,
        }
        if self.index:
            fields["index"] = self.index
        if self.stepname:
            fields["step"] = self.stepname
        if self.task_id:
            fields["task"] = self.task_id
        return fields

    @begin_end()
    def get_history(self):
        """Retrieves the history of a task or step from the storage backend.

        Uses job name and task ID to fetch previous run data, updating status.

        Examples:
            >>> from unittest.mock import Mock
            >>> backend = Mock()
            >>> backend.search.return_value = [{"completed": True}]
            >>> job = Mock(backend=backend, tracking_index="es-checkpoint", name="job1")
            >>> task = TaskOrStep(job, "test_idx")
            >>> task.task_id = "task1"
            >>> task.get_history()
            >>> task.status["completed"]
            True
        """
        kwargs = {"job": self.job, "task_id": self.task_id}
        if not self.task_id:
            debug.lv2("No task_id set, skipping get_progress_doc()")
            return
        fn = get_progress_doc
        debug.lv4("TRY: get_progress_doc()")
        result = self.fn_result(fn, kwargs=kwargs)
        debug.lv5(f"get_progress_doc() result = {result!r}")
        self.doc_id = result.get("_id", None)
        self.status = result.get("_source", {})
        self.attr2status()
