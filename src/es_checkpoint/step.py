"""Step class for tracking sub-operations within a Task.

Provides the Step class to manage and track step progress in ElasticCheckpoint,
using storage backends for persistence.
"""

# pylint: disable=C0115,R0902,R0913,R0917,W0107
import logging
import typing as t
from ._parent import TaskOrStep
from .debug import debug, begin_end
from .exceptions import ClientError, MissingDocument

if t.TYPE_CHECKING:
    from .task import Task

logger = logging.getLogger(__name__)


class Step(TaskOrStep):
    """Manages a step within a Task for ElasticCheckpoint job tracking.

    Tracks step progress, updating status in the storage backend (e.g., Elasticsearch,
    file, or in-memory). Inherits from TaskOrStep for shared tracking functionality.

    Args:
        task: Task object associated with this step.
        stepname: Unique name of the step.

    Attributes:
        stepname: Name of the step.
        stub: Descriptive string for logging (e.g., "Step: step1 of Task: task1
            of Job: job1").
        task_id: ID of the associated task, inherited from Task.
        job: Job object, inherited from Task.
        backend: Storage backend from the job.
        tracking_index: Index or container name for tracking documents.

    Examples:
        >>> from unittest.mock import Mock
        >>> job = Mock(name="job1", backend=Mock(), tracking_index="es-checkpoint")
        >>> task = Mock(job=job, index="test_idx", task_id="task1")
        >>> step = Step(task, "step1")
        >>> step.stepname
        'step1'
        >>> step.stub
        'Step: step1 of Task: task1 of Job: job1'
    """

    def __init__(self, task: "Task", stepname: str) -> None:
        debug.lv2("Initializing Step object")
        self.stepname = stepname
        super().__init__(task.job, task.index)
        self.task_id = task.task_id
        self.stub = (
            f"Step: {self.stepname} of Task: {self.task_id} of Job: {task.job.name}"
        )
        self.dry_run = task.job.dry_run
        self.get_history()
        debug.lv3("Step object initialized")

    @begin_end()
    def extra_fields(self) -> t.Dict:
        """Provides additional fields for the Step tracking document.

        Extends TaskOrStep fields with step-specific data.

        Returns:
            dict: Dictionary with job, dry_run, index, task, and step fields.

        Examples:
            >>> from unittest.mock import Mock
            >>> job = Mock(name="job1", backend=Mock(), tracking_index="es-checkpoint")
            >>> task = Mock(job=job, index="test_idx", task_id="task1")
            >>> step = Step(task, "step1")
            >>> fields = step.extra_fields()
            >>> fields["job"]
            'job1'
            >>> fields["step"]
            'step1'
            >>> fields["task"]
            'task1'
        """
        debug.lv3(f"Building Step extra fields for {self.stepname}")
        return super().extra_fields()

    @begin_end()
    def update_status(self, status: str) -> None:
        """Updates the step's status in the storage backend.

        Args:
            status: New status (e.g., "running", "completed", "failed").

        Raises:
            ValueError: If status is empty.
            ClientError: If the storage operation fails.

        Examples:
            >>> from unittest.mock import Mock
            >>> backend = Mock()
            >>> backend.save.return_value = "step1_doc"
            >>> job = Mock(name="job1", backend=backend, tracking_index="es-checkpoint")
            >>> task = Mock(job=job, index="test_idx", task_id="task1")
            >>> step = Step(task, "step1")
            >>> step.update_status("running")
            >>> backend.save.called
            True
        """
        if not status:
            raise ValueError("Status cannot be empty")
        debug.lv3(f"Updating status for {self.stub} to {status}")
        try:
            doc = self.build_doc()
            doc["status"] = status
            self.backend.save(self.tracking_index, self.doc_id, doc)
            logger.info(f"Updated status for {self.stub} to {status}")
        except ClientError as err:
            logger.error(f"Failed to update status for {self.stub}: {err}")
            raise

    @begin_end()
    def get_status(self) -> t.Optional[str]:
        """Retrieves the current status of the step from the storage backend.

        Returns:
            Optional[str]: The step's status, or None if not found.

        Examples:
            >>> from unittest.mock import Mock
            >>> backend = Mock()
            >>> backend.get.return_value = {"status": "running"}
            >>> job = Mock(name="job1", backend=backend, tracking_index="es-checkpoint")
            >>> task = Mock(job=job, index="test_idx", task_id="task1")
            >>> step = Step(task, "step1")
            >>> step.doc_id = "step1_doc"
            >>> status = step.get_status()
            >>> status
            'running'
        """
        debug.lv3(f"Retrieving status for {self.stub}")
        try:
            if not self.doc_id:
                debug.lv5("No doc_id set, returning None")
                return None
            doc = self.backend.get(self.tracking_index, self.doc_id)
            status = doc.get("status")
            debug.lv5(f"Retrieved status: {status}")
            return status
        except MissingDocument:
            debug.lv5("Step document not found")
            return None
        except ClientError as err:
            logger.error(f"Failed to retrieve status for {self.stub}: {err}")
            return None
