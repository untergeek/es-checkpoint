"""Task Class.

Provides the Task class for tracking Elasticsearch operations within a Job.
"""

# pylint: disable=C0115,R0902,R0913,R0917,W0107
import typing as t
from dotmap import DotMap  # type: ignore
from .debug import debug, begin_end
from ._parent import TaskOrStep

if t.TYPE_CHECKING:
    from .job import Job


class Task(TaskOrStep):
    def __init__(self, job: "Job", index: str, id_suffix: str = "", task_id: str = ""):
        """Initializes a Task object for tracking.

        Args:
            job: Job object associated with this task.
            index: Index name associated with this task.
            id_suffix: Suffix to append to the task ID (default: "").
            task_id: Explicit task ID to use (default: "").

        Raises:
            ValueError: If neither id_suffix nor task_id is provided.

        Examples:
            >>> from unittest.mock import Mock
            >>> job = Mock(name="job1")
            >>> task = Task(job, "test_idx", id_suffix="suffix")
            >>> task.task_id
            'test_idx---suffix'
            >>> task = Task(job, "test_idx", task_id="custom_id")
            >>> task.task_id
            'custom_id'
            >>> try:
            ...     Task(job, "test_idx")
            ... except ValueError as e:
            ...     print(str(e))
            Either id_suffix or task_id must be provided
        """
        debug.lv2("Initializing Task object")
        if not id_suffix and not task_id:
            raise ValueError("Either id_suffix or task_id must be provided")
        #: str: Task ID
        self.task_id: str = ""
        if task_id:
            self.task_id = task_id
        else:
            self.task_id = f"{index}---{id_suffix}"
        super().__init__(job, index)
        #: DotMap: Task data and results
        self.data: DotMap = DotMap()
        #: bool: Flag for ILM policy
        self.is_ilm: bool = False
        #: str: Final index name
        self.final_name = index
        #: str: Short description
        self.stub = f"Task: {self.task_id} of Job: {job.name}"
        self.get_history()
        #: t.Optional[t.Any]: Task result
        self.result: t.Optional[t.Any] = None
        debug.lv3("Task object initialized")

    @begin_end()
    def extra_fields(self) -> t.Dict:
        """Provides additional fields for the Task tracking document.

        Extends TaskOrStep fields with task-specific data, is_ilm, final_name,
        and result.

        Returns:
            dict: Dictionary with additional task fields.

        Examples:
            >>> from unittest.mock import Mock
            >>> job = Mock(name="job1")
            >>> task = Task(job, "test_idx", task_id="task1")
            >>> task.data = DotMap({"key": "value"})
            >>> task.is_ilm = True
            >>> fields = task.extra_fields()
            >>> fields["job"]
            'job1'
            >>> fields["task"]
            'task1'
            >>> fields["data"]
            {'key': 'value'}
            >>> fields["is_ilm"]
            True
        """
        debug.lv3(f"Building Task extra fields for {self.task_id}")
        fields = super().extra_fields()
        fields.update(
            {
                "data": self.data.toDict(),
                "is_ilm": self.is_ilm,
                "final_name": self.final_name,
                "result": self.result,
            }
        )
        return fields

    @begin_end()
    def dump(self) -> None:
        """Logs task attributes for debugging.

        Examples:
            >>> from unittest.mock import Mock
            >>> job = Mock(name="job1")
            >>> task = Task(job, "test_idx", task_id="task1")
            >>> task.data = DotMap({"key": "value"})
            >>> task.add_log = Mock()
            >>> task.dump()
            >>> task.add_log.called
            True
        """
        debug.lv3(f"Dumping attributes of {self.task_id} to log")
        for attr in ["index", "task_id", "stub", "data"]:
            if attr == "data":
                value = self.data.toDict()
            else:
                value = getattr(self, attr, None)
            msg = f"{attr}: {value}"
            self.add_log(msg)
            debug.lv5(f"--- {msg}")
