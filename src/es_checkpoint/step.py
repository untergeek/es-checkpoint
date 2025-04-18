"""Step Class.

Provides the Step class for tracking sub-operations within a Task.
"""

# pylint: disable=C0115,R0902,R0913,R0917,W0107
import typing as t
from .debug import debug
from ._parent import TaskOrStep

if t.TYPE_CHECKING:
    from .task import Task


class Step(TaskOrStep):
    def __init__(self, task: "Task", stepname: str):
        """Initializes a Step object for tracking.

        Args:
            task: Task object associated with this step.
            stepname: Name of the step.

        Examples:
            >>> from unittest.mock import Mock
            >>> job = Mock(name="job1")
            >>> task = Mock(job=job, index="test_idx", task_id="task1")
            >>> step = Step(task, "step1")
            >>> step.stepname
            'step1'
            >>> step.stub
            'Step: step1 of Task: task1 of Job: job1'
        """
        debug.lv2("Initializing Step object")
        self.stepname = stepname
        super().__init__(task.job, task.index)
        self.stub = f"Step: {stepname} of Task: {self.task_id} of Job: {task.job.name}"
        self.get_history()
        debug.lv3("Task object initialized")

    def extra_fields(self) -> t.Dict:
        """Provides additional fields for the Step tracking document.

        Inherits fields from TaskOrStep, including job, dry_run, index, task,
        and step.

        Returns:
            dict: Dictionary with inherited fields.

        Examples:
            >>> from unittest.mock import Mock
            >>> job = Mock(name="job1")
            >>> task = Mock(job=job, index="test_idx", task_id="task1")
            >>> step = Step(task, "step1")
            >>> fields = step.extra_fields()
            >>> fields["job"]
            'job1'
            >>> fields["step"]
            'step1'
            >>> fields["task"]
            ''
        """
        debug.lv3(f"Building Step extra fields for {self.stepname}")
        return super().extra_fields()
