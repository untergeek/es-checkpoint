"""es-checkpoint module.

Tracks progress of Elasticsearch operations, including Jobs, Tasks, and Steps.

Attributes:
    __version__ (str): Module version.
    __author__ (str): Author of the module.
    __copyright__ (str): Copyright notice.
    __license__ (str): License type.
    __status__ (str): Development status.
    __description__ (str): Module description.
    __url__ (str): Project URL.
    __email__ (str): Contact email.
    __maintainer__ (str): Maintainer name.
    __maintainer_email__ (str): Maintainer email.
    __keywords__ (list[str]): Module keywords.
    __classifiers__ (list[str]): Package classifiers.
    __all__ (list[str]): Publicly exported symbols.

Examples:
    >>> from es_checkpoint import Job, Task, Step, __version__, __author__
    >>> from es_checkpoint import __copyright__, __license__
    >>> __version__
    '0.0.10'
    >>> __author__
    'Aaron Mildenstein'
    >>> __copyright__  # doctest: +ELLIPSIS
    '2025..., Aaron Mildenstein'
    >>> __license__
    'Apache 2.0'
    >>> isinstance(Job, type)
    True
"""

from datetime import datetime
from .job import Job
from .step import Step
from .task import Task

FIRST_YEAR = 2025
now = datetime.now()
if now.year == FIRST_YEAR:
    COPYRIGHT_YEARS = "2025"
else:
    COPYRIGHT_YEARS = f"2025-{now.year}"

__version__ = "0.0.10"
__author__ = "Aaron Mildenstein"
__copyright__ = f"{COPYRIGHT_YEARS}, {__author__}"
__license__ = "Apache 2.0"
__status__ = "Development"
__description__ = (
    "es-checkpoint module. Tracks progress of Elasticsearch operations, "
    "including Jobs, Tasks, and Steps."
)
__url__ = "https://github.com/untergeek/es-checkpoint"
__email__ = "aaron@mildensteins.com"
__maintainer__ = "Aaron Mildenstein"
__maintainer_email__ = f"{__email__}"
__keywords__ = [
    "elasticsearch",
    "progress",
    "tracking",
    "job",
    "task",
    "step",
    "es-checkpoint",
]
__classifiers__ = [
    "Development Status :: 4 - Beta",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: Apache Software License",
    "Programming Language :: Python :: 3.8",
    "Programming Language :: Python :: 3.9",
    "Programming Language :: Python :: 3.10",
    "Programming Language :: Python :: 3.11",
    "Programming Language :: Python :: 3.12",
    "Programming Language :: Python :: 3.13",
    "Operating System :: OS Independent",
]

__all__ = [
    "Job",
    "Step",
    "Task",
    "__version__",
    "__author__",
    "__copyright__",
    "__license__",
    "__status__",
    "__description__",
    "__url__",
    "__email__",
    "__maintainer__",
    "__maintainer_email__",
    "__keywords__",
    "__classifiers__",
]
