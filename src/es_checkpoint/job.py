"""Job class for managing and tracking jobs.

Provides the Job class to orchestrate Elasticsearch operations, track progress,
and manage configuration.
"""

# pylint: disable=C0115,R0902,R0913,R0917,W0107
import logging
import typing as t
from ._parent import Trackable
from .tools.decorators import try_except
from .debug import debug, begin_end
from .defaults import index_settings, status_mappings
from .exceptions import ClientError, FatalError
from .utils import (
    create_index,
    get_tracking_doc,
    index_exists,
    parse_job_config,
)

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch

logger = logging.getLogger(__name__)


class Job(Trackable):
    def __init__(
        self,
        client: "Elasticsearch",
        tracking_index: str,
        name: str,
        config: t.Dict,
        dry_run: bool = False,
    ):
        """Initializes a Job for tracking Elasticsearch operations.

        Args:
            client: Elasticsearch client connection.
            tracking_index: Name of the tracking index.
            name: Unique name for the job.
            config: Configuration dictionary for the job.
            dry_run: If True, simulates operations without changes (default: False).

        Examples:
            >>> from unittest.mock import Mock
            >>> client = Mock()
            >>> job = Job(client, "es-checkpoint", "test_job", {}, dry_run=True)
            >>> job.name
            'test_job'
            >>> job.dry_run
            True
        """
        debug.lv2("Initializing Job object")
        super().__init__(client, tracking_index, doc_id=name)
        #: str: Tracker stub
        self.stub = name
        #: str: Name of the job
        self.name = name
        debug.lv5(f"Job name: {name}")
        #: dict: Configuration from file
        self.file_config = config
        debug.lv5(f"File config: {config}")
        #: dict: Parsed job configuration
        self.config: t.Dict = {}
        #: bool: Indicates if this is a dry run
        self.dry_run = dry_run
        debug.lv5(f"Job dry_run: {dry_run}")
        #: bool: Indicates if previous run was a dry run
        self.prev_dry_run = False
        self.chk_idx()
        self.get_history()
        #: t.List: Results from job tasks
        self.results: t.List[t.Any] = []
        self.cleanup: t.List[str] = []
        self.indices: t.List = []
        #: t.Dict: Index counts
        self.index_counts: t.Dict = {}
        self.total = 0
        debug.lv3("Job object initialized")

    @begin_end()
    def extra_fields(self) -> t.Dict:
        """Provides additional fields for the Job tracking document.

        Returns:
            dict: Dictionary with job, config, join_field, and dry_run fields.

        Examples:
            >>> from unittest.mock import Mock
            >>> client = Mock()
            >>> job = Job(client, "es-checkpoint", "test_job", {"query": {}})
            >>> fields = job.extra_fields()
            >>> fields["job"]
            'test_job'
            >>> fields["join_field"]
            'job'
            >>> fields["dry_run"]
            False
        """
        debug.lv3("Building Job extra fields for tracking document")
        fields = {
            "job": self.name,
            "config": parse_job_config(self.config, "write"),
            "join_field": "job",
            "dry_run": self.dry_run,
        }
        return fields

    @begin_end()
    def get_history(self):
        """Retrieves the job's history from the tracking index.

        Uses the job name as the document ID to fetch previous run data, updating
        configuration and status.

        Examples:
            >>> from unittest.mock import Mock
            >>> client = Mock()
            >>> job = Job(client, "es-checkpoint", "test_job", {"query": {}})
            >>> job.fn_result = Mock(return_value={"_source": {"config": {"query": '{"match_all": {}}'}, "dry_run": True}})
            >>> job.get_history()
            >>> job.prev_dry_run
            True
            >>> job.config
            {'query': {'match_all': {}}}
        """
        args = (self.client, self.tracking_index, self.name)
        fn = get_tracking_doc
        debug.lv4("TRY: get_tracking_doc()")
        result = self.fn_result(fn, args=args)
        debug.lv5(f"get_tracking_doc() result = {result!r}")
        self.doc_id = result.get("_id", None)
        self.status = result.get("_source", {})
        config = result.get("_source", {}).get("config", None)
        debug.lv5(f"Job config from tracking doc: {config!r}")
        if config is None:
            debug.lv3("No Job config found in tracking doc - using file config")
            self.config = self.file_config
        else:
            self.config = parse_job_config(config, "read")
        debug.lv5(f"Current value of self.config: {self.config}")
        self.prev_dry_run = result.get("_source", {}).get("dry_run", False)
        if self.prev_dry_run:
            debug.lv3("Previous run was a dry run.")
            self.null2attr()
        else:
            self.attr2status()
        debug.lv5(f"Job config = {self.config!r}")

    @begin_end()
    def chk_idx(self) -> None:
        """Checks if the tracking index exists, creating it if necessary.

        Examples:
            >>> from unittest.mock import Mock
            >>> client = Mock()
            >>> client.indices.exists.return_value = False
            >>> job = Job(client, "es-checkpoint", "test_job", {})
            >>> job.mk_idx = Mock()
            >>> job.chk_idx()
            >>> job.mk_idx.called
            True
        """
        debug.lv2("BEGIN chk_idx()")
        args = (self.client, self.tracking_index)
        if not index_exists(*args):
            self.mk_idx()

    @try_except(exceptions=ClientError, use=FatalError)
    @begin_end()
    def mk_idx(self) -> None:
        """Creates the tracking index for the job.

        Raises:
            FatalError: If the index creation fails.

        Examples:
            >>> from unittest.mock import Mock
            >>> client = Mock()
            >>> client.indices.exists.return_value = False
            >>> job = Job(client, "es-checkpoint", "test_job", {})
            >>> job.mk_idx()
            >>> client.indices.create.called
            True
        """
        args = (self.client, self.tracking_index)
        kwargs = {"settings": index_settings(), "mappings": status_mappings()}
        debug.lv5(f"Args: {args}, Kwargs: {kwargs}")
        create_index(*args, **kwargs)
