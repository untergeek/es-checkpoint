"""es-checkpoint utils module.

Provides utility functions for storage operations and progress tracking.
"""

import typing as t
from datetime import datetime, timezone
import json
import logging
from .debug import debug, begin_end
from .exceptions import FatalError, MissingDocument
from .storage import StorageBackend

logger = logging.getLogger(__name__)

if t.TYPE_CHECKING:
    from . import Job, Task


@begin_end()
def do_search(
    backend: StorageBackend,
    index_pattern: str,
    query: t.Dict,
    size: int = 0,
    aggs: t.Optional[t.Dict] = None,
) -> t.List[t.Dict]:
    """Performs a search query against a storage backend.

    Args:
        backend: Storage backend for document operations.
        index_pattern: Index name, CSV list, or pattern.
        query: Search query in backend-specific format.
        size: Maximum number of results (default: 0).
        aggs: Optional aggregation query (default: None).

    Returns:
        list[dict]: List of matching documents.

    Examples:
        >>> from unittest.mock import Mock
        >>> backend = Mock()
        >>> backend.search.return_value = [{"field": "value"}]
        >>> results = do_search(backend, "test_idx", {"query": {"match_all": {}}})
        >>> len(results)
        1
        >>> results[0]["field"]
        'value'
    """
    debug.lv5(f"Search query = {query}")
    kwargs = {"aggs": aggs} if aggs else {}
    response = backend.search(index_pattern, query, size, **kwargs)
    debug.lv5(f"Return value = {response}")
    return response


@begin_end()
def config_fieldmap(
    rw_val: t.Literal["read", "write"],
    key: t.Literal[
        "pattern",
        "query",
        "fields",
        "message",
        "expected_docs",
        "restore_settings",
        "delete",
    ],
) -> t.Union[str, int, object]:
    """Maps configuration fields to serialization/deserialization functions.

    Args:
        rw_val: Operation type ("read" or "write").
        key: Configuration field name.

    Returns:
        t.Union[str, int, object]: Function or type for the field.

    Examples:
        >>> config_fieldmap("read", "query")  # doctest: +ELLIPSIS
        <function loads at ...>
        >>> config_fieldmap("write", "message")
        <class 'str'>
    """
    debug.lv5("Getting config fieldmap")
    which = {
        "read": {
            "pattern": json.loads,
            "query": json.loads,
            "fields": json.loads,
            "message": str,
            "expected_docs": int,
            "restore_settings": json.loads,
            "delete": str,
        },
        "write": {
            "pattern": json.dumps,
            "query": json.dumps,
            "fields": json.dumps,
            "message": str,
            "expected_docs": int,
            "restore_settings": json.dumps,
            "delete": str,
        },
    }
    debug.lv5(f"Return value = {which}")
    return which[rw_val][key]


@begin_end()
def create_index(
    backend: StorageBackend,
    name: str,
    mappings: t.Union[t.Dict, None] = None,
    settings: t.Union[t.Dict, None] = None,
) -> None:
    """Ensures an index exists in the storage backend.

    Args:
        backend: Storage backend for document operations.
        name: Index name.
        mappings: Index mappings (default: None).
        settings: Index settings (default: None).

    Examples:
        >>> from unittest.mock import Mock
        >>> backend = Mock()
        >>> backend.ensure_index.return_value = None
        >>> create_index(backend, "test_idx")
        >>> backend.ensure_index.called
        True
    """
    debug.lv3(f"Creating index: {name}")
    backend.ensure_index(name, mappings=mappings, settings=settings)
    debug.lv5(f"Index {name} ensured")


@begin_end()
def get_progress_doc(
    job: t.Optional["Job"] = None,
    task: t.Optional["Task"] = None,
    task_id: t.Optional[str] = None,
    stepname: t.Optional[str] = None,
) -> t.Dict:
    """Retrieves a progress document for a Task or Step.

    Args:
        job: Job object (required if task is None).
        task: Task object (required if job is None).
        task_id: Task ID (required if job is provided).
        stepname: Step name (required if task is provided).

    Returns:
        dict: Progress document from the tracking index.

    Raises:
        FatalError: If neither or both job and task are provided.
        ValueError: If task_id or stepname is missing when required.

    Examples:
        >>> from unittest.mock import Mock
        >>> backend = Mock()
        >>> backend.get.return_value = {"task": "task1"}
        >>> job = Mock(backend=backend, tracking_index="idx", name="job1")
        >>> get_progress_doc(job=job, task_id="task1")
        {'task': 'task1'}
    """
    debug.lv2("Starting function...")
    if (job is None and task is None) or (job is not None and task is not None):
        msg = "Must provide either a job or a task"
        logger.critical(msg)
        debug.lv3("Exiting function, raising exception")
        debug.lv5("Exception: ValueError")
        raise FatalError(msg, errors=ValueError(msg))
    if job:
        debug.lv3(f"Getting progress doc for task: {task}")
        stepname = ""
        if not task_id:
            msg = "No value provided for task_id"
            logger.critical(msg)
            raise ValueError(msg)
    if task:
        debug.lv3(f"Getting progress doc for step: {stepname}")
        job = task.job
        task_id = task.task_id
        if not stepname:
            msg = "No value provided for stepname"
            logger.critical(msg)
            raise ValueError(msg)
    backend = job.backend
    tracking_idx = job.tracking_index
    job_id = job.name
    retval = progress_doc_req(backend, tracking_idx, job_id, task_id, stepname=stepname)
    debug.lv5(f"Return value = {retval}")
    return retval


@begin_end()
def get_tracking_doc(backend: StorageBackend, name: str, job_id: str) -> t.Dict:
    """Retrieves a progress tracking document for a job.

    Args:
        backend: Storage backend for document operations.
        name: Tracking index name.
        job_id: Job ID for the tracking document.

    Returns:
        dict: Tracking document from the index.

    Raises:
        MissingIndex: If the tracking index does not exist.
        MissingDocument: If the document is not found.

    Examples:
        >>> from unittest.mock import Mock
        >>> backend = Mock()
        >>> backend.get.return_value = {"job": "job1"}
        >>> get_tracking_doc(backend, "es-checkpoint", "job1")
        {'job': 'job1'}
    """
    debug.lv3(f"Getting tracking doc for {job_id}...")
    try:
        doc = backend.get(name, job_id)
        debug.lv5(f"backend.get response: {doc}")
        return doc
    except MissingDocument as err:
        msg = f"Tracking document for {job_id} does not exist"
        logger.critical(msg)
        debug.lv3("Exiting function, raising exception")
        debug.lv5(f"Exception: {msg}")
        raise MissingDocument(msg, index=name) from err


@begin_end(begin=5, end=5)
def now_iso8601() -> str:
    """Generates an ISO8601 timestamp.

    Returns:
        str: Current timestamp in ISO8601 format with Zulu notation.

    Examples:
        >>> from datetime import datetime
        >>> datetime.now = lambda tz=None: datetime(2023, 1, 1, tzinfo=timezone.utc)
        >>> now_iso8601()
        '2023-01-01T00:00:00Z'
    """
    parts = datetime.now(timezone.utc).isoformat().split("+")
    if len(parts) == 1:
        if parts[0][-1] == "Z":
            return parts[0]
        return f"{parts[0]}Z"
    if parts[1] == "00:00":
        return f"{parts[0]}Z"
    return f"{parts[0]}+{parts[1]}"


@begin_end()
def progress_doc_req(
    backend: StorageBackend,
    name: str,
    job_id: str,
    task_id: str,
    stepname: str = "",
) -> t.Dict:
    """Retrieves a task or step tracking document.

    Args:
        backend: Storage backend for document operations.
        name: Tracking index name.
        job_id: Job name for the tracking run.
        task_id: Task ID for the tracking document.
        stepname: Step name (default: "").

    Returns:
        dict: Progress tracking document.

    Raises:
        MissingIndex: If the tracking index does not exist.
        FatalError: If multiple tracking documents are found.
        MissingDocument: If no tracking document exists.

    Examples:
        >>> from unittest.mock import Mock
        >>> backend = Mock()
        >>> backend.search.return_value = [{"_source": {"task": "task1"}}]
        >>> progress_doc_req(backend, "es-checkpoint", "job1", "task1")
        {'_source': {'task': 'task1'}}
    """
    debug.lv3(f"Getting progress doc for {name}...")
    stub = f"Task: {task_id} of Job: {job_id}"
    query = {
        "bool": {
            "must": {"parent_id": {"type": "task", "id": job_id}},
            "filter": [],
        }
    }
    filters = [
        {"term": {"task": task_id}},
        {"term": {"job": job_id}},
    ]
    if not stepname:
        debug.lv2(f"Tracking progress for {stub}")
        query["bool"]["must_not"] = {"exists": {"field": "step"}}
    else:
        stub = f"Step: {stepname} of Task: {task_id} of Job: {job_id}"
        debug.lv2(f"Tracking progress for {stub}")
        filters.append({"term": {"step": stepname}})
    query["bool"]["filter"] = filters
    debug.lv4(f"Getting progress doc for {name} with query: {query}")
    result = do_search(backend, name, query)
    debug.lv5(f"do_search result: {result}")
    if len(result) > 1:
        msg = f"Tracking document for {stub} is not unique. This should never happen."
        logger.critical(msg)
        debug.lv3("Exiting function, raising exception")
        debug.lv5(f"Exception: {msg}")
        raise FatalError(msg)
    if not result:
        msg = f"Tracking document for {stub} does not exist"
        debug.lv3("Exiting function, raising exception")
        debug.lv5(f"Exception: {msg}")
        raise MissingDocument(msg, index=name)
    retval = result[0]
    debug.lv5(f"Return value = {retval}")
    return retval


@begin_end()
def parse_job_config(config: t.Dict, behavior: t.Literal["read", "write"]) -> t.Dict:
    """Parses raw job configuration.

    Deserializes or serializes JSON fields based on behavior ("read" or "write").

    Args:
        config: Raw configuration data.
        behavior: Operation type ("read" or "write").

    Returns:
        dict: Processed configuration dictionary.

    Examples:
        >>> config = {"query": '{"match_all": {}}', "message": "test"}
        >>> parse_job_config(config, "read")
        {'query': {'match_all': {}}, 'message': 'test'}
    """
    debug.lv3("Parsing job configuration")
    debug.lv5(f"config = {config}, behavior = {behavior}")
    fields = [
        "pattern",
        "query",
        "fields",
        "message",
        "restore_settings",
        "delete",
    ]
    doc = {}
    for field in fields:
        if field in config:
            func = config_fieldmap(behavior, field)
            doc[field] = func(config[field])
    debug.lv5(f"Return value = {doc}")
    return doc
