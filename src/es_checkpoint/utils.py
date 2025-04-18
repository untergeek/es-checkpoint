"""es-checkpoint utils module.

Provides utility functions for Elasticsearch operations and progress tracking.
"""

# pylint: disable=W0212
import typing as t
from datetime import datetime, timezone
import json
import logging
from elasticsearch8.exceptions import (
    ApiError,
    BadRequestError,
    NotFoundError,
    TransportError,
)
from .tools.decorators import try_except
from .tools.handlers import es_response, missing_handler, tracker_handler
from .debug import debug, begin_end
from .exceptions import ClientError, FatalError, MissingIndex, MissingDocument

logger = logging.getLogger(__name__)

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch
    from elastic_transport import HeadApiResponse
    from . import Job, Task

EsExceptions = (ApiError, TransportError, BadRequestError)
Missing = (NotFoundError, MissingIndex, MissingDocument)


@try_except(exceptions=EsExceptions, handler=es_response)
@begin_end()
def do_search(
    client: "Elasticsearch",
    index_pattern: str,
    query: t.Dict,
    size: int = 0,
    aggs: t.Optional[t.Dict] = None,
) -> t.Dict:
    """Performs a search query against an Elasticsearch index pattern.

    Args:
        client: Elasticsearch client connection.
        index_pattern: Index name, CSV list, or pattern.
        query: Elasticsearch DSL search query.
        size: Maximum number of results (default: 0).
        aggs: Optional aggregation query (default: None).

    Returns:
        dict: Search response from Elasticsearch.

    Examples:
        >>> from unittest.mock import Mock
        >>> client = Mock()
        >>> client.search.return_value = {"hits": {"total": {"value": 1}}}
        >>> result = do_search(client, "test_idx", {"query": {"match_all": {}}})
        >>> result["hits"]["total"]["value"]
        1
    """
    kwargs = {
        "index": index_pattern,
        "query": query,
        "size": size,
        "expand_wildcards": ["open", "hidden"],
    }
    if aggs:
        kwargs.update({"aggs": aggs})
    debug.lv5(f"Search kwargs = {kwargs}")
    response = dict(client.search(**kwargs))
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


@try_except(exceptions=EsExceptions, handler=es_response)
@begin_end()
def create_index(
    client: "Elasticsearch",
    name: str,
    mappings: t.Union[t.Dict, None] = None,
    settings: t.Union[t.Dict, None] = None,
) -> None:
    """Creates an Elasticsearch index with mappings and settings.

    Args:
        client: Elasticsearch client connection.
        name: Index name.
        mappings: Index mappings (default: None).
        settings: Index settings (default: None).

    Examples:
        >>> from unittest.mock import Mock
        >>> client = Mock()
        >>> client.indices.exists.return_value = False
        >>> client.indices.create.return_value = {"acknowledged": True}
        >>> create_index(client, "test_idx")
        >>> client.indices.create.called
        True
    """
    debug.lv3(f"Creating index: {name}")
    if index_exists(client, name):
        debug.lv3(f"Index {name} already exists")
        return
    response = client.indices.create(index=name, settings=settings, mappings=mappings)
    debug.lv5(f"indices.create response: {response}")


@try_except(exceptions=(FatalError, ValueError), handler=tracker_handler)
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
        >>> job = Mock(client=Mock(), tracking_index="idx", name="job1")
        >>> job.client.get.return_value = {"_source": {"task": "task1"}}
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
    client = job.client
    tracking_idx = job.tracking_index
    job_id = job.name
    retval = progress_doc_req(client, tracking_idx, job_id, task_id, stepname=stepname)
    debug.lv5(f"Return value = {retval}")
    return retval


@try_except(exceptions=EsExceptions, handler=es_response)
@try_except(exceptions=Missing, handler=missing_handler)
@begin_end()
def get_tracking_doc(client: "Elasticsearch", name: str, job_id: str) -> t.Dict:
    """Retrieves a progress tracking document for a job.

    Args:
        client: Elasticsearch client connection.
        name: Tracking index name.
        job_id: Job ID for the tracking document.

    Returns:
        dict: Tracking document from the index.

    Raises:
        MissingIndex: If the tracking index does not exist.

    Examples:
        >>> from unittest.mock import Mock
        >>> client = Mock()
        >>> client.get.return_value = {"_source": {"job": "job1"}}
        >>> client.indices.exists.return_value = True
        >>> get_tracking_doc(client, "es-checkpoint", "job1")
        {'job': 'job1'}
    """
    debug.lv3(f"Getting tracking doc for {job_id}...")
    if not index_exists(client, name):
        msg = f"Tracking index {name} is missing"
        logger.critical(msg)
        debug.lv3("Exiting function, raising exception")
        debug.lv5(f"Exception: {msg}")
        raise MissingIndex(msg, index=name)
    doc = dict(client.get(index=name, id=job_id))
    debug.lv5(f"client.get response: {doc}")
    retval = doc["_source"]
    debug.lv5(f"Return value = {retval}")
    return retval


def index_exists(client: "Elasticsearch", name: str) -> "HeadApiResponse":
    """Tests whether an index exists."""
    debug.lv3(f"Checking if index {name} exists...")
    retval = client.indices.exists(index=name, expand_wildcards=["open", "hidden"])
    debug.lv5(f"Return value = {retval}")
    return retval


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


@try_except(exceptions=Missing, handler=missing_handler)
@begin_end()
def progress_doc_req(
    client: "Elasticsearch",
    name: str,
    job_id: str,
    task_id: str,
    stepname: str = "",
) -> t.Dict:
    """Retrieves a task or step tracking document.

    Args:
        client: Elasticsearch client connection.
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
        >>> client = Mock()
        >>> client.indices.exists.return_value = True
        >>> client.search.return_value = {
        ...     "hits": {
        ...         "total": {"value": 1},
        ...         "hits": [{"_source": {"task": "task1"}}]
        ...     }
        ... }
        >>> progress_doc_req(client, "es-checkpoint", "job1", "task1")
        {'_source': {'task': 'task1'}}
    """
    debug.lv3(f"Getting progress doc for {name}...")
    if not index_exists(client, name):
        msg = f"Tracking index {name} is missing"
        logger.critical(msg)
        debug.lv3("Exiting function, raising exception")
        debug.lv5(f"Exception: {msg}")
        raise MissingIndex(msg, index=name)
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
    result = do_search(client, index_pattern=name, query=query)
    debug.lv5(f"do_search result: {result}")
    if result["hits"]["total"]["value"] > 1:
        msg = f"Tracking document for {stub} is not unique. This should never happen."
        logger.critical(msg)
        debug.lv3("Exiting function, raising exception")
        debug.lv5(f"Exception: {msg}")
        raise FatalError(msg)
    if result["hits"]["total"]["value"] != 1:
        msg = f"Tracking document for {stub} does not exist"
        debug.lv3("Exiting function, raising exception")
        debug.lv5(f"Exception: {msg}")
        raise MissingDocument(msg, index=name)
    retval = result["hits"]["hits"][0]
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


@begin_end()
def update_doc(
    client: "Elasticsearch",
    index: str,
    doc_id: str,
    doc: t.Dict,
    routing: int = 0,
) -> None:
    """Upserts a document in an Elasticsearch index.

    Args:
        client: Elasticsearch client connection.
        index: Index to write to.
        doc_id: Document ID to update.
        doc: Document contents.
        routing: Routing value for parent/child relationships (default: 0).

    Raises:
        MissingIndex: If the index does not exist.
        ClientError: If the update operation fails.

    Examples:
        >>> from unittest.mock import Mock
        >>> client = Mock()
        >>> client.indices.exists.return_value = True
        >>> client.update.return_value = {"result": "updated"}
        >>> update_doc(client, "es-checkpoint", "doc1", {"field": "value"})
        >>> client.update.called
        True
    """
    if doc_id:
        debug.lv3(f"Updating document {doc_id} in index {index}")
    if not index_exists(client, index):
        msg = f"Index {index} does not exist"
        logger.critical(msg)
        debug.lv3("Exiting function, raising exception")
        debug.lv5(f"Exception: {msg}")
        raise MissingIndex(msg, index=index)
    try:
        debug.lv4("TRY: client.update()")
        if doc_id:
            _ = client.update(
                index=index,
                id=doc_id,
                doc=doc,
                doc_as_upsert=True,
                routing=str(routing),
                refresh=True,
            )
            debug.lv5(f"document update response: {_}")
        else:
            debug.lv5("No value for document id. Creating new document.")
            _ = client.index(
                index=index, document=doc, routing=str(routing), refresh=True
            )
    except (ApiError, TransportError, BadRequestError) as err:
        msg = f"Error updating document: {err.args[0]}"
        logger.error(msg)
        debug.lv3("Exiting function, raising exception")
        debug.lv5(f"Exception: {msg}")
        raise ClientError(msg, errors=err) from err
