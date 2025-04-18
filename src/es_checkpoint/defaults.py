"""App Defaults.

Provides default settings and mappings for the es-checkpoint tracking index.
"""

# pylint: disable=C0115,C0116
import typing as t

TRACKING_INDEX: str = "es-checkpoint"
"""Name of the index used for progress/status tracking."""


def index_settings() -> t.Dict[str, t.Dict[str, str]]:
    """Provides Elasticsearch index settings for the tracking index.

    Returns:
        dict: Dictionary of index settings.

    Examples:
        >>> settings = index_settings()
        >>> settings["index"]["number_of_shards"]
        '1'
        >>> settings["index"]["auto_expand_replicas"]
        '0-1'
    """
    return {
        "index": {
            "number_of_shards": "1",
            "auto_expand_replicas": "0-1",
        }
    }


def status_mappings() -> (
    t.Dict[str, t.Union[t.Dict[str, t.Any], t.List[t.Dict[str, t.Dict[str, t.Any]]]]]
):
    """Provides Elasticsearch index mappings for the tracking index.

    Returns:
        dict: Dictionary of index mappings.

    Examples:
        >>> mappings = status_mappings()
        >>> mappings["properties"]["job"]["type"]
        'keyword'
        >>> mappings["dynamic_templates"][0]["configuration"]["mapping"]["type"]
        'keyword'
    """
    return {
        "properties": {
            "job": {"type": "keyword"},
            "task": {"type": "keyword"},
            "step": {"type": "keyword"},
            "join_field": {"type": "join", "relations": {"job": "task"}},
            "cleanup": {"type": "keyword"},
            "completed": {"type": "boolean"},
            "end_time": {"type": "date"},
            "errors": {"type": "boolean"},
            "dry_run": {"type": "boolean"},
            "index": {"type": "keyword"},
            "logs": {"type": "text"},
            "start_time": {"type": "date"},
            "data": {"type": "wildcard"},
        },
        "dynamic_templates": [
            {
                "configuration": {
                    "path_match": "config.*",
                    "mapping": {"type": "keyword", "index": False},
                }
            }
        ],
    }
