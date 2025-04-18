"""Storage backends for es-checkpoint.

Provides an abstract StorageBackend and concrete implementations for
Elasticsearch, local file, and in-memory storage.
"""

# pylint: disable=W0107
import typing as t
import json
import uuid
from abc import ABC, abstractmethod
from pathlib import Path
from elasticsearch8 import Elasticsearch
from .exceptions import MissingDocument, MissingIndex, ClientError


class StorageBackend(ABC):
    """Abstract base class for storage backends.

    Defines methods for saving, retrieving, and searching tracking documents.
    """

    @abstractmethod
    def save(self, index: str, doc_id: t.Optional[str], doc: t.Dict, **kwargs) -> str:
        """Saves or updates a document.

        Args:
            index: Index or container name.
            doc_id: Document ID (None for new documents).
            doc: Document data.
            **kwargs: Additional arguments (backend-specific).

        Returns:
            str: Assigned document ID.

        Raises:
            MissingIndex: If the index does not exist.
            ClientError: If the save operation fails.
        """
        pass

    @abstractmethod
    def get(self, index: str, doc_id: str) -> t.Dict:
        """Retrieves a document by ID.

        Args:
            index: Index or container name.
            doc_id: Document ID.

        Returns:
            dict: Document data.

        Raises:
            MissingIndex: If the index does not exist.
            MissingDocument: If the document is not found.
        """
        pass

    @abstractmethod
    def search(
        self, index: str, query: t.Dict, size: int = 0, **kwargs
    ) -> t.List[t.Dict]:
        """Searches documents in the index.

        Args:
            index: Index or container name.
            query: Search query (backend-specific format).
            size: Maximum number of results (0 for all).
            **kwargs: Additional arguments (backend-specific).

        Returns:
            list[dict]: List of matching documents.

        Raises:
            MissingIndex: If the index does not exist.
        """
        pass

    @abstractmethod
    def ensure_index(self, index: str, **kwargs) -> None:
        """Ensures an index exists, creating it if necessary.

        Args:
            index: Index or container name.
            **kwargs: Additional arguments (backend-specific).

        Raises:
            ClientError: If index creation fails.
        """
        pass


class ElasticsearchBackend(StorageBackend):
    """Elasticsearch storage backend.

    Uses an Elasticsearch client to store and retrieve tracking documents.

    Args:
        client: Elasticsearch client instance.

    Examples:
        >>> from unittest.mock import Mock
        >>> client = Mock(spec=Elasticsearch)
        >>> backend = ElasticsearchBackend(client)
        >>> backend.client == client
        True
    """

    def __init__(self, client: Elasticsearch):
        self.client = client

    def save(self, index: str, doc_id: t.Optional[str], doc: t.Dict, **kwargs) -> str:
        """Saves or updates a document in Elasticsearch.

        Args:
            index: Index name.
            doc_id: Document ID (None for new documents).
            doc: Document data.
            **kwargs: Additional arguments (e.g., routing).

        Returns:
            str: Assigned document ID.

        Raises:
            MissingIndex: If the index does not exist.
            ClientError: If the save operation fails.

        Examples:
            >>> from unittest.mock import Mock
            >>> client = Mock(spec=Elasticsearch)
            >>> client.indices.exists.return_value = True
            >>> client.update.return_value = {"result": "updated"}
            >>> client.index.return_value = {"_id": "doc1"}
            >>> backend = ElasticsearchBackend(client)
            >>> doc_id = backend.save("test_idx", None, {"field": "value"})
            >>> doc_id
            'doc1'
        """
        self.ensure_index(index)
        try:
            if doc_id:
                self.client.update(
                    index=index,
                    id=doc_id,
                    doc=doc,
                    doc_as_upsert=True,
                    refresh=True,
                    **kwargs,
                )
                return doc_id
            else:
                response = self.client.index(
                    index=index, document=doc, refresh=True, **kwargs
                )
                return response["_id"]
        except Exception as err:
            raise ClientError(f"Error saving document: {str(err)}", errors=err) from err

    def get(self, index: str, doc_id: str) -> t.Dict:
        """Retrieves a document by ID from Elasticsearch.

        Args:
            index: Index name.
            doc_id: Document ID.

        Returns:
            dict: Document data.

        Raises:
            MissingIndex: If the index does not exist.
            MissingDocument: If the document is not found.

        Examples:
            >>> from unittest.mock import Mock
            >>> client = Mock(spec=Elasticsearch)
            >>> client.indices.exists.return_value = True
            >>> client.get.return_value = {"_source": {"field": "value"}}
            >>> backend = ElasticsearchBackend(client)
            >>> doc = backend.get("test_idx", "doc1")
            >>> doc["field"]
            'value'
        """
        self.ensure_index(index)
        try:
            response = self.client.get(index=index, id=doc_id)
            return response["_source"]
        except Exception as err:
            raise MissingDocument(
                f"Document {doc_id} not found in {index}", index=index
            ) from err

    def search(
        self, index: str, query: t.Dict, size: int = 0, **kwargs
    ) -> t.List[t.Dict]:
        """Searches documents in Elasticsearch.

        Args:
            index: Index name.
            query: Elasticsearch DSL query.
            size: Maximum number of results (0 for all).
            **kwargs: Additional arguments (e.g., aggs).

        Returns:
            list[dict]: List of matching documents.

        Raises:
            MissingIndex: If the index does not exist.
            ClientError: If the search fails.

        Examples:
            >>> from unittest.mock import Mock
            >>> client = Mock(spec=Elasticsearch)
            >>> client.indices.exists.return_value = True
            >>> client.search.return_value = {
            ...     "hits": {"hits": [{"_source": {"field": "value"}}]}
            ... }
            >>> backend = ElasticsearchBackend(client)
            >>> results = backend.search("test_idx", {"query": {"match_all": {}}})
            >>> len(results)
            1
            >>> results[0]["field"]
            'value'
        """
        self.ensure_index(index)
        try:
            kwargs.update(
                {
                    "index": index,
                    "query": query,
                    "size": size,
                    "expand_wildcards": ["open", "hidden"],
                }
            )
            response = self.client.search(**kwargs)
            return [hit["_source"] for hit in response["hits"]["hits"]]
        except Exception as err:
            raise ClientError(
                f"Error searching documents: {str(err)}", errors=err
            ) from err

    def ensure_index(self, index: str, **kwargs) -> None:
        """Ensures an index exists in Elasticsearch, creating it if necessary.

        Args:
            index: Index name.
            **kwargs: Additional arguments (e.g., settings, mappings).

        Raises:
            ClientError: If index creation fails.

        Examples:
            >>> from unittest.mock import Mock
            >>> client = Mock(spec=Elasticsearch)
            >>> client.indices.exists.return_value = False
            >>> client.indices.create.return_value = {"acknowledged": True}
            >>> backend = ElasticsearchBackend(client)
            >>> backend.ensure_index("test_idx")
            >>> client.indices.create.called
            True
        """
        try:
            if not self.client.indices.exists(
                index=index, expand_wildcards=["open", "hidden"]
            ):
                settings = kwargs.get("settings")
                mappings = kwargs.get("mappings")
                self.client.indices.create(
                    index=index, settings=settings, mappings=mappings
                )
        except Exception as err:
            raise ClientError(f"Error ensuring index: {str(err)}", errors=err) from err


class FileBackend(StorageBackend):
    """Local file storage backend.

    Stores documents as JSON files with an index file for search efficiency.

    Args:
        base_path: Directory path for storing documents.

    Examples:
        >>> import tempfile
        >>> backend = FileBackend(tempfile.mkdtemp())
        >>> backend.base_path.exists()
        True
    """

    def __init__(self, base_path: str):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)

    def save(self, index: str, doc_id: t.Optional[str], doc: t.Dict, **kwargs) -> str:
        """Saves or updates a document as a JSON file.

        Args:
            index: Directory name (subdirectory).
            doc_id: Document ID (None for new documents).
            doc: Document data.
            **kwargs: Ignored (for compatibility).

        Returns:
            str: Assigned document ID.

        Raises:
            MissingIndex: If the index directory does not exist.
            ClientError: If the save operation fails.

        Examples:
            >>> import tempfile
            >>> backend = FileBackend(tempfile.mkdtemp())
            >>> backend.ensure_index("test_idx")
            >>> doc_id = backend.save("test_idx", None, {"field": "value"})
            >>> len(doc_id) > 0
            True
            >>> backend.save("test_idx", doc_id, {"field": "updated"})
            >>> with open(
            ...     backend.base_path / "test_idx" / f"{doc_id}.json",
            ...     encoding="utf-8"
            ... ) as f:
            ...     json.load(f)["field"]
            'updated'
        """
        self.ensure_index(index)
        index_path = self.base_path / index
        try:
            if not doc_id:
                doc_id = str(uuid.uuid4())
            file_path = index_path / f"{doc_id}.json"
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(doc, f)
            # Update index file
            index_file = index_path / "_index.json"
            index_data = {}
            if index_file.exists():
                with open(index_file, encoding="utf-8") as f:
                    index_data = json.load(f)
            index_data[doc_id] = {
                k: v for k, v in doc.items() if isinstance(v, (str, int, bool))
            }
            with open(index_file, "w", encoding="utf-8") as f:
                json.dump(index_data, f)
            return doc_id
        except Exception as err:
            raise ClientError(f"Error saving document: {str(err)}", errors=err) from err

    def get(self, index: str, doc_id: str) -> t.Dict:
        """Retrieves a document by ID from a JSON file.

        Args:
            index: Directory name.
            doc_id: Document ID (filename without .json).

        Returns:
            dict: Document data.

        Raises:
            MissingIndex: If the index directory does not exist.
            MissingDocument: If the document file is not found.

        Examples:
            >>> import tempfile
            >>> backend = FileBackend(tempfile.mkdtemp())
            >>> backend.ensure_index("test_idx")
            >>> doc_id = backend.save("test_idx", None, {"field": "value"})
            >>> doc = backend.get("test_idx", doc_id)
            >>> doc["field"]
            'value'
        """
        index_path = self.base_path / index
        if not index_path.exists():
            raise MissingIndex(f"Index {index} does not exist", index=index)
        file_path = index_path / f"{doc_id}.json"
        if not file_path.exists():
            raise MissingDocument(
                f"Document {doc_id} not found in {index}", index=index
            )
        try:
            with open(file_path, encoding="utf-8") as f:
                return json.load(f)
        except Exception as err:
            raise ClientError(
                f"Error retrieving document: {str(err)}", errors=err
            ) from err

    def search(
        self, index: str, query: t.Dict, size: int = 0, **kwargs
    ) -> t.List[t.Dict]:
        """Searches documents using an index file.

        Args:
            index: Directory name.
            query: Query dictionary (supports term matching).
            size: Maximum number of results (0 for all).
            **kwargs: Ignored (for compatibility).

        Returns:
            list[dict]: List of matching documents.

        Raises:
            MissingIndex: If the index directory does not exist.

        Examples:
            >>> import tempfile
            >>> backend = FileBackend(tempfile.mkdtemp())
            >>> backend.ensure_index("test_idx")
            >>> doc_id = backend.save("test_idx", None, {"field": "value"})
            >>> results = backend.search("test_idx", {"term": {"field": "value"}})
            >>> len(results)
            1
            >>> results[0]["field"]
            'value'
        """
        index_path = self.base_path / index
        if not index_path.exists():
            raise MissingIndex(f"Index {index} does not exist", index=index)
        index_file = index_path / "_index.json"
        if not index_file.exists():
            return []
        try:
            with open(index_file, encoding="utf-8") as f:
                index_data = json.load(f)
            results = []
            for doc_id, metadata in index_data.items():
                matches = True
                if "term" in query:
                    for key, value in query["term"].items():
                        if metadata.get(key) != value:
                            matches = False
                            break
                if matches:
                    with open(index_path / f"{doc_id}.json", encoding="utf-8") as f:
                        results.append(json.load(f))
            return results[:size] if size > 0 else results
        except Exception as err:
            raise ClientError(
                f"Error searching documents: {str(err)}", errors=err
            ) from err

    def ensure_index(self, index: str, **kwargs) -> None:
        """Ensures an index directory exists, creating it if necessary.

        Args:
            index: Directory name.
            **kwargs: Ignored (for compatibility).

        Examples:
            >>> import tempfile
            >>> backend = FileBackend(tempfile.mkdtemp())
            >>> backend.ensure_index("test_idx")
            >>> backend.base_path.joinpath("test_idx").exists()
            True
        """
        try:
            index_path = self.base_path / index
            index_path.mkdir(parents=True, exist_ok=True)
            # Initialize index file
            index_file = index_path / "_index.json"
            if not index_file.exists():
                with open(index_file, "w", encoding="utf-8") as f:
                    json.dump({}, f)
        except Exception as err:
            raise ClientError(f"Error ensuring index: {str(err)}", errors=err) from err


class InMemoryBackend(StorageBackend):
    """In-memory storage backend for testing or ephemeral use.

    Stores documents in a nested dictionary.

    Examples:
        >>> backend = InMemoryBackend()
        >>> backend.ensure_index("test_idx")
        >>> doc_id = backend.save("test_idx", None, {"field": "value"})
        >>> len(doc_id) > 0
        True
    """

    def __init__(self):
        self.store: t.Dict[str, t.Dict[str, t.Dict]] = {}

    def save(self, index: str, doc_id: t.Optional[str], doc: t.Dict, **kwargs) -> str:
        """Saves or updates a document in memory.

        Args:
            index: Index name.
            doc_id: Document ID (None for new documents).
            doc: Document data.
            **kwargs: Ignored (for compatibility).

        Returns:
            str: Assigned document ID.

        Raises:
            MissingIndex: If the index does not exist.

        Examples:
            >>> backend = InMemoryBackend()
            >>> backend.ensure_index("test_idx")
            >>> doc_id = backend.save("test_idx", None, {"field": "value"})
            >>> backend.save("test_idx", doc_id, {"field": "updated"})
            >>> backend.get("test_idx", doc_id)["field"]
            'updated'
        """
        self.ensure_index(index)
        if index not in self.store:
            raise MissingIndex(f"Index {index} does not exist", index=index)
        try:
            if not doc_id:
                doc_id = str(uuid.uuid4())
            self.store[index][doc_id] = doc
            return doc_id
        except Exception as err:
            raise ClientError(f"Error saving document: {str(err)}", errors=err) from err

    def get(self, index: str, doc_id: str) -> t.Dict:
        """Retrieves a document by ID from memory.

        Args:
            index: Index name.
            doc_id: Document ID.

        Returns:
            dict: Document data.

        Raises:
            MissingIndex: If the index does not exist.
            MissingDocument: If the document is not found.

        Examples:
            >>> backend = InMemoryBackend()
            >>> backend.ensure_index("test_idx")
            >>> doc_id = backend.save("test_idx", None, {"field": "value"})
            >>> doc = backend.get("test_idx", doc_id)
            >>> doc["field"]
            'value'
        """
        if index not in self.store:
            raise MissingIndex(f"Index {index} does not exist", index=index)
        if doc_id not in self.store[index]:
            raise MissingDocument(
                f"Document {doc_id} not found in {index}", index=index
            )
        return self.store[index][doc_id]

    def search(
        self, index: str, query: t.Dict, size: int = 0, **kwargs
    ) -> t.List[t.Dict]:
        """Searches documents in memory.

        Args:
            index: Index name.
            query: Query dictionary (supports term matching).
            size: Maximum number of results (0 for all).
            **kwargs: Ignored (for compatibility).

        Returns:
            list[dict]: List of matching documents.

        Raises:
            MissingIndex: If the index does not exist.

        Examples:
            >>> backend = InMemoryBackend()
            >>> backend.ensure_index("test_idx")
            >>> _ = backend.save("test_idx", None, {"field": "value"})
            >>> results = backend.search("test_idx", {"term": {"field": "value"}})
            >>> len(results)
            1
            >>> results[0]["field"]
            'value'
        """
        if index not in self.store:
            raise MissingIndex(f"Index {index} does not exist", index=index)
        results = []
        for _, doc in self.store[index].items():
            matches = True
            if "term" in query:
                for key, value in query["term"].items():
                    if doc.get(key) != value:
                        matches = False
                        break
            if matches:
                results.append(doc)
        return results[:size] if size > 0 else results

    def ensure_index(self, index: str, **kwargs) -> None:
        """Ensures an index exists in memory, creating it if necessary.

        Args:
            index: Index name.
            **kwargs: Ignored (for compatibility).

        Examples:
            >>> backend = InMemoryBackend()
            >>> backend.ensure_index("test_idx")
            >>> "test_idx" in backend.store
            True
        """
        if index not in self.store:
            self.store[index] = {}
