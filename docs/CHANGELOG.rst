Changelog
=========

All notable changes to the `es-checkpoint` module are documented here.

0.0.10 (2025-04-18)
-------------------

Added
~~~~~
- Abstract storage backend in ``storage.py`` with ``StorageBackend`` base class
  and concrete implementations: ``ElasticsearchBackend`` for Elasticsearch,
  ``FileBackend`` for local JSON file storage, and ``InMemoryBackend`` for testing.
- ``update_status`` and ``get_status`` methods in ``Step`` to manage step status
  (e.g., "running", "completed") in the storage backend.
- Enhanced docstrings in ``step.py`` with detailed ``Attributes`` and ``Examples``
  sections, emphasizing storage backend usage.

Changed
~~~~~~~
- Replaced Elasticsearch ``client`` with ``StorageBackend`` in ``_parent.py``,
  ``job.py``, ``task.py``, ``step.py``, and ``utils.py``, enabling flexible storage
  backends (e.g., file, in-memory).
- Refactored ``utils.py`` to use ``StorageBackend`` methods (``get``, ``search``,
  ``ensure_index``) in ``do_search``, ``create_index``, ``get_progress_doc``,
  ``get_tracking_doc``, and ``progress_doc_req``.
- Removed ``index_exists`` and ``update_doc`` from ``utils.py``, as
  ``StorageBackend.ensure_index`` and ``save`` handle these operations.
- Updated doctests to mock ``StorageBackend`` instead of ``Elasticsearch`` across
  all modified files, ensuring backend-agnostic testing.
- Fixed doctest line length in ``job.py`` (``get_history``) for 80/88-character
  compliance.
- Removed unused ``ClientError`` and ``MissingIndex`` imports in ``utils.py`` for
  cleaner code.
- Incremented version to ``0.0.10`` in ``__init__.py`` to reflect storage backend
  abstraction.

Removed
~~~~~~~
- Elasticsearch-specific dependencies (e.g., ``elasticsearch8.exceptions``) from
  ``utils.py`` and ``job.py``, replaced by ``StorageBackend`` interface.

Notes
~~~~~
- The storage backend abstraction enables tracking in Elasticsearch, local files,
  or in-memory storage, enhancing flexibility for new use cases.
- Pending tasks include testing the storage backends, creating ``index.rst`` and
  ``_templates`` for ReadTheDocs, and removing obsolete files (``_child.py``,
  ``_base.py``).

0.0.9 (2025-04-18)
------------------

Added
~~~~~
- Comprehensive metadata in ``__init__.py``: ``__version__``, ``__author__``,
  ``__copyright__``, ``__license__``, ``__status__``, ``__description__``,
  ``__url__``, ``__email__``, ``__maintainer__``, ``__maintainer_email__``,
  ``__keywords__``, ``__classifiers__`` for package distribution and documentation.
- Google-style docstrings with doctests for functions/methods with >2 lines in
  ``exceptions.py``, ``_parent.py``, ``job.py``, ``task.py``, ``step.py``,
  ``utils.py``, ``defaults.py``, ``debug.py``, and ``__init__.py``.
- ``extra_fields`` method in ``Trackable`` (abstract), ``TaskOrStep``, ``Job``,
  ``Task``, and ``Step`` to provide class-specific fields for tracking documents.
- ``conf.py`` in ``docs`` folder for Sphinx/Napoleon, extracting metadata from
  ``__init__.py`` dynamically.
- ``tools/decorators.py``, ``tools/utils.py``, and ``tools/handlers.py`` submodules
  to replace ``common.py``, improving modularity.

Changed
~~~~~~~
- Merged ``_base.py`` and ``_child.py`` into ``_parent.py``, consolidating
  ``Trackable`` and ``TaskOrStep`` to reduce module sprawl.
- Centralized ``build_doc`` in ``Trackable`` to construct tracking documents from
  ``ATTRLIST`` and ``extra_fields``, removing ``build_doc`` from ``TaskOrStep``,
  ``Job``; confirmed no ``build_doc`` in ``Task``, ``Step``.
- Updated imports in ``job.py``, ``task.py``, ``step.py`` to use ``_parent.py``
  instead of ``_base.py`` or ``_child.py``.
- Updated imports in ``utils.py`` to use ``tools.decorators`` and
  ``tools.handlers`` instead of ``common.py``.
- Standardized line lengths to 80 (soft) and 88 (hard) limits across all files,
  using multiline doctests where needed for indentation.
- Switched single quotes to double quotes in ``__init__.py`` for consistency.
- Enhanced ``conf.py`` for portability with dynamic path construction
  (``../src/es_checkpoint``) and selective metadata extraction
  (``__author__``, ``__copyright__``, ``__version__``).

Removed
~~~~~~~
- ``common.py``, replaced by ``tools`` submodules.
- ``_base.py`` and ``_child.py``, merged into ``_parent.py`` (pending physical
  removal from project).

Notes
~~~~~
- Changes prepare the module for ReadTheDocs with standardized docstrings and
  Sphinx configuration.
- Pending tasks include testing, creating ``index.rst`` and ``_templates``,
  and removing obsolete files (``_child.py``, ``_base.py``).
