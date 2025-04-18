Changelog
=========

All notable changes to the `es-checkpoint` module are documented here.

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
