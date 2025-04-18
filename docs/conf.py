"""Sphinx configuration for es-checkpoint documentation.

Configures Sphinx to generate documentation for the es-checkpoint module, using
autodoc, Napoleon, and doctest. Extracts metadata (__version__, __author__,
__copyright__) from __init__.py dynamically, supporting portable paths for the
source codebase.

Attributes:
    project (str): Project name ("es-checkpoint").
    module (str): Module name derived from project ("es_checkpoint").
    codebase (str): Path to source code ("../src/es_checkpoint").
"""

# pylint: disable=C0103,C0114,E0401,W0611,W0622

import sys
import os

# -- Project information -----------------------------------------------------

project = "es-checkpoint"
module = project.replace("-", "_")
codebase = f"../src/{module}"

# Extract metadata from __init__.py
path = f"{codebase}/__init__.py"
myinit = os.path.join(os.path.dirname(os.path.abspath(__file__)), path)
metadata = {
    "__version__": "",
    "__author__": "",
    "__copyright__": "",
}
with open(myinit, "r", encoding="utf-8") as file:
    for line in file:
        line = line.strip()
        if not line.startswith("__") or line.startswith("__all__"):
            continue
        for key in metadata:
            if line.startswith(key):
                if '"' in line:
                    metadata[key] = line.split('"')[1]
                elif "'" in line:
                    metadata[key] = line.split("'")[1]
                break

author = metadata["__author__"]
copyright = metadata["__copyright__"]
release = metadata["__version__"]
version = ".".join(release.split(".")[:2])

# -- General configuration ---------------------------------------------------

# Add module path for autodoc
sys.path.insert(0, os.path.abspath(codebase))

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx.ext.doctest",
    "sphinx.ext.intersphinx",
]
napoleon_google_docstring = True
napoleon_numpy_docstring = False

templates_path = ["_templates"]
exclude_patterns = ["_build"]
source_suffix = ".rst"
master_doc = "index"

# -- Options for HTML output -------------------------------------------------

pygments_style = "sphinx"
html_theme = "sphinx_rtd_theme" if os.environ.get("READTHEDOCS") != "True" else None

# -- Autodoc configuration ---------------------------------------------------

autoclass_content = "both"
autodoc_member_order = "bysource"
autodoc_default_options = {
    "members": True,
    "undoc-members": True,
    "show-inheritance": True,
}

# -- Intersphinx configuration -----------------------------------------------

intersphinx_mapping = {
    "python": ("https://docs.python.org/3.12", None),
    "elasticsearch8": ("https://elasticsearch-py.readthedocs.io/en/v8.18.0", None),
    "elastic-transport": (
        "https://elastic-transport-python.readthedocs.io/en/stable",
        None,
    ),
}
