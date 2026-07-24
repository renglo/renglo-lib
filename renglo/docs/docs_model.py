"""Compatibility shim: DocsModel is FilesModel under the old name."""

from renglo.files.files_model import FilesModel as DocsModel

__all__ = ["DocsModel"]
 