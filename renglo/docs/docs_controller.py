"""Compatibility shim: DocsController is FilesController under the old name."""

from renglo.files.files_controller import FilesController as DocsController

__all__ = ["DocsController"]
