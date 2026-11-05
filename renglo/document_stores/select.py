"""Choose a document store.

Every subsystem follows CLOUD. Each model passes its subsystem name so a
later per-subsystem setting can be applied in ``cloud_for`` alone.
"""

from __future__ import annotations

import importlib

from renglo.config_names import setting
from renglo.document_stores.errors import Conflict, PreconditionFailed, StoreError

__all__ = ["Conflict", "PreconditionFailed", "StoreError", "cloud_for", "open_store"]


def cloud_for(config, subsystem):
    """Return the store cloud for this subsystem.

    ``subsystem`` is required and currently unused. A later implementation
    can read a subsystem setting here and fall back to CLOUD.
    """
    if not subsystem:
        raise ValueError("subsystem is required")
    return str(setting(config, "CLOUD", "aws") or "aws").strip().lower() or "aws"


def open_store(config, subsystem, table_name, partition_key, sort_key, **kwargs):
    """Open the document store selected by ``cloud_for``."""
    cloud = cloud_for(config, subsystem)
    if cloud == "azure":
        module_name = "renglo.document_stores.document_store_azure"
        container = kwargs.get("container", kwargs.get("table"))
        kwargs = {"container": container} if container is not None else {}
    elif cloud == "aws":
        module_name = "renglo.document_stores.document_store_aws"
        kwargs.pop("container", None)
    else:
        raise NotImplementedError(f"No document store for cloud {cloud!r}")
    module = importlib.import_module(module_name)
    return module.DocumentStore(config, table_name, partition_key, sort_key, **kwargs)
