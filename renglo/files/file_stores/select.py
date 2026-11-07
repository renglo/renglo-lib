"""Choose a file store.

Every subsystem follows CLOUD. Files passes ``files`` so a later
per-subsystem setting can be applied in ``cloud_for`` alone.
"""

from __future__ import annotations

import importlib

from renglo.data.data_stores.select import cloud_for

__all__ = ["cloud_for", "open_file_store"]


def open_file_store(config, subsystem, *, client=None, region=None):
    """Open the file store selected by ``cloud_for``."""
    cloud = cloud_for(config, subsystem)
    if cloud == "azure":
        module_name = "renglo.files.file_stores.file_store_azure"
    elif cloud == "aws":
        module_name = "renglo.files.file_stores.file_store_aws"
    else:
        raise NotImplementedError(f"No file store for cloud {cloud!r}")
    module = importlib.import_module(module_name)
    return module.FileStore(config, client=client, region=region)
