"""Load the submodel for a Renglo model.

The application constructs FilesModel, ConnectionModel, SchdModel, and the
other Renglo classes. Each class calls ``load_submodel`` with its package
and file stem. ``CLOUD`` in config selects ``<stem>_<cloud>.py`` in that
package. The default is aws, which loads ``<stem>_aws.py``.
"""

from __future__ import annotations

import importlib

from renglo.config_names import setting


def load_submodel(package: str, stem: str, class_name: str, config=None):
    """Return the submodel class for this cloud.

    ``CLOUD=azure`` loads ``<stem>_azure``. ``CLOUD=supabase`` would load
    ``<stem>_supabase``. A missing module raises NotImplementedError.
    """
    cloud = str(setting(config, "CLOUD", "aws") or "aws").strip().lower()
    module_name = f"{package}.{stem}_{cloud}"
    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError as exc:
        raise NotImplementedError(f"No {class_name} submodel for {cloud!r}") from exc
    try:
        return getattr(module, class_name)
    except AttributeError as exc:
        raise NotImplementedError(f"{module_name} does not define {class_name}") from exc
