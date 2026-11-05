"""Load the submodel for a Renglo service that is not a document store.

Document models use ``renglo.document_stores.select.open_store`` instead.
Files, connections, schedule, vector, and the other service clients still
call ``load_submodel``. ``CLOUD`` selects ``<stem>_<cloud>.py``. The default
is aws.
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
