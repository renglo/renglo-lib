from renglo.config_names import setting
from renglo.document_stores.select import StoreError, open_store
from renglo.logger import get_logger


class StateModel:

    def __init__(self, config=None, tid=False, ip=False, *, store=None):
        self.config = config or {}
        self.logger = get_logger()
        table_name = setting(self.config, "BLUEPRINT_TABLE", "default_blueprint_table")
        self.store = store or open_store(
            self.config,
            "state",
            table_name,
            "irn",
            "version",
            region_name="us-east-1",
            use_profile=False,
        )

    def get_state(self, name, v):
        irn = "irn:state:irma:" + name
        self.logger.debug("Get State " + irn + " v:" + v)
        try:
            if v == "last":
                items, _token = self.store.query(irn, descending=True)
                if len(items) == 0:
                    return {"success": False, "message": "Document not found"}
                item = items[0]
            else:
                item = self.store.get(irn, v)
            if item:
                return item
            return {"success": False, "message": "Document not found"}
        except StoreError as exc:
            return {"error": exc.message}
