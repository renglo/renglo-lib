from renglo.config_names import setting, table_name
from renglo.document_stores.select import StoreError, open_store
from renglo.logger import get_logger


class BlueprintModel:

    def __init__(
        self,
        config=None,
        tid=False,
        ip=False,
        *,
        dynamodb_resource=None,
        region_name=None,
        store=None,
    ):
        self.config = config or {}
        self.logger = get_logger()
        resolved_region = region_name or setting(self.config, "REGION", "us-east-1")
        resolved = table_name(self.config, "BLUEPRINT_TABLE", "default_blueprint_table")
        self.store = store or open_store(
            self.config,
            "blueprint",
            resolved,
            "irn",
            "version",
            resource=dynamodb_resource,
            region_name=resolved_region,
            use_profile=False,
        )

    def put_blueprint(self, data):
        try:
            self.store.put(data)
            return {"success": True, "message": "Document created", "document": data, "status": 201}
        except StoreError as exc:
            return {"success": False, "error": exc.message, "status": 500}

    def get_blueprint(self, handle, name, v):
        irn = "irn:blueprint:" + handle + ":" + name
        legacy_irn = "blueprint:" + handle + ":" + name
        self.logger.debug("Get Blueprint " + irn + " v:" + v)
        try:
            if v == "last":
                items, _token = self.store.query(irn, descending=True)
                if len(items) == 0:
                    items, _token = self.store.query(legacy_irn, descending=True)
                if len(items) == 0:
                    return {"success": False, "message": "Document not found"}
                item = items[0]
            else:
                item = self.store.get(irn, v)
                if not item:
                    item = self.store.get(legacy_irn, v)
            if item:
                return item
            return {"success": False, "message": "Document not found"}
        except StoreError as exc:
            return {"error": exc.message}

    def update_blueprint(self, data):
        try:
            self.store.put(data)
            return {"success": True, "message": "Document updated", "document": data, "status": 200}
        except StoreError as exc:
            return {"success": False, "error": exc.message, "status": 500}

    def delete_blueprint(self, handle, name, v):
        pk = "irn:blueprint:" + handle + ":" + name
        try:
            self.store.delete(pk, v)
            return {"success": True, "message": "Document deleted", "status": 200}
        except StoreError as exc:
            return {"success": False, "error": exc.message, "status": 500}
