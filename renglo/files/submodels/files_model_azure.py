"""Azure submodel of FilesModel.

Auth trees and other JSON objects live in the private `data` container.
FILES_BUCKET is that container name. The account is AZURE_STORAGE_ACCOUNT
or AZURE_STORAGE_BLOB_ENDPOINT. Local runs sign in with the Azure CLI.
"""

from __future__ import annotations

import json

from renglo.config_names import setting


class FilesModel:
    def __init__(self, config=None, *_args, s3_client=None, **_kwargs):
        self.config = config if isinstance(config, dict) else {}
        self._injected = s3_client
        self._client = None

    def __getattr__(self, name):
        raise NotImplementedError("FilesModel has no Azure submodel yet")

    def _bucket_name(self):
        return self._container_name()

    def _container_name(self, bucket=None):
        name = str(bucket or setting(self.config, "FILES_BUCKET", "") or "").strip()
        if not name:
            raise ValueError("FILES_BUCKET configuration is required")
        return name

    def _endpoint(self):
        endpoint = str(setting(self.config, "AZURE_STORAGE_BLOB_ENDPOINT", "") or "").strip().rstrip("/")
        if endpoint:
            return endpoint
        account = str(setting(self.config, "AZURE_STORAGE_ACCOUNT", "") or "").strip()
        if account:
            return f"https://{account}.blob.core.windows.net"
        raise ValueError("AZURE_STORAGE_ACCOUNT or AZURE_STORAGE_BLOB_ENDPOINT is required")

    def _service(self):
        if self._injected is not None:
            return self._injected
        if self._client is None:
            from azure.storage.blob import BlobServiceClient
            from renglo.document_stores.document_store_azure import _credential

            self._client = BlobServiceClient(self._endpoint(), credential=_credential(self.config))
        return self._client

    def _blob(self, key, bucket=None):
        return self._service().get_blob_client(self._container_name(bucket), key)

    @staticmethod
    def _missing(exc):
        return getattr(exc, "status_code", None) == 404 or type(exc).__name__ == "ResourceNotFoundError"

    def get_bytes(self, key, bucket=None, region=None):
        """Return object bytes, or None when the object is missing."""
        try:
            return self._blob(key, bucket).download_blob().readall()
        except Exception as exc:
            if self._missing(exc):
                return None
            raise

    def put_bytes(self, key, body, content_type=None, bucket=None, region=None):
        settings = None
        if content_type:
            from azure.storage.blob import ContentSettings

            settings = ContentSettings(content_type=content_type)
        self._blob(key, bucket).upload_blob(body, overwrite=True, content_settings=settings)

    def get_json(self, key, bucket=None, region=None):
        raw = self.get_bytes(key, bucket=bucket, region=region)
        if raw is None:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode()
        return json.loads(raw)

    def put_json(self, key, document, bucket=None, region=None, content_type="application/json", default=None):
        body = json.dumps(document, default=default)
        self.put_bytes(key, body, content_type=content_type, bucket=bucket, region=region)
