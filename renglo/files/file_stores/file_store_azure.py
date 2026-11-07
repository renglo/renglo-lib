"""Blob file store.

Auth trees, uploads, and thumbnails live in the private container named by
FILES_BUCKET. The account is AZURE_STORAGE_ACCOUNT or
AZURE_STORAGE_BLOB_ENDPOINT. Local runs sign in with the Azure CLI.

This is the only files module that imports the Azure storage SDK.
Downloads for the browser are a user-delegation SAS read URL. Signing does
not check that the blob exists.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from urllib.parse import quote

from renglo.config_names import setting

_MISSING = frozenset({"ResourceNotFoundError", "404", "NotFound"})


def _missing(exc):
    if getattr(exc, "status_code", None) == 404 or type(exc).__name__ in _MISSING:
        return True
    return False


class FileStore:
    def __init__(self, config=None, *, client=None, region=None):
        self.config = config if isinstance(config, dict) else {}
        self._injected = client
        self.region = region
        self._client = None

    def _container(self, container=None):
        name = str(container or setting(self.config, "FILES_BUCKET", "") or "").strip()
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

    def _account_name(self):
        account = str(setting(self.config, "AZURE_STORAGE_ACCOUNT", "") or "").strip()
        if account:
            return account
        host = self._endpoint().split("://", 1)[-1].split("/", 1)[0]
        name = host.split(".", 1)[0]
        if not name:
            raise ValueError("AZURE_STORAGE_ACCOUNT or AZURE_STORAGE_BLOB_ENDPOINT is required")
        return name

    def _service(self):
        if self._injected is not None:
            return self._injected
        if self._client is None:
            from azure.storage.blob import BlobServiceClient
            from renglo.data.data_stores.data_store_azure import _credential

            self._client = BlobServiceClient(self._endpoint(), credential=_credential(self.config))
        return self._client

    def _blob(self, key, container=None):
        return self._service().get_blob_client(self._container(container), key)

    def head(self, key, container=None, region=None):
        """Return True when the blob exists. Missing blobs return False."""
        blob = self._blob(key, container)
        try:
            blob.get_blob_properties()
            return True
        except Exception as exc:
            if _missing(exc):
                return False
            raise

    def get(self, key, container=None, region=None):
        """Return ``(bytes, content_type)``, or None when the blob is missing."""
        blob = self._blob(key, container)
        try:
            downloader = blob.download_blob()
        except Exception as exc:
            if _missing(exc):
                return None
            raise
        content_type = getattr(blob, "content_type", None) or ""
        properties = getattr(downloader, "properties", None)
        settings = getattr(properties, "content_settings", None)
        downloaded = getattr(settings, "content_type", None)
        if downloaded:
            content_type = downloaded
        return downloader.readall(), content_type or ""

    def put(self, key, body, content_type=None, container=None, region=None):
        settings = None
        if content_type:
            from azure.storage.blob import ContentSettings

            settings = ContentSettings(content_type=content_type)
        self._blob(key, container).upload_blob(body, overwrite=True, content_settings=settings)

    def presign_get(self, key, expires_in, container=None, region=None):
        """Return a time-limited GET URL. Does not check that the blob exists."""
        from azure.storage.blob import BlobSasPermissions, generate_blob_sas

        container_name = self._container(container)
        blob = self._blob(key, container_name)
        start = datetime.now(timezone.utc)
        expiry = start + timedelta(seconds=int(expires_in))
        delegation = self._service().get_user_delegation_key(
            key_start_time=start,
            key_expiry_time=expiry,
        )
        sas = generate_blob_sas(
            account_name=self._account_name(),
            container_name=container_name,
            blob_name=key,
            user_delegation_key=delegation,
            permission=BlobSasPermissions(read=True),
            expiry=expiry,
            start=start,
        )
        base = getattr(blob, "url", None)
        if not base:
            base = f"{self._endpoint()}/{container_name}/{quote(key, safe='/')}"
        separator = "&" if "?" in base else "?"
        return f"{base}{separator}{sas}"
