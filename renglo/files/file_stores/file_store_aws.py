"""S3 file store.

Method names match ``file_store_azure.FileStore``. This is the only files
module that imports boto3.
"""

from __future__ import annotations

from renglo.config_names import setting

_MISSING = frozenset({"NoSuchKey", "404", "NotFound"})


def _missing(exc):
    if type(exc).__name__ in _MISSING:
        return True
    response = getattr(exc, "response", None) or {}
    if not isinstance(response, dict):
        return False
    code = str((response.get("Error") or {}).get("Code") or "")
    return code in _MISSING


class FileStore:
    def __init__(self, config=None, *, client=None, region=None):
        self.config = config or {}
        self._injected = client
        self.region = region

    def _container(self, container=None):
        name = str(container or setting(self.config, "FILES_BUCKET", "") or "").strip()
        if not name:
            raise ValueError("FILES_BUCKET configuration is required")
        return name

    def _client(self, region=None):
        if self._injected is not None and not region:
            return self._injected
        import boto3

        chosen = region or self.region
        if chosen:
            return boto3.client("s3", region_name=chosen)
        return boto3.client("s3")

    def head(self, key, container=None, region=None):
        """Return True when the object exists. Missing objects return False."""
        try:
            self._client(region).head_object(Bucket=self._container(container), Key=key)
            return True
        except Exception as exc:
            if _missing(exc):
                return False
            raise

    def get(self, key, container=None, region=None):
        """Return ``(bytes, content_type)``, or None when the object is missing."""
        try:
            document = self._client(region).get_object(
                Bucket=self._container(container), Key=key
            )
        except Exception as exc:
            if _missing(exc):
                return None
            raise
        body = document["Body"].read()
        return body, document.get("ContentType") or ""

    def put(self, key, body, content_type=None, container=None, region=None):
        params = {"Bucket": self._container(container), "Key": key, "Body": body}
        if content_type:
            params["ContentType"] = content_type
        self._client(region).put_object(**params)

    def presign_get(self, key, expires_in, container=None, region=None):
        """Return a time-limited GET URL. Does not check that the object exists."""
        return self._client(region).generate_presigned_url(
            "get_object",
            Params={"Bucket": self._container(container), "Key": key},
            ExpiresIn=expires_in,
        )
