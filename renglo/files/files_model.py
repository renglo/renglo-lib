"""Files stored in the bucket selected by CLOUD.

Callers construct this class. Object keys are the same on AWS and Azure.
The file store owns the SDK calls.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime

from renglo.files.file_stores.select import open_file_store
from renglo.logger import get_logger


class FilesModel:
    # HTTP GETs redirect to a signed URL. Signing does not read the object.
    PRESIGN_EXPIRES_IN = 3600

    USER_THUMBNAIL_PREFIX = "auth/thumbnails"

    _EXTENSION_TYPES = {
        "jpg": "image/jpeg",
        "jpeg": "image/jpeg",
        "png": "image/png",
        "svg": "image/svg+xml",
        "pdf": "application/pdf",
        "txt": "text/plain",
        "csv": "text/csv",
        "json": "application/json",
    }

    def __init__(self, config=None, tid=False, ip=False, s3_client=None):
        self.config = config or {}
        self.logger = get_logger()
        self.store = open_file_store(self.config, "files", client=s3_client)
        self.valid_types = {
            "image/jpeg": "jpg",
            "image/png": "png",
            "image/svg+xml": "svg",
            "application/pdf": "pdf",
            "text/plain": "txt",
            "text/csv": "csv",
            "application/json": "json",
        }

    @staticmethod
    def file_key(portfolio, org, ring, filename):
        return f"_files/{portfolio}/{org}/{ring}/{filename}"

    def user_thumbnail_key(self, handle: str) -> str:
        return f"{self.USER_THUMBNAIL_PREFIX}/{handle}.png"

    @staticmethod
    def tmp_key(portfolio, org, entity, date, object_id):
        return f"_tmp/{portfolio}/{org}/{entity}/{date}/{object_id}"

    def _resolved_type(self, stored, path, *, extensionless_json=False):
        content_type = stored or "binary/octet-stream"
        if content_type != "binary/octet-stream":
            return content_type
        last = str(path).rsplit("/", 1)[-1]
        if "." in last:
            extension = last.rsplit(".", 1)[-1].lower()
            return self._EXTENSION_TYPES.get(extension, "application/octet-stream")
        if extensionless_json:
            return "application/json"
        return "application/octet-stream"

    def head(self, key, bucket=None, region=None):
        """Return True when the object exists. Missing objects return False."""
        return self.store.head(key, container=bucket, region=region)

    def get_bytes(self, key, bucket=None, region=None):
        """Return object bytes, or None when the object is missing."""
        found = self.store.get(key, container=bucket, region=region)
        if found is None:
            return None
        body, _content_type = found
        return body

    def put_bytes(self, key, body, content_type=None, bucket=None, region=None):
        self.store.put(
            key,
            body,
            content_type=content_type,
            container=bucket,
            region=region,
        )

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

    def presign_get(self, file_path, expires_in=None):
        """Return a time-limited GET URL. Does not check that the object exists."""
        if expires_in is None:
            expires_in = self.PRESIGN_EXPIRES_IN
        try:
            url = self.store.presign_get(file_path, expires_in)
            return {"success": True, "url": url, "path": file_path}
        except Exception as exc:
            self.logger.error(f"Error presigning {file_path}: {str(exc)}")
            return {"success": False, "error": "Error generating download URL"}

    def a_b_c_presign(self, portfolio, org, ring, filename, expires_in=None):
        return self.presign_get(self.file_key(portfolio, org, ring, filename), expires_in)

    def user_thumbnail_presign(self, handle, expires_in=None):
        return self.presign_get(self.user_thumbnail_key(handle), expires_in)

    def tmp_presign(self, portfolio, org, entity, date, object_id, expires_in=None):
        return self.presign_get(
            self.tmp_key(portfolio, org, entity, date, object_id),
            expires_in,
        )

    def _storage_type(self, mime_type):
        return {
            "image/jpeg": "image/jpeg",
            "image/png": "image/png",
            "image/svg+xml": "image/svg+xml",
            "application/pdf": "application/pdf",
            "application/json": "application/json",
            "text/plain": "text/plain",
            "text/csv": "text/csv",
        }.get(mime_type, "application/octet-stream")

    def a_b_post(self, portfolio, org, ring, raw_doc, type, name):
        if not name:
            name = str(uuid.uuid4())
        filename = f"{name}.{self.valid_types[type]}"
        file_path = self.file_key(portfolio, org, ring, filename)
        try:
            self.store.put(file_path, raw_doc, content_type=self._storage_type(type))
            return {"success": True, "path": file_path, "id": name}
        except Exception as exc:
            self.logger.error(f"Error uploading file {file_path}: {str(exc)}")
            return {"success": False}

    def user_thumbnail_post(self, handle, raw_doc):
        file_path = self.user_thumbnail_key(handle)
        try:
            self.store.put(file_path, raw_doc, content_type="image/png")
            return {"success": True, "path": file_path, "id": handle}
        except Exception as exc:
            self.logger.error(f"Error uploading user thumbnail {file_path}: {str(exc)}")
            return {"success": False}

    def user_thumbnail_get(self, handle):
        file_path = self.user_thumbnail_key(handle)
        try:
            found = self.store.get(file_path)
        except Exception as exc:
            self.logger.error(f"Error retrieving user thumbnail: {str(exc)}")
            return {"success": False, "error": "Error retrieving file"}
        if found is None:
            return {"success": False, "error": "File not found"}
        content, stored = found
        return {
            "success": True,
            "content": content,
            "content_type": stored or "image/png",
        }

    def a_b_c_get(self, portfolio, org, ring, filename):
        file_path = self.file_key(portfolio, org, ring, filename)
        try:
            found = self.store.get(file_path)
        except Exception as exc:
            self.logger.error(f"Error retrieving file: {str(exc)}")
            return {"success": False, "error": "Error retrieving file"}
        if found is None:
            return {"success": False, "error": "File not found"}
        content, stored = found
        content_type = self._resolved_type(stored, filename)
        self.logger.info(f"Content Type: {content_type}")
        return {
            "success": True,
            "content": content,
            "content_type": content_type,
        }

    def tmp_post(self, portfolio, org, entity, raw_doc):
        date = datetime.now().strftime("%Y-%m-%d")
        object_id = str(uuid.uuid4())
        file_path = self.tmp_key(portfolio, org, entity, date, object_id)
        try:
            self.store.put(file_path, raw_doc, content_type="application/json")
            return {
                "success": True,
                "path": file_path,
                "key": f"{portfolio}/{org}/{entity}/{date}/{object_id}",
            }
        except Exception as exc:
            self.logger.error(f"Error uploading file {file_path}: {str(exc)}")
            return {"success": False}

    def tmp_get(self, portfolio, org, entity, date, object_id):
        file_path = self.tmp_key(portfolio, org, entity, date, object_id)
        try:
            found = self.store.get(file_path)
        except Exception as exc:
            self.logger.error(f"Error retrieving file: {str(exc)}")
            return {"success": False, "error": "Error retrieving file"}
        if found is None:
            return {"success": False, "error": "File not found"}
        content, stored = found
        content_type = self._resolved_type(stored, file_path, extensionless_json=True)
        self.logger.info(f"Content Type: {content_type}")
        return {"success": True, "content": content, "content_type": content_type}
