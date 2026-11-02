"""FilesModel interface.

Callers construct this class. CLOUD in config selects
renglo.files.submodels.files_model_<implementation>: aws, azure, or another submodel.
"""

from __future__ import annotations

from renglo.submodel import load_submodel

def _submodel(config):
    return load_submodel("renglo.files.submodels", "files_model", "FilesModel", config)


class FilesModel:
    # HTTP GETs redirect to a signed S3 URL. Signing is local HMAC; no GetObject.
    PRESIGN_EXPIRES_IN = 3600

    USER_THUMBNAIL_PREFIX = 'auth/thumbnails'

    def __init__(self, config=None, tid=False, ip=False, s3_client=None):
        cls = _submodel(config)
        object.__setattr__(self, "_impl", cls(config, tid, ip, s3_client))

    def _bucket_name(self):
        return self._impl._bucket_name()

    def _bucket(self, bucket=None):
        return self._impl._bucket(bucket)

    def _s3_client(self, region=None):
        return self._impl._s3_client(region)

    @staticmethod
    def _missing(exc):
        return _submodel(None)._missing(exc)

    def head(self, key, bucket=None, region=None):
        """Return True when the object exists. Missing objects return False."""
        return self._impl.head(key, bucket, region)

    def get_bytes(self, key, bucket=None, region=None):
        """Return object bytes, or None when the object is missing."""
        return self._impl.get_bytes(key, bucket, region)

    def put_bytes(self, key, body, content_type=None, bucket=None, region=None):
        return self._impl.put_bytes(key, body, content_type, bucket, region)

    def get_json(self, key, bucket=None, region=None):
        return self._impl.get_json(key, bucket, region)

    def put_json(self, key, document, bucket=None, region=None, content_type='application/json', default=None):
        return self._impl.put_json(key, document, bucket, region, content_type, default)

    def presign_get(self, file_path, expires_in=None):
        """Return a time-limited S3 GET URL. Does not check that the object exists."""
        return self._impl.presign_get(file_path, expires_in)

    def a_b_c_presign(self, portfolio, org, ring, filename, expires_in=None):
        return self._impl.a_b_c_presign(portfolio, org, ring, filename, expires_in)

    def user_thumbnail_presign(self, handle, expires_in=None):
        return self._impl.user_thumbnail_presign(handle, expires_in)

    def tmp_presign(self, portfolio, org, entity, date, object_id, expires_in=None):
        return self._impl.tmp_presign(portfolio, org, entity, date, object_id, expires_in)

    def a_b_post(self, portfolio, org, ring, raw_doc, type, name):
        return self._impl.a_b_post(portfolio, org, ring, raw_doc, type, name)

    def user_thumbnail_key(self, handle: str) -> str:
        return self._impl.user_thumbnail_key(handle)

    def user_thumbnail_post(self, handle, raw_doc):
        return self._impl.user_thumbnail_post(handle, raw_doc)

    def user_thumbnail_get(self, handle):
        return self._impl.user_thumbnail_get(handle)

    def a_b_c_get(self, portfolio, org, ring, filename):
        return self._impl.a_b_c_get(portfolio, org, ring, filename)

    def tmp_post(self, portfolio, org, entity, raw_doc):
        return self._impl.tmp_post(portfolio, org, entity, raw_doc)

    def tmp_get(self, portfolio, org, entity, date, object_id):
        return self._impl.tmp_get(portfolio, org, entity, date, object_id)

    def __getattr__(self, name):
        return getattr(object.__getattribute__(self, "_impl"), name)

    def __setattr__(self, name, value):
        if name == "_impl":
            object.__setattr__(self, name, value)
            return
        setattr(self._impl, name, value)
