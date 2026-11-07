"""Blob reads and writes used by auth trees."""

from __future__ import annotations

import json
import unittest


class _Missing(Exception):
    status_code = 404


class _Blob:
    def __init__(self):
        self.body = None
        self.content_type = None

    def download_blob(self):
        if self.body is None:
            raise _Missing()
        return self

    def readall(self):
        return self.body

    def upload_blob(self, body, overwrite=False, content_settings=None):
        self.body = body.encode("utf-8") if isinstance(body, str) else body
        self.content_type = getattr(content_settings, "content_type", None)


class _Service:
    def __init__(self):
        self.blob = _Blob()
        self.container = None
        self.key = None

    def get_blob_client(self, container, key):
        self.container = container
        self.key = key
        return self.blob


class AzureFilesTests(unittest.TestCase):
    def test_put_and_get_json_round_trip(self):
        from renglo.files.submodels.files_model_azure import FilesModel

        service = _Service()
        model = FilesModel(
            {"FILES_BUCKET": "data", "AZURE_STORAGE_ACCOUNT": "example"},
            s3_client=service,
        )
        model.put_json("auth/user/u1.json", {"user_id": "u1"}, default=str)
        self.assertEqual(service.container, "data")
        self.assertEqual(service.key, "auth/user/u1.json")
        self.assertEqual(model.get_json("auth/user/u1.json"), {"user_id": "u1"})
        self.assertEqual(json.loads(service.blob.body.decode()), {"user_id": "u1"})

    def test_missing_object_returns_none(self):
        from renglo.files.submodels.files_model_azure import FilesModel

        model = FilesModel({"FILES_BUCKET": "data"}, s3_client=_Service())
        self.assertIsNone(model.get_json("auth/user/missing.json"))
