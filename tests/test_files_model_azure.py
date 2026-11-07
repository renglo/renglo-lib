"""File keys and object bytes are the same shape on each cloud."""

from __future__ import annotations

import io
import json
import unittest
from datetime import datetime


class _Missing(Exception):
    status_code = 404
    response = {"Error": {"Code": "NoSuchKey", "Message": "Not found"}}


class _DelegationKey:
    signed_oid = "oid"
    signed_tid = "tid"
    signed_start = "2020-01-01T00:00:00Z"
    signed_expiry = "2030-01-01T00:00:00Z"
    signed_service = "b"
    signed_version = "2020-02-10"
    value = "c2VjcmV0"


class _Blob:
    def __init__(self, service, key):
        self.service = service
        self.key = key
        self.body = None
        self.content_type = None
        self.url = f"https://example.blob.core.windows.net/data/{key}"

    def download_blob(self):
        self.service.downloads += 1
        if self.body is None:
            raise _Missing()
        return self

    def readall(self):
        return self.body

    def upload_blob(self, body, overwrite=False, content_settings=None):
        self.body = body.encode("utf-8") if isinstance(body, str) else body
        self.content_type = getattr(content_settings, "content_type", None)
        self.service.puts.append(self.key)

    def get_blob_properties(self):
        if self.body is None:
            raise _Missing()
        return {}


class _BlobService:
    def __init__(self):
        self.blobs = {}
        self.container = None
        self.key = None
        self.downloads = 0
        self.puts = []
        self.delegation_calls = 0

    def get_blob_client(self, container, key):
        self.container = container
        self.key = key
        blob = self.blobs.get((container, key))
        if blob is None:
            blob = _Blob(self, key)
            self.blobs[(container, key)] = blob
        return blob

    def get_user_delegation_key(self, key_start_time, key_expiry_time):
        self.delegation_calls += 1
        return _DelegationKey()


class _S3:
    def __init__(self):
        self.objects = {}
        self.gets = []
        self.puts = []
        self.presigns = []

    def put_object(self, Bucket, Key, Body, **kwargs):
        if isinstance(Body, str):
            Body = Body.encode()
        self.objects[(Bucket, Key)] = (bytes(Body), kwargs.get("ContentType") or "")
        self.puts.append(Key)
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}

    def get_object(self, Bucket, Key):
        self.gets.append(Key)
        found = self.objects.get((Bucket, Key))
        if found is None:
            raise _Missing()
        body, content_type = found
        return {"Body": io.BytesIO(body), "ContentType": content_type}

    def head_object(self, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            raise _Missing()
        return {}

    def generate_presigned_url(self, operation, Params, ExpiresIn):
        self.presigns.append(Params["Key"])
        return f"https://s3.example/{Params['Bucket']}/{Params['Key']}?exp={ExpiresIn}"


def _azure_model(service):
    from renglo.files.files_model import FilesModel

    return FilesModel(
        {
            "CLOUD": "azure",
            "FILES_BUCKET": "data",
            "AZURE_STORAGE_ACCOUNT": "example",
        },
        s3_client=service,
    )


def _aws_model(client):
    from renglo.files.files_model import FilesModel

    return FilesModel({"CLOUD": "aws", "FILES_BUCKET": "data"}, s3_client=client)


class AzureFilesTests(unittest.TestCase):
    def test_put_and_get_json_round_trip(self):
        service = _BlobService()
        model = _azure_model(service)
        model.put_json("auth/user/u1.json", {"user_id": "u1"}, default=str)
        self.assertEqual(service.container, "data")
        self.assertEqual(service.key, "auth/user/u1.json")
        self.assertEqual(model.get_json("auth/user/u1.json"), {"user_id": "u1"})
        body = service.blobs[("data", "auth/user/u1.json")].body
        self.assertEqual(json.loads(body.decode()), {"user_id": "u1"})

    def test_missing_object_returns_none(self):
        model = _azure_model(_BlobService())
        self.assertIsNone(model.get_json("auth/user/missing.json"))
        self.assertFalse(model.head("auth/user/missing.json"))

    def test_route_keys_and_presign_do_not_download(self):
        service = _BlobService()
        model = _azure_model(service)
        posted = model.a_b_post("p", "o", "ring", b"png", "image/png", "pic")
        thumb = model.user_thumbnail_post("abc123xyz", b"png")
        transient = model.tmp_post("p", "o", "entity", b"{}")
        today = datetime.now().strftime("%Y-%m-%d")

        self.assertEqual(posted["path"], "_files/p/o/ring/pic.png")
        self.assertEqual(thumb["path"], "auth/thumbnails/abc123xyz.png")
        self.assertTrue(transient["path"].startswith(f"_tmp/p/o/entity/{today}/"))
        self.assertEqual(
            service.puts,
            [
                "_files/p/o/ring/pic.png",
                "auth/thumbnails/abc123xyz.png",
                transient["path"],
            ],
        )

        signed = model.user_thumbnail_presign("abc123xyz")
        self.assertTrue(signed["success"])
        self.assertIn("auth/thumbnails/abc123xyz.png", signed["url"])
        self.assertIn("sig=", signed["url"])
        self.assertEqual(signed["path"], "auth/thumbnails/abc123xyz.png")
        self.assertEqual(service.downloads, 0)
        self.assertEqual(service.delegation_calls, 1)

        fetched = model.a_b_c_get("p", "o", "ring", "pic.png")
        self.assertEqual(fetched["content_type"], "image/png")
        self.assertEqual(service.downloads, 1)


class AwsFilesTests(unittest.TestCase):
    def test_route_keys_and_presign_do_not_download(self):
        client = _S3()
        model = _aws_model(client)
        posted = model.a_b_post("p", "o", "ring", b"png", "image/png", "pic")
        thumb = model.user_thumbnail_post("abc123xyz", b"png")
        transient = model.tmp_post("p", "o", "entity", b"{}")
        today = datetime.now().strftime("%Y-%m-%d")

        self.assertEqual(posted["path"], "_files/p/o/ring/pic.png")
        self.assertEqual(thumb["path"], "auth/thumbnails/abc123xyz.png")
        self.assertTrue(transient["path"].startswith(f"_tmp/p/o/entity/{today}/"))
        self.assertEqual(client.gets, [])

        signed = model.a_b_c_presign("p", "o", "ring", "pic.png")
        self.assertTrue(signed["success"])
        self.assertEqual(
            signed["url"],
            "https://s3.example/data/_files/p/o/ring/pic.png?exp=3600",
        )
        self.assertEqual(client.presigns, ["_files/p/o/ring/pic.png"])
        self.assertEqual(client.gets, [])

        client.objects[("data", "_files/p/o/ring/pic.png")] = (b"png", "binary/octet-stream")
        fetched = model.a_b_c_get("p", "o", "ring", "pic.png")
        self.assertEqual(fetched["content"], b"png")
        self.assertEqual(fetched["content_type"], "image/png")
        self.assertTrue(model.head("_files/p/o/ring/pic.png"))
        self.assertFalse(model.head("_files/p/o/ring/missing.png"))

        stored = client.objects[("data", transient["path"])]
        client.objects[("data", transient["path"])] = (stored[0], "binary/octet-stream")
        loaded = model.tmp_get("p", "o", "entity", today, transient["key"].rsplit("/", 1)[-1])
        self.assertEqual(loaded["content_type"], "application/json")
        self.assertEqual(loaded["content"], b"{}")


if __name__ == "__main__":
    unittest.main()
