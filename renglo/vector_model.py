"""
VectorModel — S3 Vectors backend for live entity ANN (put/query/delete/ensure).

Controllers and handlers must not import boto3 for vectors; this model owns clients.
When S3_VECTORS_BUCKET is unset, uses a local JSON store for offline demos.
"""

from __future__ import annotations

import json
import logging
import math
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

log = logging.getLogger(__name__)

DEFAULT_EMBEDDING_MODEL = "amazon.titan-embed-text-v2:0"
DEFAULT_DIM = 1024


def _cosine(a: Sequence[float], b: Sequence[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = 0.0
    na = 0.0
    nb = 0.0
    for x, y in zip(a, b):
        fx = float(x)
        fy = float(y)
        dot += fx * fy
        na += fx * fx
        nb += fy * fy
    if na <= 0.0 or nb <= 0.0:
        return 0.0
    return max(-1.0, min(1.0, dot / (math.sqrt(na) * math.sqrt(nb))))


def _clean_metadata(meta: Dict[str, Any]) -> Dict[str, Any]:
    clean: Dict[str, Any] = {}
    for k, v in meta.items():
        if v is None:
            continue
        if isinstance(v, (str, int, float, bool)):
            clean[str(k)] = v
        elif isinstance(v, list):
            clean[str(k)] = ",".join(str(x) for x in v)
        else:
            clean[str(k)] = str(v)
    return clean


class VectorModel:
    """Persistence/backend for Amazon S3 Vectors (+ Bedrock Titan embed)."""

    def __init__(self, config: Optional[Dict[str, Any]] = None) -> None:
        self.config = config or {}
        self.region = str(
            self.config.get("AWS_REGION") or os.environ.get("AWS_REGION") or "us-east-1"
        )
        self.bucket = str(self.config.get("S3_VECTORS_BUCKET") or "").strip()
        self.embedding_model_id = str(
            self.config.get("EMBEDDING_MODEL_ID") or DEFAULT_EMBEDDING_MODEL
        ).strip()
        self.dimension = int(self.config.get("S3_VECTORS_DIMENSION") or DEFAULT_DIM)
        self._s3vectors = None
        self._bedrock_runtime = None
        self._local_root = Path(
            self.config.get("S3_VECTORS_LOCAL_DIR")
            or os.environ.get("S3_VECTORS_LOCAL_DIR")
            or "/tmp/renglo_s3_vectors"
        )
        self._ensured_indexes: set[str] = set()

    @property
    def use_local(self) -> bool:
        return not bool(self.bucket)

    def configured_indexes(self) -> Dict[str, str]:
        """Map env key → index name for all S3_VECTORS_INDEX_* config entries."""
        out: Dict[str, str] = {}
        for key, value in self.config.items():
            if not str(key).startswith("S3_VECTORS_INDEX_"):
                continue
            name = str(value or "").strip()
            if name:
                out[str(key)] = name
        return out

    def status(self) -> Dict[str, Any]:
        return {
            "backend": "local" if self.use_local else "s3vectors",
            "bucket": self.bucket or None,
            "indexes": self.configured_indexes(),
            "embedding_model_id": self.embedding_model_id,
            "dimension": self.dimension,
            "region": self.region,
        }

    def _get_s3vectors(self):
        if self._s3vectors is None:
            import boto3

            self._s3vectors = boto3.client("s3vectors", region_name=self.region)
        return self._s3vectors

    def _get_bedrock_runtime(self):
        if self._bedrock_runtime is None:
            import boto3

            self._bedrock_runtime = boto3.client("bedrock-runtime", region_name=self.region)
        return self._bedrock_runtime

    def embed_text(self, text: str) -> List[float]:
        body = json.dumps(
            {
                "inputText": str(text or "")[:8000],
                "dimensions": self.dimension,
                "normalize": True,
            }
        )
        try:
            resp = self._get_bedrock_runtime().invoke_model(
                modelId=self.embedding_model_id,
                contentType="application/json",
                accept="application/json",
                body=body,
            )
            payload = json.loads(resp["body"].read())
            vector = payload.get("embedding") or payload.get("embeddings") or []
            if isinstance(vector, dict):
                vector = vector.get("values") or []
            if not isinstance(vector, list) or not vector:
                raise RuntimeError(f"empty embedding from model {self.embedding_model_id}")
            return [float(x) for x in vector]
        except Exception as exc:
            if self.use_local:
                log.warning(
                    "Bedrock embed failed (%s); using deterministic local hash vector",
                    exc,
                )
                return self._local_hash_embedding(text)
            raise

    def _local_hash_embedding(self, text: str) -> List[float]:
        import hashlib

        seed = hashlib.sha256(str(text or "").encode("utf-8")).digest()
        values: List[float] = []
        buf = seed
        while len(values) < self.dimension:
            for b in buf:
                values.append(((b / 255.0) * 2.0) - 1.0)
                if len(values) >= self.dimension:
                    break
            buf = hashlib.sha256(buf).digest()
        norm = math.sqrt(sum(v * v for v in values)) or 1.0
        return [v / norm for v in values]

    def _local_path(self, index_name: str) -> Path:
        path = self._local_root / f"{index_name}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            path.write_text("{}", encoding="utf-8")
        return path

    def _local_load(self, index_name: str) -> Dict[str, Any]:
        return json.loads(self._local_path(index_name).read_text(encoding="utf-8") or "{}")

    def _local_save(self, index_name: str, data: Dict[str, Any]) -> None:
        self._local_path(index_name).write_text(json.dumps(data), encoding="utf-8")

    def ensure_index(self, index_name: str, dimension: Optional[int] = None) -> Dict[str, Any]:
        name = str(index_name or "").strip()
        if not name:
            raise ValueError("index_name is required")
        dim = int(dimension or self.dimension)
        if self.use_local:
            self._local_path(name)
            self._ensured_indexes.add(name)
            return {
                "success": True,
                "backend": "local",
                "index": name,
                "dimension": dim,
                "created": False,
            }
        if name in self._ensured_indexes:
            return {
                "success": True,
                "backend": "s3vectors",
                "index": name,
                "dimension": dim,
                "created": False,
            }
        client = self._get_s3vectors()
        try:
            client.get_index(vectorBucketName=self.bucket, indexName=name)
            self._ensured_indexes.add(name)
            return {
                "success": True,
                "backend": "s3vectors",
                "index": name,
                "dimension": dim,
                "created": False,
            }
        except Exception:
            pass
        client.create_index(
            vectorBucketName=self.bucket,
            indexName=name,
            dataType="float32",
            dimension=dim,
            distanceMetric="cosine",
        )
        self._ensured_indexes.add(name)
        return {
            "success": True,
            "backend": "s3vectors",
            "index": name,
            "dimension": dim,
            "created": True,
        }

    def put_vector(
        self,
        *,
        index_name: str,
        entity_id: str,
        portfolio: str,
        org: str,
        extension: str,
        vector: Sequence[float],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        name = str(index_name or "").strip()
        if not name:
            raise ValueError("index_name is required")
        self.ensure_index(name)
        key = f"{portfolio}/{org}/{extension}/{entity_id}"
        meta = _clean_metadata(
            {
                "portfolio": str(portfolio or ""),
                "org": str(org or ""),
                "extension": str(extension or ""),
                "entity_id": str(entity_id or ""),
                **(metadata or {}),
            }
        )
        if self.use_local:
            store = self._local_load(name)
            store[key] = {"vector": list(vector), "metadata": meta}
            self._local_save(name, store)
            return {"success": True, "backend": "local", "key": key, "index": name}

        self._get_s3vectors().put_vectors(
            vectorBucketName=self.bucket,
            indexName=name,
            vectors=[
                {
                    "key": key,
                    "data": {"float32": [float(x) for x in vector]},
                    "metadata": meta,
                }
            ],
        )
        return {"success": True, "backend": "s3vectors", "key": key, "index": name}

    def delete_vector(
        self,
        *,
        index_name: str,
        entity_id: str,
        portfolio: str,
        org: str,
        extension: str,
    ) -> Dict[str, Any]:
        name = str(index_name or "").strip()
        key = f"{portfolio}/{org}/{extension}/{entity_id}"
        if self.use_local:
            store = self._local_load(name)
            store.pop(key, None)
            self._local_save(name, store)
            return {"success": True, "backend": "local", "key": key, "index": name}
        self._get_s3vectors().delete_vectors(
            vectorBucketName=self.bucket,
            indexName=name,
            keys=[key],
        )
        return {"success": True, "backend": "s3vectors", "key": key, "index": name}

    def query(
        self,
        *,
        index_name: str,
        portfolio: str,
        org: str,
        extension: str,
        vector: Sequence[float],
        top_k: int = 10,
        extra_filter: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        name = str(index_name or "").strip()
        if not name:
            raise ValueError("index_name is required")
        top_k = max(1, min(int(top_k or 10), 100))
        if self.use_local:
            store = self._local_load(name)
            scored: List[Dict[str, Any]] = []
            for key, row in store.items():
                meta = row.get("metadata") or {}
                if str(meta.get("portfolio") or "") != str(portfolio):
                    continue
                if str(meta.get("org") or "") != str(org):
                    continue
                if str(meta.get("extension") or "") != str(extension):
                    continue
                if extra_filter:
                    skip = False
                    for fk, fv in extra_filter.items():
                        if str(meta.get(fk) or "") != str(fv):
                            skip = True
                            break
                    if skip:
                        continue
                score = _cosine(vector, row.get("vector") or [])
                scored.append(
                    {
                        "key": key,
                        "score": score,
                        "distance": 1.0 - score,
                        "metadata": meta,
                        "entity_id": meta.get("entity_id"),
                        "extension": meta.get("extension"),
                    }
                )
            scored.sort(key=lambda r: r["score"], reverse=True)
            return scored[:top_k]

        metadata_filter: Dict[str, Any] = {
            "$and": [
                {"portfolio": {"$eq": str(portfolio)}},
                {"org": {"$eq": str(org)}},
                {"extension": {"$eq": str(extension)}},
            ]
        }
        if extra_filter:
            for fk, fv in extra_filter.items():
                metadata_filter["$and"].append({str(fk): {"$eq": fv}})

        resp = self._get_s3vectors().query_vectors(
            vectorBucketName=self.bucket,
            indexName=name,
            queryVector={"float32": [float(x) for x in vector]},
            topK=top_k,
            filter=metadata_filter,
            returnMetadata=True,
            returnDistance=True,
        )
        out: List[Dict[str, Any]] = []
        for item in resp.get("vectors") or resp.get("results") or []:
            meta = item.get("metadata") or {}
            distance = item.get("distance")
            try:
                dist_f = float(distance) if distance is not None else None
            except (TypeError, ValueError):
                dist_f = None
            score = (1.0 - dist_f) if dist_f is not None else float(item.get("score") or 0.0)
            out.append(
                {
                    "key": item.get("key"),
                    "score": score,
                    "distance": dist_f,
                    "metadata": meta,
                    "entity_id": meta.get("entity_id"),
                    "extension": meta.get("extension"),
                }
            )
        return out
