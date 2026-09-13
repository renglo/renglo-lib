"""
VectorModel — S3 Vectors backend for live entity ANN (put/query/delete/ensure).

One physical index per authorized {portfolio}/{org}. Keys are
{entity_type}/{entity_id}. Platform coordinates are stamped last onto metadata.
When S3_VECTORS_BUCKET is unset, uses a local JSON store for offline demos.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

log = logging.getLogger(__name__)

DEFAULT_EMBEDDING_MODEL = "amazon.titan-embed-text-v2:0"
DEFAULT_DIM = 1024
RESERVED_META_KEYS = frozenset(
    {"portfolio", "org", "entity_type", "entity_id", "updated_at", "attrs"}
)
ATTRS_KEY = "attrs"
_INDEX_SAFE = re.compile(r"[^a-z0-9.-]+")
_INDEX_DASHES = re.compile(r"-{2,}")


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


def org_index_name(portfolio: str, org: str) -> str:
    """Sanitize path tenancy into an S3 Vectors index name (3–63, [a-z0-9.-])."""
    raw = f"{str(portfolio or '').strip()}-{str(org or '').strip()}".lower()
    cleaned = _INDEX_DASHES.sub("-", _INDEX_SAFE.sub("-", raw)).strip(".-")
    if len(cleaned) < 3:
        cleaned = f"{cleaned or 'org'}-idx"
    if len(cleaned) > 63:
        digest = hashlib.sha1(cleaned.encode("utf-8")).hexdigest()[:8]
        cleaned = f"{cleaned[:54].rstrip('.-')}-{digest}"
    if cleaned[0] in ".-":
        cleaned = f"i{cleaned[1:]}"
    if cleaned[-1] in ".-":
        cleaned = f"{cleaned[:-1]}0"
    return cleaned


def vector_key(entity_type: str, entity_id: str) -> str:
    return f"{str(entity_type or '').strip()}/{str(entity_id or '').strip()}"


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _encode_attrs(attrs: Optional[Dict[str, Any]]) -> str:
    bag: Dict[str, Any] = {}
    if isinstance(attrs, dict):
        for key, value in attrs.items():
            name = str(key)
            if name in RESERVED_META_KEYS:
                continue
            bag[name] = value
    return json.dumps(bag, default=str, separators=(",", ":"))


def _decode_attrs(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, dict):
        return {str(k): v for k, v in raw.items() if str(k) not in RESERVED_META_KEYS}
    if not isinstance(raw, str) or not raw.strip():
        return {}
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    if not isinstance(parsed, dict):
        return {}
    return {str(k): v for k, v in parsed.items() if str(k) not in RESERVED_META_KEYS}


def _stamp_metadata(
    *,
    portfolio: str,
    org: str,
    entity_type: str,
    entity_id: str,
    attrs: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    return {
        "portfolio": str(portfolio or ""),
        "org": str(org or ""),
        "entity_type": str(entity_type or ""),
        "entity_id": str(entity_id or ""),
        "updated_at": _now_iso(),
        ATTRS_KEY: _encode_attrs(attrs),
    }


def _vector_from_data(data: Any) -> List[float]:
    if isinstance(data, list):
        return [float(x) for x in data]
    if isinstance(data, dict):
        raw = data.get("float32") or data.get("float16") or data.get("values")
        if isinstance(raw, list):
            return [float(x) for x in raw]
    return []


def _hit_from_row(
    *,
    key: Any,
    score: float,
    distance: Optional[float],
    meta: Dict[str, Any],
) -> Dict[str, Any]:
    attrs = _decode_attrs(meta.get(ATTRS_KEY))
    entity_type = str(meta.get("entity_type") or "")
    entity_id = str(meta.get("entity_id") or "")
    root = {
        "portfolio": meta.get("portfolio"),
        "org": meta.get("org"),
        "entity_type": entity_type,
        "entity_id": entity_id,
        "updated_at": meta.get("updated_at"),
    }
    return {
        "key": key,
        "score": score,
        "distance": distance,
        "entity_id": entity_id,
        "entity_type": entity_type,
        "portfolio": root["portfolio"],
        "org": root["org"],
        "updated_at": root["updated_at"],
        "attrs": attrs,
        "metadata": {**root, **attrs},
    }


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

    def index_name(self, portfolio: str, org: str) -> str:
        return org_index_name(portfolio, org)

    def status(self, portfolio: str = "", org: str = "") -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "backend": "local" if self.use_local else "s3vectors",
            "bucket": self.bucket or None,
            "embedding_model_id": self.embedding_model_id,
            "dimension": self.dimension,
            "region": self.region,
        }
        if portfolio and org:
            name = self.index_name(portfolio, org)
            out["index"] = name
            listed = self.list_vectors(portfolio=portfolio, org=org, max_results=500)
            items = list(listed.get("items") or [])
            counts: Dict[str, int] = {}
            for item in items:
                et = str(item.get("entity_type") or "") or "_unknown"
                counts[et] = counts.get(et, 0) + 1
            out["count"] = int(listed.get("count") or len(items))
            out["counts"] = counts
        return out

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

    def ensure_index(
        self,
        portfolio: str,
        org: str,
        dimension: Optional[int] = None,
    ) -> Dict[str, Any]:
        name = self.index_name(portfolio, org)
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
        create_kwargs: Dict[str, Any] = {
            "vectorBucketName": self.bucket,
            "indexName": name,
            "dataType": "float32",
            "dimension": dim,
            "distanceMetric": "cosine",
            "metadataConfiguration": {"nonFilterableMetadataKeys": [ATTRS_KEY]},
        }
        try:
            client.create_index(**create_kwargs)
        except Exception as exc:
            log.warning("create_index with nonFilterable attrs failed (%s); retrying plain", exc)
            create_kwargs.pop("metadataConfiguration", None)
            client.create_index(**create_kwargs)
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
        portfolio: str,
        org: str,
        entity_type: str,
        entity_id: str,
        vector: Sequence[float],
        attrs: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        et = str(entity_type or "").strip()
        eid = str(entity_id or "").strip()
        if not et or not eid:
            raise ValueError("entity_type and entity_id are required")
        ensured = self.ensure_index(portfolio, org)
        name = str(ensured.get("index") or self.index_name(portfolio, org))
        key = vector_key(et, eid)
        meta = _stamp_metadata(
            portfolio=portfolio,
            org=org,
            entity_type=et,
            entity_id=eid,
            attrs=attrs,
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

    def get_vector(
        self,
        *,
        portfolio: str,
        org: str,
        entity_type: str,
        entity_id: str,
    ) -> Dict[str, Any]:
        et = str(entity_type or "").strip()
        eid = str(entity_id or "").strip()
        if not et or not eid:
            raise ValueError("entity_type and entity_id are required")
        name = self.index_name(portfolio, org)
        key = vector_key(et, eid)
        missing = {
            "success": True,
            "found": False,
            "key": key,
            "index": name,
            "entity_type": et,
            "entity_id": eid,
            "portfolio": portfolio,
            "org": org,
        }
        if self.use_local:
            store = self._local_load(name)
            row = store.get(key)
            if not isinstance(row, dict):
                return {**missing, "backend": "local"}
            meta = row.get("metadata") or {}
            if str(meta.get("portfolio") or "") != str(portfolio):
                return {**missing, "backend": "local"}
            if str(meta.get("org") or "") != str(org):
                return {**missing, "backend": "local"}
            vec = [float(x) for x in (row.get("vector") or [])]
            hit = _hit_from_row(key=key, score=1.0, distance=0.0, meta=meta)
            return {
                "success": True,
                "found": True,
                "backend": "local",
                "index": name,
                "vector": vec,
                "dim": len(vec),
                **hit,
            }

        try:
            resp = self._get_s3vectors().get_vectors(
                vectorBucketName=self.bucket,
                indexName=name,
                keys=[key],
                returnData=True,
                returnMetadata=True,
            )
        except Exception as exc:
            if "ResourceNotFound" in type(exc).__name__ or "NotFound" in str(exc):
                return {**missing, "backend": "s3vectors"}
            raise
        items = resp.get("vectors") or resp.get("results") or []
        if not items:
            return {**missing, "backend": "s3vectors"}
        item = items[0] if isinstance(items[0], dict) else {}
        meta = item.get("metadata") or {}
        vec = _vector_from_data(item.get("data") or item.get("vector"))
        hit = _hit_from_row(key=item.get("key") or key, score=1.0, distance=0.0, meta=meta)
        return {
            "success": True,
            "found": True,
            "backend": "s3vectors",
            "index": name,
            "vector": vec,
            "dim": len(vec),
            **hit,
        }

    def delete_vector(
        self,
        *,
        portfolio: str,
        org: str,
        entity_type: str,
        entity_id: str,
    ) -> Dict[str, Any]:
        name = self.index_name(portfolio, org)
        key = vector_key(entity_type, entity_id)
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

    def purge_index(self, *, portfolio: str, org: str) -> Dict[str, Any]:
        name = self.index_name(portfolio, org)
        if self.use_local:
            path = self._local_root / f"{name}.json"
            if path.exists():
                path.unlink()
            self._ensured_indexes.discard(name)
            return {"success": True, "backend": "local", "index": name, "purged": True}
        try:
            self._get_s3vectors().delete_index(
                vectorBucketName=self.bucket,
                indexName=name,
            )
        except Exception as exc:
            log.warning("delete_index %s failed: %s", name, exc)
            raise
        self._ensured_indexes.discard(name)
        return {"success": True, "backend": "s3vectors", "index": name, "purged": True}

    def list_vectors(
        self,
        *,
        portfolio: str,
        org: str,
        entity_type: Optional[str] = None,
        max_results: int = 100,
        next_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        name = self.index_name(portfolio, org)
        et = str(entity_type or "").strip()
        limit = max(1, min(int(max_results or 100), 500))
        if self.use_local:
            store = self._local_load(name)
            items: List[Dict[str, Any]] = []
            for key, row in store.items():
                meta = row.get("metadata") or {}
                if str(meta.get("portfolio") or "") != str(portfolio):
                    continue
                if str(meta.get("org") or "") != str(org):
                    continue
                if et and str(meta.get("entity_type") or "") != et:
                    continue
                items.append(
                    _hit_from_row(key=key, score=0.0, distance=None, meta=meta)
                )
            items.sort(key=lambda r: str(r.get("key") or ""))
            start = 0
            if next_token:
                try:
                    start = max(0, int(next_token))
                except (TypeError, ValueError):
                    start = 0
            page = items[start : start + limit]
            nxt = str(start + limit) if start + limit < len(items) else None
            return {
                "success": True,
                "backend": "local",
                "index": name,
                "items": page,
                "count": len(items),
                "next_token": nxt,
            }

        client = self._get_s3vectors()
        collected: List[Dict[str, Any]] = []
        token = next_token or None
        pages = 0
        while pages < 20 and len(collected) < limit:
            kwargs: Dict[str, Any] = {
                "vectorBucketName": self.bucket,
                "indexName": name,
                "maxResults": min(limit, 100),
                "returnMetadata": True,
                "returnData": False,
            }
            if token:
                kwargs["nextToken"] = token
            try:
                resp = client.list_vectors(**kwargs)
            except Exception as exc:
                if "ResourceNotFound" in type(exc).__name__ or "NotFound" in str(exc):
                    return {
                        "success": True,
                        "backend": "s3vectors",
                        "index": name,
                        "items": [],
                        "count": 0,
                        "next_token": None,
                    }
                raise
            for item in resp.get("vectors") or resp.get("results") or []:
                meta = item.get("metadata") or {}
                if str(meta.get("portfolio") or "") != str(portfolio):
                    continue
                if str(meta.get("org") or "") != str(org):
                    continue
                if et and str(meta.get("entity_type") or "") != et:
                    continue
                collected.append(
                    _hit_from_row(
                        key=item.get("key"),
                        score=0.0,
                        distance=None,
                        meta=meta,
                    )
                )
                if len(collected) >= limit:
                    break
            token = resp.get("nextToken")
            pages += 1
            if not token:
                break
        return {
            "success": True,
            "backend": "s3vectors",
            "index": name,
            "items": collected[:limit],
            "count": len(collected),
            "next_token": token,
        }

    def query(
        self,
        *,
        portfolio: str,
        org: str,
        vector: Sequence[float],
        entity_type: Optional[str] = None,
        top_k: int = 10,
        extra_filter: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        name = self.index_name(portfolio, org)
        et = str(entity_type or "").strip()
        top_k = max(1, min(int(top_k or 10), 100))
        allowed_extra = self._sanitize_extra_filter(extra_filter)
        if self.use_local:
            store = self._local_load(name)
            scored: List[Dict[str, Any]] = []
            for key, row in store.items():
                meta = row.get("metadata") or {}
                if not self._row_matches(meta, portfolio, org, et, allowed_extra):
                    continue
                score = _cosine(vector, row.get("vector") or [])
                scored.append(
                    _hit_from_row(
                        key=key,
                        score=score,
                        distance=1.0 - score,
                        meta=meta,
                    )
                )
            scored.sort(key=lambda r: float(r.get("score") or 0.0), reverse=True)
            return scored[:top_k]

        clauses: List[Dict[str, Any]] = [
            {"portfolio": {"$eq": str(portfolio)}},
            {"org": {"$eq": str(org)}},
        ]
        if et:
            clauses.append({"entity_type": {"$eq": et}})
        for fk, fv in allowed_extra.items():
            clauses.append({fk: {"$eq": fv}})
        metadata_filter: Dict[str, Any] = {"$and": clauses} if len(clauses) > 1 else clauses[0]

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
                _hit_from_row(
                    key=item.get("key"),
                    score=score,
                    distance=dist_f,
                    meta=meta,
                )
            )
        return out

    def _sanitize_extra_filter(self, extra_filter: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if not isinstance(extra_filter, dict):
            return {}
        clean: Dict[str, Any] = {}
        for key, value in extra_filter.items():
            name = str(key)
            if name in RESERVED_META_KEYS:
                continue
            clean[name] = value
        return clean

    def _row_matches(
        self,
        meta: Dict[str, Any],
        portfolio: str,
        org: str,
        entity_type: str,
        extra_filter: Dict[str, Any],
    ) -> bool:
        if str(meta.get("portfolio") or "") != str(portfolio):
            return False
        if str(meta.get("org") or "") != str(org):
            return False
        if entity_type and str(meta.get("entity_type") or "") != entity_type:
            return False
        for fk, fv in extra_filter.items():
            if str(meta.get(fk) or "") != str(fv):
                return False
        return True
