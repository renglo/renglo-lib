"""
VectorController — application-facing API for live entity ANN (S3 Vectors).

Document RAG stays on RagController. This controller is for high-churn entity
vectors written by handlers (threat events, catalogs, campaigns, …).

Tenancy and index name come only from the authorized portfolio/org path.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

from renglo.auth.auth_controller import AuthController
from renglo.auth.authorize import authorize
from renglo.vector.vector_model import RESERVED_META_KEYS, VectorModel


class VectorController:
    """Public amenity surface; delegates persistence to VectorModel."""

    def __init__(self, config: Optional[Dict[str, Any]] = None) -> None:
        self.config = config or {}
        self.VM = VectorModel(config=self.config)
        self.AUC = AuthController(config=self.config)

    @property
    def model(self) -> VectorModel:
        return self.VM

    def embed_text(self, text: str) -> List[float]:
        return self.VM.embed_text(text)

    def _require_coords(
        self, action: str, portfolio: str, org: str, entity_type: str = "", entity_id: str = ""
    ) -> Optional[Dict[str, Any]]:
        if not str(portfolio or "").strip() or not str(org or "").strip():
            return {
                "success": False,
                "action": action,
                "error": "portfolio and org are required",
                "status": 400,
            }
        if entity_type is not None and entity_id is not None:
            if (entity_type == "" and entity_id) or (
                entity_id == ""
                and entity_type
                and action in {"put_vector", "delete_vector", "get_vector"}
            ):
                return {
                    "success": False,
                    "action": action,
                    "error": "entity_type and entity_id are required",
                    "status": 400,
                }
        return None

    def _reject_reserved_bag(self, action: str, bag: Optional[Dict[str, Any]], label: str) -> Optional[Dict[str, Any]]:
        if not isinstance(bag, dict):
            return None
        overlap = sorted(k for k in bag if str(k) in RESERVED_META_KEYS)
        if overlap:
            return {
                "success": False,
                "action": action,
                "error": f"{label} cannot include reserved keys: {', '.join(overlap)}",
                "status": 400,
            }
        return None

    @authorize()
    def status(
        self,
        portfolio: str,
        org: str,
        **_kwargs: Any,
    ) -> Dict[str, Any]:
        try:
            missing = self._require_coords("status", portfolio, org)
            if missing:
                return missing
            base = self.VM.status(portfolio=portfolio, org=org)
            return {"success": True, "action": "status", **base, "portfolio": portfolio, "org": org}
        except Exception as exc:
            return {"success": False, "action": "status", "error": str(exc), "status": 400}

    @authorize()
    def ensure_index(
        self,
        portfolio: str,
        org: str,
        dimension: int = 1024,
        **_kwargs: Any,
    ) -> Dict[str, Any]:
        try:
            missing = self._require_coords("ensure_index", portfolio, org)
            if missing:
                return missing
            result = self.VM.ensure_index(portfolio, org, dimension=dimension)
            return {"success": True, "action": "ensure_index", **result}
        except Exception as exc:
            return {"success": False, "action": "ensure_index", "error": str(exc), "status": 400}

    @authorize()
    def put_vector(
        self,
        portfolio: str,
        org: str,
        entity_type: str,
        entity_id: str,
        vector: Optional[Sequence[float]] = None,
        text: Optional[str] = None,
        attrs: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **_kwargs: Any,
    ) -> Dict[str, Any]:
        try:
            et = str(entity_type or "").strip()
            eid = str(entity_id or "").strip()
            if not et or not eid:
                return {
                    "success": False,
                    "action": "put_vector",
                    "error": "entity_type and entity_id are required",
                    "status": 400,
                }
            bag = attrs if isinstance(attrs, dict) else None
            if bag is None and isinstance(metadata, dict):
                rejected = self._reject_reserved_bag("put_vector", metadata, "metadata")
                if rejected:
                    return rejected
                bag = metadata
            else:
                rejected = self._reject_reserved_bag("put_vector", bag, "attrs")
                if rejected:
                    return rejected
            if vector is not None:
                vec: Sequence[float] = vector
            elif text is not None:
                vec = self.VM.embed_text(str(text))
            else:
                return {
                    "success": False,
                    "action": "put_vector",
                    "error": "vector or text is required",
                    "status": 400,
                }
            result = self.VM.put_vector(
                portfolio=portfolio,
                org=org,
                entity_type=et,
                entity_id=eid,
                vector=vec,
                attrs=bag,
            )
            return {
                "success": True,
                "action": "put_vector",
                "dim": len(vec),
                "model": self.VM.embedding_model_id,
                **result,
            }
        except Exception as exc:
            return {"success": False, "action": "put_vector", "error": str(exc), "status": 400}

    @authorize()
    def query(
        self,
        portfolio: str,
        org: str,
        entity_type: Optional[str] = None,
        vector: Optional[Sequence[float]] = None,
        text: Optional[str] = None,
        top_k: int = 10,
        filters: Optional[Dict[str, Any]] = None,
        **_kwargs: Any,
    ) -> Dict[str, Any]:
        try:
            missing = self._require_coords("query", portfolio, org)
            if missing:
                return missing
            if isinstance(filters, dict):
                rejected = self._reject_reserved_bag("query", filters, "filters")
                if rejected:
                    return rejected
            if vector is not None:
                vec: Sequence[float] = vector
            elif text is not None:
                vec = self.VM.embed_text(str(text))
            else:
                return {
                    "success": False,
                    "action": "query",
                    "error": "vector or text is required",
                    "status": 400,
                }
            et = str(entity_type or "").strip() or None
            hits = self.VM.query(
                portfolio=portfolio,
                org=org,
                entity_type=et,
                vector=vec,
                top_k=top_k,
                extra_filter=filters if isinstance(filters, dict) else None,
            )
            return {
                "success": True,
                "action": "query",
                "hits": hits,
                "count": len(hits),
                "index": self.VM.index_name(portfolio, org),
                "entity_type": et,
            }
        except Exception as exc:
            return {"success": False, "action": "query", "error": str(exc), "status": 400}

    @authorize()
    def delete_vector(
        self,
        portfolio: str,
        org: str,
        entity_type: str,
        entity_id: str,
        **_kwargs: Any,
    ) -> Dict[str, Any]:
        try:
            et = str(entity_type or "").strip()
            eid = str(entity_id or "").strip()
            if not et or not eid:
                return {
                    "success": False,
                    "action": "delete_vector",
                    "error": "entity_type and entity_id are required",
                    "status": 400,
                }
            result = self.VM.delete_vector(
                portfolio=portfolio,
                org=org,
                entity_type=et,
                entity_id=eid,
            )
            return {"success": True, "action": "delete_vector", **result}
        except Exception as exc:
            return {"success": False, "action": "delete_vector", "error": str(exc), "status": 400}

    @authorize()
    def get_vector(
        self,
        portfolio: str,
        org: str,
        entity_type: str,
        entity_id: str,
        **_kwargs: Any,
    ) -> Dict[str, Any]:
        try:
            et = str(entity_type or "").strip()
            eid = str(entity_id or "").strip()
            if not et or not eid:
                return {
                    "success": False,
                    "action": "get_vector",
                    "error": "entity_type and entity_id are required",
                    "status": 400,
                }
            result = self.VM.get_vector(
                portfolio=portfolio,
                org=org,
                entity_type=et,
                entity_id=eid,
            )
            return {
                "success": True,
                "action": "get_vector",
                "model": self.VM.embedding_model_id,
                **result,
            }
        except Exception as exc:
            return {"success": False, "action": "get_vector", "error": str(exc), "status": 400}

    @authorize()
    def list_vectors(
        self,
        portfolio: str,
        org: str,
        entity_type: Optional[str] = None,
        max_results: int = 100,
        next_token: Optional[str] = None,
        **_kwargs: Any,
    ) -> Dict[str, Any]:
        try:
            missing = self._require_coords("list", portfolio, org)
            if missing:
                return missing
            result = self.VM.list_vectors(
                portfolio=portfolio,
                org=org,
                entity_type=str(entity_type or "").strip() or None,
                max_results=max_results,
                next_token=next_token,
            )
            return {"success": True, "action": "list", **result}
        except Exception as exc:
            return {"success": False, "action": "list", "error": str(exc), "status": 400}

    @authorize()
    def purge_index(
        self,
        portfolio: str,
        org: str,
        confirm: bool = False,
        **_kwargs: Any,
    ) -> Dict[str, Any]:
        try:
            missing = self._require_coords("purge", portfolio, org)
            if missing:
                return missing
            if not confirm:
                return {
                    "success": False,
                    "action": "purge",
                    "error": "confirm=true is required to purge this org index",
                    "status": 400,
                }
            result = self.VM.purge_index(portfolio=portfolio, org=org)
            return {"success": True, "action": "purge", **result}
        except Exception as exc:
            return {"success": False, "action": "purge", "error": str(exc), "status": 400}
