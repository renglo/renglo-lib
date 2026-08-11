"""
VectorController — application-facing API for live entity ANN (S3 Vectors).

Document RAG stays on RagController. This controller is for high-churn entity
vectors written by handlers (threat events, catalogs, campaigns, …).
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Union

from renglo.auth.auth_controller import AuthController
from renglo.auth.authorize import authorize
from renglo.vector_model import VectorModel


class VectorController:
    """Public amenity surface; delegates persistence to VectorModel."""

    def __init__(self, config: Optional[Dict[str, Any]] = None) -> None:
        self.config = config or {}
        self.VM = VectorModel(config=self.config)
        self.AUC = AuthController(config=self.config)

    @property
    def model(self) -> VectorModel:
        return self.VM

    def status(
        self,
        portfolio: str = "",
        org: str = "",
        **_kwargs: Any,
    ) -> Dict[str, Any]:
        base = self.VM.status()
        return {"success": True, "action": "status", **base, "portfolio": portfolio, "org": org}

    def embed_text(self, text: str) -> List[float]:
        return self.VM.embed_text(text)

    @authorize()
    def ensure_index(
        self,
        portfolio: str,
        org: str,
        index_name: str,
        dimension: int = 1024,
        **_kwargs: Any,
    ) -> Dict[str, Any]:
        try:
            result = self.VM.ensure_index(index_name, dimension=dimension)
            return {"success": True, "action": "ensure_index", **result}
        except Exception as exc:
            return {"success": False, "action": "ensure_index", "error": str(exc)}

    @authorize()
    def put_vector(
        self,
        portfolio: str,
        org: str,
        extension: str,
        index_name: str,
        entity_id: str,
        vector: Optional[Sequence[float]] = None,
        text: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **_kwargs: Any,
    ) -> Dict[str, Any]:
        try:
            ext = str(extension or "").strip()
            idx = str(index_name or "").strip()
            eid = str(entity_id or "").strip()
            if not ext or not idx or not eid:
                return {
                    "success": False,
                    "action": "put_vector",
                    "error": "extension, index_name, and entity_id are required",
                }
            vec: Sequence[float]
            if vector is not None:
                vec = vector
            elif text is not None:
                vec = self.VM.embed_text(str(text))
            else:
                return {
                    "success": False,
                    "action": "put_vector",
                    "error": "vector or text is required",
                }
            result = self.VM.put_vector(
                index_name=idx,
                entity_id=eid,
                portfolio=portfolio,
                org=org,
                extension=ext,
                vector=vec,
                metadata=metadata,
            )
            return {
                "success": True,
                "action": "put_vector",
                "dim": len(vec),
                "model": self.VM.embedding_model_id,
                **result,
            }
        except Exception as exc:
            return {"success": False, "action": "put_vector", "error": str(exc)}

    @authorize()
    def query(
        self,
        portfolio: str,
        org: str,
        extension: str,
        index_name: str,
        vector: Optional[Sequence[float]] = None,
        text: Optional[str] = None,
        top_k: int = 10,
        filters: Optional[Dict[str, Any]] = None,
        **_kwargs: Any,
    ) -> Dict[str, Any]:
        try:
            ext = str(extension or "").strip()
            idx = str(index_name or "").strip()
            if not ext or not idx:
                return {
                    "success": False,
                    "action": "query",
                    "error": "extension and index_name are required",
                }
            if vector is not None:
                vec: Sequence[float] = vector
            elif text is not None:
                vec = self.VM.embed_text(str(text))
            else:
                return {
                    "success": False,
                    "action": "query",
                    "error": "vector or text is required",
                }
            hits = self.VM.query(
                index_name=idx,
                portfolio=portfolio,
                org=org,
                extension=ext,
                vector=vec,
                top_k=top_k,
                extra_filter=filters if isinstance(filters, dict) else None,
            )
            return {
                "success": True,
                "action": "query",
                "hits": hits,
                "count": len(hits),
                "index": idx,
                "extension": ext,
            }
        except Exception as exc:
            return {"success": False, "action": "query", "error": str(exc)}

    @authorize()
    def delete_vector(
        self,
        portfolio: str,
        org: str,
        extension: str,
        index_name: str,
        entity_id: str,
        **_kwargs: Any,
    ) -> Dict[str, Any]:
        try:
            result = self.VM.delete_vector(
                index_name=str(index_name or "").strip(),
                entity_id=str(entity_id or "").strip(),
                portfolio=portfolio,
                org=org,
                extension=str(extension or "").strip(),
            )
            return {"success": True, "action": "delete_vector", **result}
        except Exception as exc:
            return {"success": False, "action": "delete_vector", "error": str(exc)}
