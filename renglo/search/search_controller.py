import json
import re
import unicodedata
import boto3
from urllib.parse import quote
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from renglo.logger import get_logger
from renglo.search.search_index_service import SearchIndexService
from renglo.search.search_model import SearchModel


class SearchConfigError(Exception):
    pass


class SearchValidationError(Exception):
    pass


class SearchStorageError(Exception):
    pass


class SearchController:
    DEFAULT_STOPWORDS = {
        "and",
        "the",
        "of",
        "for",
        "inc",
        "llc",
        "a",
        "an",
    }
    TITLE_FIELDS = {"title", "name", "label"}
    SHORT_TOKEN_FIELD_HINTS = {"sku", "code", "id", "zip", "iata", "icao"}

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        *,
        region_name: Optional[str] = None,
        dynamodb_resource: Optional[Any] = None,
    ):
        self.config = config or {}
        self.logger = get_logger()
        self.model: Optional[SearchModel] = None
        self.index_service: Optional[SearchIndexService] = None
        self.dynamodb_resource = dynamodb_resource
        self.region_name = region_name
        self._ring_data_table = None
        self.enabled = bool(self.config.get("DYNAMODB_SEARCH_TABLE"))
        if self.enabled:
            self.model = SearchModel(
                config=self.config,
                region_name=region_name,
                dynamodb_resource=dynamodb_resource,
            )
            self.index_service = SearchIndexService(
                config=self.config,
                dynamodb_resource=dynamodb_resource,
                region_name=region_name,
            )
        else:
            self.logger.warning("Search disabled: DYNAMODB_SEARCH_TABLE configuration not found")

    def is_enabled(self) -> bool:
        return self.enabled and self.model is not None

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _normalize_string(self, text: str) -> str:
        normalized = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
        normalized = normalized.lower()
        normalized = re.sub(r"[^\w\s]", " ", normalized)
        normalized = re.sub(r"\s+", " ", normalized).strip()
        return normalized

    def _tokenize_text(self, text: str) -> List[str]:
        if not isinstance(text, str) or not text.strip():
            return []
        normalized = self._normalize_string(text)
        if not normalized:
            return []
        return [part for part in normalized.split(" ") if part]

    def _normalize_exact_token(self, text: str) -> str:
        if not isinstance(text, str):
            return ""
        # Exact mode keeps content as-is (after trim) and URL-encodes reserved chars.
        # This preserves punctuation while staying safe for search key composition.
        return quote(text.strip(), safe="")

    def _field_allows_short_tokens(self, field_name: str) -> bool:
        field_name = str(field_name or "").lower()
        return any(hint in field_name for hint in self.SHORT_TOKEN_FIELD_HINTS)

    def _extract_doc_id(self, doc: Dict[str, Any]) -> str:
        return str(doc.get("id") or doc.get("_id") or "").strip()

    def _extract_blueprint_handle(self, doc: Dict[str, Any]) -> Optional[str]:
        blueprint_uri = doc.get("blueprint")
        if not isinstance(blueprint_uri, str) or "/_blueprint/" not in blueprint_uri:
            return None
        tail = blueprint_uri.split("/_blueprint/", 1)[1]
        parts = [p for p in tail.split("/") if p]
        if len(parts) < 2:
            return None
        return parts[0]

    def _extract_searchable_values(self, doc: Dict[str, Any], searchable_fields: Optional[List[str]]) -> Dict[str, str]:
        attrs = doc.get("attributes", doc)
        if not isinstance(attrs, dict):
            return {}

        # Important: None means "caller did not provide a list" (fallback to attrs keys).
        # An empty list means "explicitly index nothing".
        if searchable_fields is None:
            selected_fields = list(attrs.keys())
        else:
            selected_fields = searchable_fields
        out: Dict[str, str] = {}
        for field in selected_fields:
            if field not in attrs:
                continue
            raw = attrs.get(field)
            if raw is None:
                continue
            if isinstance(raw, str):
                value = raw
            elif isinstance(raw, (int, float, bool)):
                value = str(raw)
            else:
                value = json.dumps(raw, default=str)
            if value.strip():
                out[str(field)] = value
        return out

    def _build_index_rows(
        self,
        portfolio: str,
        org: str,
        ring: str,
        doc_id: str,
        extracted_values: Dict[str, str],
        *,
        field_weights: Optional[Dict[str, float]] = None,
        field_modes: Optional[Dict[str, str]] = None,
        source_updated_at: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        now = self._now_iso()
        for field, value in extracted_values.items():
            positions_by_token: Dict[str, List[int]] = defaultdict(list)
            mode = str((field_modes or {}).get(field, "text")).lower()

            if mode == "exact":
                exact_token = self._normalize_exact_token(value)
                if not exact_token:
                    continue
                positions_by_token[exact_token].append(0)
            else:
                tokens = self._tokenize_text(value)
                if not tokens:
                    continue
                for position, token in enumerate(tokens):
                    if token in self.DEFAULT_STOPWORDS:
                        continue
                    if len(token) < 3 and not self._field_allows_short_tokens(field):
                        continue
                    positions_by_token[token].append(position)

            for token, positions in positions_by_token.items():
                rows.append(
                    {
                        "portfolio_id": portfolio,
                        "org_id": org,
                        "ring": ring,
                        "doc_id": doc_id,
                        "field": field,
                        "token": token,
                        "token_count": len(positions),
                        "field_weight": float((field_weights or {}).get(field, 1.0)),
                        "positions": positions,
                        "source_updated_at": source_updated_at or now,
                    }
                )
        return rows

    def _resolve_target_rings(
        self,
        datatypes: Optional[List[str]],
        rings: Optional[List[str]],
        filters: Optional[Dict[str, Any]],
    ) -> List[str]:
        if isinstance(filters, dict):
            ring_filter = filters.get("rings")
            if isinstance(ring_filter, list):
                return [str(v).strip() for v in ring_filter if str(v).strip()]
            if isinstance(ring_filter, str) and ring_filter.strip():
                return [ring_filter.strip()]
        config_defaults = self.config.get("SEARCH_DEFAULT_RINGS")
        if isinstance(config_defaults, list):
            return [str(v).strip() for v in config_defaults if str(v).strip()]
        return []

    def _get_ring_data_table(self):
        if self._ring_data_table is not None:
            return self._ring_data_table
        table_name = self.config.get("DYNAMODB_RINGDATA_TABLE")
        if not table_name:
            return None
        dynamodb = self.dynamodb_resource or boto3.resource(
            "dynamodb",
            region_name=self.region_name or self.config.get("AWS_REGION", "us-east-1"),
        )
        self._ring_data_table = dynamodb.Table(table_name)
        return self._ring_data_table

    def _resolve_documents_for_hits(
        self,
        portfolio: str,
        org: str,
        hits: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        if not hits:
            return hits

        table = self._get_ring_data_table()
        if table is None:
            self.logger.warning("Search resolve requested but DYNAMODB_RINGDATA_TABLE is not configured")
            return hits

        keys = []
        key_to_hits: Dict[str, List[int]] = {}
        for index, hit in enumerate(hits):
            ring = str(hit.get("ring", "")).strip()
            doc_id = str(hit.get("doc_id", "")).strip()
            if not ring or not doc_id:
                continue
            doc_index = f"{org}:{ring}:{doc_id}"
            key = f"{portfolio}|{doc_index}"
            if key not in key_to_hits:
                keys.append({"portfolio_index": f"irn:data:{portfolio}", "doc_index": doc_index})
                key_to_hits[key] = []
            key_to_hits[key].append(index)

        if not keys:
            return hits

        client = table.meta.client
        table_name = table.name
        resolved_map: Dict[str, Dict[str, Any]] = {}

        for start in range(0, len(keys), 100):
            pending_keys = keys[start : start + 100]
            while pending_keys:
                response = client.batch_get_item(RequestItems={table_name: {"Keys": pending_keys}})
                for item in response.get("Responses", {}).get(table_name, []):
                    doc_index = str(item.get("doc_index", ""))
                    key = f"{portfolio}|{doc_index}"
                    resolved_map[key] = {
                        "_id": item.get("_id"),
                        "_modified": item.get("modified", ""),
                        "_index": item.get("path_index", ""),
                        "attributes": item.get("attributes", {}),
                        "blueprint": item.get("blueprint"),
                        "ring": doc_index.split(":")[1] if ":" in doc_index else None,
                    }
                pending_keys = response.get("UnprocessedKeys", {}).get(table_name, {}).get("Keys", [])

        resolved_hits: List[Dict[str, Any]] = []
        for hit in hits:
            ring = str(hit.get("ring", "")).strip()
            doc_id = str(hit.get("doc_id", "")).strip()
            doc_index = f"{org}:{ring}:{doc_id}"
            key = f"{portfolio}|{doc_index}"
            resolved_hits.append({**hit, "document": resolved_map.get(key)})
        return resolved_hits

    def _row_score(self, row: Dict[str, Any], token: str, boost_fields: Optional[Dict[str, float]]) -> float:
        field = str(row.get("field", ""))
        field_weight = float(row.get("field_weight", 1.0))
        token_count = int(row.get("token_count", 1))
        boost = float((boost_fields or {}).get(field, 1.0))
        score = field_weight * max(token_count, 1) * boost
        score += 1.5  # exact token match bonus
        if field in self.TITLE_FIELDS:
            score += 1.0
        return score

    def index_document(
        self,
        portfolio: str,
        org: str,
        ring: str,
        doc: dict,
        *,
        blueprint_handle: Optional[str] = None,
        searchable_fields: Optional[List[str]] = None,
        field_weights: Optional[Dict[str, float]] = None,
    ) -> Dict[str, Any]:
        if not self.is_enabled():
            return {"success": False, "message": "Search is not configured"}
        if not portfolio or not org or not ring:
            raise SearchValidationError("portfolio, org and ring are required")
        if not isinstance(doc, dict):
            raise SearchValidationError("doc must be a dictionary")

        doc_id = self._extract_doc_id(doc)
        if not doc_id:
            raise SearchValidationError("doc id is required (id or _id)")

        resolved_blueprint_handle = blueprint_handle or self._extract_blueprint_handle(doc)
        plan = (
            self.index_service.get_index_plan(ring, blueprint_handle=resolved_blueprint_handle)
            if self.index_service
            else {}
        )
        blueprint_searchable_fields = plan.get("searchable_fields", [])
        if searchable_fields:
            allowed = set(blueprint_searchable_fields)
            selected_fields = [field for field in searchable_fields if field in allowed]
        else:
            selected_fields = list(blueprint_searchable_fields)

        if not selected_fields:
            self.logger.debug(
                f"search index_document: no searchable fields defined in blueprint for ring={ring}. "
                f"doc_id={doc_id} will have index rows removed."
            )

        resolved_field_weights = dict(plan.get("field_weights") or {})
        resolved_field_modes = dict(plan.get("field_modes") or {})
        if field_weights:
            for key, value in field_weights.items():
                if key in selected_fields:
                    resolved_field_weights[key] = float(value)

        extracted_values = self._extract_searchable_values(doc, selected_fields)
        rows = self._build_index_rows(
            portfolio,
            org,
            ring,
            doc_id,
            extracted_values,
            field_weights=resolved_field_weights,
            field_modes=resolved_field_modes,
            source_updated_at=str(doc.get("modified") or doc.get("updated_at") or self._now_iso()),
        )

        try:
            deleted_rows = self.model.delete_document_rows(portfolio, org, ring, doc_id)
            indexed_rows = self.model.put_index_rows(rows)
        except Exception as exc:
            self.logger.error(f"search index_document failed for {portfolio}/{org}/{ring}/{doc_id}: {exc}")
            raise SearchStorageError(str(exc)) from exc

        return {
            "success": True,
            "portfolio": portfolio,
            "org": org,
            "ring": ring,
            "doc_id": doc_id,
            "searchable_fields": selected_fields,
            "indexed_rows": indexed_rows,
            "deleted_rows": deleted_rows,
            "token_count": len(rows),
        }

    def delete_document(self, portfolio: str, org: str, ring: str, doc_id: str) -> Dict[str, Any]:
        if not self.is_enabled():
            return {"success": False, "message": "Search is not configured"}
        if not portfolio or not org or not ring or not doc_id:
            raise SearchValidationError("portfolio, org, ring and doc_id are required")
        try:
            deleted_rows = self.model.delete_document_rows(portfolio, org, ring, doc_id)
        except Exception as exc:
            self.logger.error(f"search delete_document failed for {portfolio}/{org}/{ring}/{doc_id}: {exc}")
            raise SearchStorageError(str(exc)) from exc
        return {
            "success": True,
            "portfolio": portfolio,
            "org": org,
            "ring": ring,
            "doc_id": doc_id,
            "deleted_rows": deleted_rows,
        }

    def search(
        self,
        portfolio: str,
        org: str,
        query: str,
        datatypes: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 20,
        offset: int = 0,
        search_fields: Optional[List[str]] = None,
        boost_fields: Optional[Dict[str, float]] = None,
        rings: Optional[List[str]] = None,
        resolve_matches: bool = False,
    ) -> Dict[str, Any]:
        if not self.is_enabled():
            return {"success": False, "message": "Search is not configured", "items": [], "total": 0}
        if not portfolio or not org:
            return {"success": False, "message": "portfolio and org are required", "items": [], "total": 0}
        if not isinstance(query, str) or not query.strip():
            return {"success": True, "items": [], "total": 0, "query": query}

        query_tokens = self._tokenize_text(query)
        query_tokens = [t for t in query_tokens if t not in self.DEFAULT_STOPWORDS and len(t) >= 2]
        exact_query_token = self._normalize_exact_token(query)
        candidate_tokens = list(query_tokens)
        if exact_query_token and exact_query_token not in candidate_tokens:
            candidate_tokens.append(exact_query_token)

        if not candidate_tokens:
            return {"success": True, "items": [], "total": 0, "query": query}

        target_rings = self._resolve_target_rings(datatypes, rings, filters)
        if not target_rings:
            return {
                "success": False,
                "message": "No target rings provided. Use filters.rings or configure SEARCH_DEFAULT_RINGS.",
                "items": [],
                "total": 0,
            }

        requested_fields = [str(v).strip() for v in (search_fields or []) if str(v).strip()]
        per_token_limit = max(limit * 10, 100)
        hits: Dict[str, Dict[str, Any]] = {}

        try:
            for ring in target_rings:
                for token in candidate_tokens:
                    field_scopes = requested_fields or [None]
                    for field in field_scopes:
                        response = self.model.query_token(
                            portfolio,
                            org,
                            ring,
                            token,
                            field_prefix=field,
                            limit=per_token_limit,
                        )
                        for row in response.get("items", []):
                            hit_key = f"{row['ring']}#{row['doc_id']}"
                            hit = hits.setdefault(
                                hit_key,
                                {
                                    "portfolio": portfolio,
                                    "org": org,
                                    "ring": row["ring"],
                                    "doc_id": row["doc_id"],
                                    "score": 0.0,
                                    "matched_tokens": set(),
                                    "matched_fields": set(),
                                    "match_details": [],
                                },
                            )
                            hit["score"] += self._row_score(row, token, boost_fields)
                            hit["matched_tokens"].add(token)
                            hit["matched_fields"].add(row["field"])
                            hit["match_details"].append(
                                {
                                    "token": token,
                                    "field": row["field"],
                                    "token_count": row["token_count"],
                                    "positions": row.get("positions", []),
                                }
                            )
        except Exception as exc:
            self.logger.error(f"search query failed for {portfolio}/{org}: {exc}")
            raise SearchStorageError(str(exc)) from exc

        ranked: List[Dict[str, Any]] = []
        for hit in hits.values():
            token_bonus = max(0, len(hit["matched_tokens"]) - 1) * 2.0
            field_bonus = max(0, len(hit["matched_fields"]) - 1) * 1.5
            hit["score"] = round(hit["score"] + token_bonus + field_bonus, 5)
            hit["matched_tokens"] = sorted(hit["matched_tokens"])
            hit["matched_fields"] = sorted(hit["matched_fields"])
            ranked.append(hit)

        ranked.sort(key=lambda h: (-float(h["score"]), str(h["doc_id"])))
        total = len(ranked)
        paged = ranked[offset : offset + max(1, min(limit, 100))]
        if resolve_matches:
            paged = self._resolve_documents_for_hits(portfolio, org, paged)

        return {
            "success": True,
            "query": query,
            "tokens": query_tokens,
            "exact_token": exact_query_token,
            "rings": target_rings,
            "total": total,
            "offset": offset,
            "limit": limit,
            "resolved": resolve_matches,
            "items": paged,
        }
