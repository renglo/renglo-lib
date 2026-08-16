"""Scheduler activity ledger.

Cloud: one Dynamo index doc per UTC day, details on S3.
Local (ebe): JSONL under dev/ebe/tmp — never Dynamo or S3.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from renglo.files.files_model import FilesModel
from renglo.logger import get_logger

RING = "schd_activity"
S3_RING_PREFIX = "schd_activity"
_META_KEYS = frozenset(
    {
        "portfolio_index",
        "doc_index",
        "added",
        "modified",
        "license",
        "public",
        "blueprint",
        "portfolio",
        "org",
        "ring",
        "blueprint_version",
        "path_index",
        "attributes",
    }
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _truncate(text: str, limit: int = 500) -> str:
    text = str(text or "").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def row_as_view(row: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(row, dict):
        return {}
    attrs = row.get("attributes")
    if isinstance(attrs, dict) and attrs:
        view = dict(attrs)
    else:
        view = {k: v for k, v in row.items() if k not in _META_KEYS}
    if row.get("_id"):
        view["_id"] = row["_id"]
    return view


def flatten_entries(raw: Any) -> list[dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    flat: list[dict[str, Any]] = []
    for item in raw:
        if isinstance(item, dict):
            flat.append(item)
        elif isinstance(item, list):
            for sub in item:
                if isinstance(sub, dict):
                    flat.append(sub)
    return flat


class SchdActivityLog:
    """Append-only scheduler activity ledger. Best-effort; never raises."""

    def __init__(
        self,
        data_controller: Any,
        portfolio: str,
        org: str,
        config: Optional[dict] = None,
        shm: Any = None,
    ):
        self.DAC = data_controller
        self.portfolio = portfolio
        self.org = org
        self.logger = get_logger()
        self._shm = shm
        self._files = FilesModel(config=config or getattr(data_controller, "config", None) or {})

    def _raw_day_row(self, journal_date: str) -> dict[str, Any] | None:
        try:
            row = self.DAC.DAM.get_a_b_c(self.portfolio, self.org, RING, journal_date)
            if not row or row.get("error"):
                return None
            return row
        except Exception as exc:
            self.logger.warning("schd activity raw day row failed: %s", exc)
            return None

    def _get_day_doc(self, journal_date: str) -> dict[str, Any] | None:
        row = self._raw_day_row(journal_date)
        if not row:
            return None
        view = row_as_view(row)
        if not view:
            return None
        view["_id"] = row.get("_id") or journal_date
        view["entries"] = flatten_entries(view.get("entries"))
        return view

    def _create_day_doc(self, journal_date: str, entries: list[dict[str, Any]]) -> bool:
        entries = flatten_entries(entries)
        payload = {"journal_date": journal_date, "entries": entries}
        try:
            item = self.DAC.construct_post_item(self.portfolio, self.org, RING, payload)
            if not isinstance(item, dict) or item.get("error"):
                return False
            item["_id"] = journal_date
            result = self.DAC.DAM.post_a_b(self.portfolio, self.org, RING, item)
            return "error" not in result
        except Exception as exc:
            self.logger.warning("schd activity create day doc failed: %s", exc)
            return False

    def _put_day_entries(self, journal_date: str, entries: list[dict[str, Any]]) -> bool:
        entries = flatten_entries(entries)
        payload = {"journal_date": journal_date, "entries": entries}
        try:
            raw = self._raw_day_row(journal_date)
            if raw:
                item = self.DAC.construct_put_item(
                    self.portfolio, self.org, RING, journal_date, payload
                )
                if not isinstance(item, dict) or item.get("error"):
                    return False
                result = self.DAC.DAM.put_a_b_c(
                    self.portfolio, self.org, RING, journal_date, item
                )
                return "error" not in result
            return self._create_day_doc(journal_date, entries)
        except Exception as exc:
            self.logger.warning("schd activity put day entries failed: %s", exc)
            return False

    def _store_detail_s3(self, journal_date: str, event_id: str, detail: dict[str, Any]) -> str:
        ring = f"{S3_RING_PREFIX}/{journal_date}"
        body = json.dumps(detail, default=str).encode("utf-8")
        try:
            resp = self._files.a_b_post(
                self.portfolio,
                self.org,
                ring,
                body,
                "application/json",
                event_id,
            )
            if resp.get("success") and resp.get("path"):
                return str(resp["path"])
        except Exception as exc:
            self.logger.warning("schd activity S3 detail write failed: %s", exc)
        return ""

    def append(
        self,
        *,
        event_type: str,
        summary: str,
        trigger: str = "",
        schd_jobs_id: str = "",
        heartbeat_id: str = "",
        refs: Optional[dict[str, Any]] = None,
        detail: Optional[dict[str, Any]] = None,
        event_id: str = "",
        schedule_origin: str = "",
        schd_machine_id: str = "",
    ) -> dict[str, Any]:
        journal_date = _today()
        event_id = str(event_id or uuid.uuid4())
        origin = str(schedule_origin or "cloud").strip().lower() or "cloud"
        entry: dict[str, Any] = {
            "event_id": event_id,
            "ts": _now_iso(),
            "event_type": event_type,
            "summary": _truncate(summary, 500),
            "trigger": trigger or "",
            "schd_jobs_id": schd_jobs_id or "",
            "heartbeat_id": heartbeat_id or "",
            "refs": refs or {},
            "schedule_origin": origin,
            "schd_machine_id": schd_machine_id or "",
            "portfolio": self.portfolio,
            "org": self.org,
        }
        if origin == "local":
            if detail is not None:
                entry["detail"] = detail
                entry["has_detail"] = True
            self._append_local(entry)
            return entry
        if detail:
            s3_path = self._store_detail_s3(journal_date, event_id, detail)
            if s3_path:
                entry["detail_s3_path"] = s3_path

        try:
            doc = self._get_day_doc(journal_date)
            entries = list((doc or {}).get("entries") or [])
            entries.append(entry)
            if not self._put_day_entries(journal_date, entries):
                self.logger.warning("schd activity index append failed for %s", event_type)
        except Exception as exc:
            self.logger.warning("schd activity append failed: %s", exc)

        return entry

    def _append_local(self, entry: dict[str, Any]) -> None:
        """Local origin never writes Dynamo or S3 — ebe tmp JSONL only."""
        writer = getattr(self._shm, "append_local_activity", None)
        if not callable(writer):
            self.logger.warning("schd local activity skipped: ebe is not available")
            return
        try:
            result = writer(entry)
            if isinstance(result, dict) and result.get("success") is False:
                self.logger.warning("schd local activity write failed: %s", result.get("output"))
        except Exception as exc:
            self.logger.warning("schd local activity write failed: %s", exc)

    def get_local(self, event_id: str) -> dict[str, Any] | None:
        getter = getattr(self._shm, "get_local_activity", None)
        if not callable(getter):
            return None
        try:
            result = getter(event_id)
        except Exception as exc:
            self.logger.warning("schd local activity read failed: %s", exc)
            return None
        entry = result.get("entry") if isinstance(result, dict) else None
        if not isinstance(entry, dict):
            return None
        if str(entry.get("portfolio") or "") not in {"", self.portfolio}:
            return None
        if str(entry.get("org") or "") not in {"", self.org}:
            return None
        return entry

    def load_detail(self, detail_s3_path: str) -> dict[str, Any] | None:
        path = (detail_s3_path or "").strip()
        if not path.startswith("_files/"):
            return None
        parts = path.split("/")
        if len(parts) < 6:
            return None
        portfolio, org = parts[1], parts[2]
        ring = "/".join(parts[3:-1])
        filename = parts[-1]
        try:
            resp = self._files.a_b_c_get(portfolio, org, ring, filename)
            if not resp.get("success"):
                return None
            raw = resp.get("content")
            if isinstance(raw, (bytes, bytearray)):
                return json.loads(raw.decode("utf-8"))
            if isinstance(raw, str):
                return json.loads(raw)
        except Exception as exc:
            self.logger.warning("schd activity detail load failed: %s", exc)
        return None

    def list_recent(
        self,
        *,
        days: int = 7,
        limit: int = 100,
        event_type: str = "",
        schd_jobs_id: str = "",
        schedule_origin: str = "",
        schd_machine_id: str = "",
    ) -> list[dict[str, Any]]:
        days = max(1, min(int(days or 7), 31))
        limit = max(1, min(int(limit or 100), 500))
        event_filter = (event_type or "").strip()
        job_filter = (schd_jobs_id or "").strip()
        origin_filter = str(schedule_origin or "cloud").strip().lower()
        machine_filter = str(schd_machine_id or "").strip()

        if origin_filter == "local":
            return self._list_local(
                days=days,
                limit=limit,
                event_type=event_filter,
                schd_jobs_id=job_filter,
                schd_machine_id=machine_filter,
            )

        listed = self.DAC.DAM.get_a_b(self.portfolio, self.org, RING, limit=days + 5)
        docs = list(listed.get("items") or [])

        def _doc_date(doc: dict[str, Any]) -> str:
            view = row_as_view(doc)
            return str(view.get("journal_date") or view.get("_id") or "")

        docs.sort(key=_doc_date, reverse=True)
        docs = docs[:days]

        flat: list[dict[str, Any]] = []
        for doc in docs:
            view = row_as_view(doc)
            for row in flatten_entries(view.get("entries")):
                if event_filter and str(row.get("event_type") or "") != event_filter:
                    continue
                if job_filter and str(row.get("schd_jobs_id") or "") != job_filter:
                    continue
                row_origin = str(row.get("schedule_origin") or "cloud").strip().lower()
                if row_origin == "local":
                    continue
                flat.append(row)
        flat.sort(key=lambda e: str(e.get("ts") or ""), reverse=True)
        return flat[:limit]

    def _list_local(
        self,
        *,
        days: int,
        limit: int,
        event_type: str,
        schd_jobs_id: str,
        schd_machine_id: str,
    ) -> list[dict[str, Any]]:
        lister = getattr(self._shm, "list_local_activity", None)
        if not callable(lister):
            return []
        try:
            result = lister(
                portfolio=self.portfolio,
                org=self.org,
                days=days,
                limit=limit,
                event_type=event_type,
                schd_jobs_id=schd_jobs_id,
                schd_machine_id=schd_machine_id,
            )
        except Exception as exc:
            self.logger.warning("schd local activity list failed: %s", exc)
            return []
        if not isinstance(result, dict) or result.get("success") is False:
            return []
        items = result.get("items")
        return list(items) if isinstance(items, list) else []
