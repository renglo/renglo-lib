"""Core scheduler utility methods mixed into SchdController."""

from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from renglo.auth.authorize import authorize
from renglo.schd.schd_activity_log import SchdActivityLog, row_as_view
from renglo.schd.schd_machine_id import schd_machine_id

HEARTBEAT_CATALOG = (
    {
        "handle": "every_1_minute",
        "name": "Every 1 minute",
        "interval_seconds": "60",
        "schedule_expression": "rate(1 minute)",
        "always_enabled": True,
    },
    {
        "handle": "every_5_minutes",
        "name": "Every 5 minutes",
        "interval_seconds": "300",
        "schedule_expression": "rate(5 minutes)",
        "always_enabled": False,
    },
    {
        "handle": "every_15_minutes",
        "name": "Every 15 minutes",
        "interval_seconds": "900",
        "schedule_expression": "rate(15 minutes)",
        "always_enabled": False,
    },
    {
        "handle": "every_1_hour",
        "name": "Every 1 hour",
        "interval_seconds": "3600",
        "schedule_expression": "rate(1 hour)",
        "always_enabled": False,
    },
    {
        "handle": "every_1_day",
        "name": "Every 1 day",
        "interval_seconds": "86400",
        "schedule_expression": "cron(0 0 * * ? *)",
        "always_enabled": False,
    },
)

DUE_CHECK_HANDLE = "every_1_minute"
FANOUT_SOURCE = "custom.renglo.schd"
VALID_TRIGGERS = ("heartbeat", "cron", "once", "manual", "call")
LEASE_SECONDS = 120
ORIGIN_CLOUD = "cloud"
ORIGIN_LOCAL = "local"
_SCHEDULE_STAMP_KEYS = ("schedule_origin", "schd_machine_id")


def _now_ts() -> int:
    return int(time.time())


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "enabled"}


def _is_local_origin(row: Any) -> bool:
    return str((row or {}).get("schedule_origin") or "").strip().lower() == ORIGIN_LOCAL


def _requested_origin(payload: Any) -> str:
    raw = str((payload or {}).get("schedule_origin") or (payload or {}).get("origin") or "").strip().lower()
    return ORIGIN_LOCAL if raw == ORIGIN_LOCAL else ORIGIN_CLOUD


def _parse_payload(raw: Any) -> dict:
    if isinstance(raw, dict):
        return raw
    text = str(raw or "").strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else {}
    except (TypeError, ValueError):
        return {}


class SchdScheduleMixin:
    def _activity(self, portfolio, org) -> SchdActivityLog:
        return SchdActivityLog(self.DAC, portfolio, org, config=self.config, shm=self.SHM)

    def _flatten_doc(self, row: dict) -> dict:
        if not isinstance(row, dict) or row.get("error"):
            return {}
        view = row_as_view(row)
        if row.get("_id") and not view.get("_id"):
            view["_id"] = row["_id"]
        return view

    def _get_doc(self, portfolio, org, ring, idx) -> dict:
        row = self.DAC.DAM.get_a_b_c(portfolio, org, ring, idx)
        return self._flatten_doc(row)

    def _list_docs(self, portfolio, org, ring) -> list[dict]:
        listed = self.DAC.DAM.get_a_b(portfolio, org, ring, limit=10000)
        items = []
        for row in listed.get("items") or []:
            view = self._flatten_doc(row)
            if view:
                items.append(view)
        return items

    def _stamp_schedule_fields(self, item: dict, payload: dict) -> dict:
        """Keep origin/machine id even when the live blueprint omits those fields."""
        if not isinstance(item, dict) or item.get("error"):
            return item
        attrs = item.get("attributes")
        if not isinstance(attrs, dict):
            attrs = {}
            item["attributes"] = attrs
        for key in _SCHEDULE_STAMP_KEYS:
            if key in (payload or {}):
                attrs[key] = payload.get(key) or ""
        return item

    def _create_doc(self, portfolio, org, ring, payload, doc_id=None) -> dict:
        item = self.DAC.construct_post_item(portfolio, org, ring, payload)
        self._stamp_schedule_fields(item, payload)
        if doc_id:
            item["_id"] = doc_id
        result = self.DAC.DAM.post_a_b(portfolio, org, ring, item)
        if "error" in result:
            return {"success": False, "output": result, "item": item}
        return {
            "success": True,
            "path": f"{portfolio}/{org}/{ring}/{item['_id']}",
            "_id": item["_id"],
            "item": item,
        }

    def _patch_doc(self, portfolio, org, ring, idx, changes: dict) -> dict:
        item = self.DAC.construct_put_item(portfolio, org, ring, idx, changes)
        if isinstance(item, dict) and item.get("error"):
            return {"success": False, "output": item}
        self._stamp_schedule_fields(item, changes)
        result = self.DAC.DAM.put_a_b_c(portfolio, org, ring, idx, item)
        if "error" in result:
            return {"success": False, "output": result}
        return {"success": True, "output": result}

    def _delete_doc(self, portfolio, org, ring, idx) -> dict:
        result = self.DAC.DAM.delete_a_b_c(portfolio, org, ring, idx)
        if isinstance(result, dict) and result.get("error"):
            return {"success": False, "output": result}
        return {"success": True, "output": result}

    def _rule_name(self, portfolio, org, suffix: str) -> str:
        return f"cron_{portfolio}_{org}_{suffix}"

    def _local_rule_name(self, portfolio, org, suffix: str) -> str:
        mid = "".join(ch for ch in self._machine_id() if ch.isalnum())[:12] or "local"
        return f"ebe_{mid}_{portfolio}_{org}_{suffix}"

    def _fanout_rule_name(self, portfolio, org) -> str:
        return f"schd_fanout_{portfolio}_{org}"

    def _machine_id(self) -> str:
        return schd_machine_id()

    def _eventbridge_ready(self) -> bool:
        if not self.config.get("ROLE_ARN"):
            return False
        return bool(self.config.get("RENGLO_INGRESS_DESTINATION") or self.config.get("WL_NAME"))

    def _scheduler_ready(self) -> bool:
        return self._eventbridge_ready()

    def _inline_execute(self) -> bool:
        if self.config.get("SCHD_INLINE_EXECUTE"):
            return True
        return not self._eventbridge_ready()

    def _count_subscribers(self, portfolio, org, heartbeat_id: str, *, origin: str = ORIGIN_CLOUD) -> int:
        count = 0
        machine_id = self._machine_id() if origin == ORIGIN_LOCAL else ""
        for job in self._list_docs(portfolio, org, "schd_jobs"):
            if str(job.get("schedule_kind") or "") != "heartbeat":
                continue
            if str(job.get("heartbeat_id") or "") != heartbeat_id:
                continue
            if not _truthy(job.get("enabled", "true")):
                continue
            if origin == ORIGIN_LOCAL:
                if _is_local_origin(job) and str(job.get("schd_machine_id") or "") == machine_id:
                    count += 1
            elif not _is_local_origin(job):
                count += 1
        return count

    def _count_local_once(self, portfolio, org) -> int:
        machine_id = self._machine_id()
        count = 0
        for job in self._list_docs(portfolio, org, "schd_jobs"):
            if str(job.get("schedule_kind") or "") != "once":
                continue
            if not _truthy(job.get("enabled", "true")):
                continue
            if _is_local_origin(job) and str(job.get("schd_machine_id") or "") == machine_id:
                count += 1
        return count

    def _upsert_rule_doc(self, portfolio, org, *, kind, rule_name, **fields) -> None:
        docs = self._list_docs(portfolio, org, "schd_rules")
        existing = next((d for d in docs if d.get("eventbridge_rule_name") == rule_name), None)
        payload = {
            "kind": kind,
            "eventbridge_rule_name": rule_name,
            "aws_state": fields.get("aws_state") or "ENABLED",
            "last_sync_at": str(_now_ts()),
            "last_sync_error": fields.get("last_sync_error") or "",
            "schedule_expression": fields.get("schedule_expression") or "",
            "heartbeat_id": fields.get("heartbeat_id") or "",
            "schd_jobs_id": fields.get("schd_jobs_id") or "",
            "schedule_origin": fields.get("schedule_origin") or ORIGIN_CLOUD,
            "schd_machine_id": fields.get("schd_machine_id") or "",
        }
        if existing:
            self._patch_doc(portfolio, org, "schd_rules", existing["_id"], payload)
            return
        self._create_doc(portfolio, org, "schd_rules", payload)

    def _sync_heartbeat_rule(self, portfolio, org, heartbeat: dict, enable: bool, *, origin: str = ORIGIN_CLOUD) -> dict:
        handle = heartbeat.get("handle") or heartbeat.get("_id")
        local = origin == ORIGIN_LOCAL
        machine_id = self._machine_id() if local else ""
        rule_name = self._local_rule_name(portfolio, org, handle) if local else self._rule_name(portfolio, org, handle)
        expression = heartbeat.get("schedule_expression") or "rate(1 minute)"
        payload = {
            "type": "heartbeat",
            "portfolio": portfolio,
            "org": org,
            "heartbeat_id": handle,
            "schedule_origin": ORIGIN_LOCAL if local else ORIGIN_CLOUD,
            "schd_machine_id": machine_id,
        }
        backend = "local" if local else "cloud"
        ready = True if local else self._eventbridge_ready()
        if not ready:
            self._upsert_rule_doc(
                portfolio,
                org,
                kind="heartbeat",
                rule_name=rule_name,
                schedule_expression=expression,
                heartbeat_id=handle,
                aws_state="ENABLED" if enable else "DISABLED",
                last_sync_error="" if not enable else "EventBridge not configured",
                schedule_origin=payload["schedule_origin"],
                schd_machine_id=machine_id,
            )
            return {"success": True, "skipped": True, "rule_name": rule_name}

        if enable:
            result = self.SHM.create_https_target_event(rule_name, expression, payload, backend=backend)
            state = "ENABLED" if result.get("success") else "missing"
            self._upsert_rule_doc(
                portfolio,
                org,
                kind="heartbeat",
                rule_name=rule_name,
                schedule_expression=expression,
                heartbeat_id=handle,
                aws_state=state,
                last_sync_error="" if result.get("success") else str(result.get("output") or ""),
                schedule_origin=payload["schedule_origin"],
                schd_machine_id=machine_id,
            )
            return result

        result = self.SHM.delete_https_target_event(rule_name, backend=backend)
        self._upsert_rule_doc(
            portfolio,
            org,
            kind="heartbeat",
            rule_name=rule_name,
            schedule_expression=expression,
            heartbeat_id=handle,
            aws_state="missing",
            schedule_origin=payload["schedule_origin"],
            schd_machine_id=machine_id,
        )
        return result

    def _sync_custom_rule(self, portfolio, org, job: dict, enable: bool) -> dict:
        job_id = job.get("_id")
        local = _is_local_origin(job)
        machine_id = str(job.get("schd_machine_id") or "") if local else ""
        if local and machine_id != self._machine_id():
            return {"success": True, "skipped": True, "rule_name": "", "message": "other machine"}
        rule_name = self._local_rule_name(portfolio, org, job_id) if local else self._rule_name(portfolio, org, job_id)
        expression = job.get("schedule_expression") or ""
        payload = {
            "type": "schd_job",
            "portfolio": portfolio,
            "org": org,
            "schd_jobs_id": job_id,
            "trigger": "cron",
            "author": "schd",
            "schedule_origin": ORIGIN_LOCAL if local else ORIGIN_CLOUD,
            "schd_machine_id": machine_id,
        }
        backend = "local" if local else "cloud"
        ready = True if local else self._eventbridge_ready()
        if not ready:
            self._upsert_rule_doc(
                portfolio,
                org,
                kind="custom",
                rule_name=rule_name,
                schedule_expression=expression,
                schd_jobs_id=job_id,
                aws_state="ENABLED" if enable else "DISABLED",
                schedule_origin=payload["schedule_origin"],
                schd_machine_id=machine_id,
            )
            return {"success": True, "skipped": True, "rule_name": rule_name}
        if enable:
            result = self.SHM.create_https_target_event(rule_name, expression, payload, backend=backend)
            self._upsert_rule_doc(
                portfolio,
                org,
                kind="custom",
                rule_name=rule_name,
                schedule_expression=expression,
                schd_jobs_id=job_id,
                aws_state="ENABLED" if result.get("success") else "missing",
                last_sync_error="" if result.get("success") else str(result.get("output") or ""),
                schedule_origin=payload["schedule_origin"],
                schd_machine_id=machine_id,
            )
            return result
        result = self.SHM.delete_https_target_event(rule_name, backend=backend)
        self._upsert_rule_doc(
            portfolio,
            org,
            kind="custom",
            rule_name=rule_name,
            schedule_expression=expression,
            schd_jobs_id=job_id,
            aws_state="missing",
            schedule_origin=payload["schedule_origin"],
            schd_machine_id=machine_id,
        )
        return result

    def _ensure_fanout_rule(self, portfolio, org) -> dict:
        rule_name = self._fanout_rule_name(portfolio, org)
        if not self._eventbridge_ready():
            return {"success": True, "skipped": True}
        result = self.SHM.create_event_pattern_target(rule_name, FANOUT_SOURCE, backend="cloud")
        self._upsert_rule_doc(
            portfolio,
            org,
            kind="fanout",
            rule_name=rule_name,
            aws_state="ENABLED" if result.get("success") else "missing",
            last_sync_error="" if result.get("success") else str(result.get("output") or ""),
            schedule_origin=ORIGIN_CLOUD,
        )
        return result

    def _refresh_heartbeat_subscribers(self, portfolio, org, heartbeat_id: str) -> dict:
        hb = self._get_doc(portfolio, org, "schd_heartbeats", heartbeat_id)
        if not hb:
            return {"success": False}
        cloud_count = self._count_subscribers(portfolio, org, heartbeat_id, origin=ORIGIN_CLOUD)
        always = heartbeat_id == DUE_CHECK_HANDLE
        cloud_enable = always or cloud_count > 0
        self._patch_doc(
            portfolio,
            org,
            "schd_heartbeats",
            heartbeat_id,
            {
                "subscriber_count": str(cloud_count),
                "status": "enabled" if cloud_enable else "disabled",
                "eventbridge_rule_name": self._rule_name(portfolio, org, heartbeat_id),
            },
        )
        cloud_sync = self._sync_heartbeat_rule(
            portfolio, org, {**hb, "handle": heartbeat_id}, cloud_enable, origin=ORIGIN_CLOUD
        )
        local_count = self._count_subscribers(portfolio, org, heartbeat_id, origin=ORIGIN_LOCAL)
        local_enable = local_count > 0
        if heartbeat_id == DUE_CHECK_HANDLE:
            local_enable = local_enable or self._count_local_once(portfolio, org) > 0
        local_sync = self._sync_heartbeat_rule(
            portfolio, org, {**hb, "handle": heartbeat_id}, local_enable, origin=ORIGIN_LOCAL
        )
        return {"success": True, "cloud": cloud_sync, "local": local_sync}

    def ensure_heartbeats(self, portfolio, org) -> dict:
        action = "ensure_heartbeats"
        created = []
        existing = {d.get("handle") or d.get("_id"): d for d in self._list_docs(portfolio, org, "schd_heartbeats")}
        for spec in HEARTBEAT_CATALOG:
            handle = spec["handle"]
            if handle in existing:
                continue
            payload = {
                "name": spec["name"],
                "handle": handle,
                "interval_seconds": spec["interval_seconds"],
                "schedule_expression": spec["schedule_expression"],
                "status": "enabled" if spec["always_enabled"] else "disabled",
                "eventbridge_rule_name": self._rule_name(portfolio, org, handle),
                "subscriber_count": "0",
                "last_fired_at": "",
            }
            result = self._create_doc(portfolio, org, "schd_heartbeats", payload, doc_id=handle)
            created.append(result)
        self._resync_schedule_rules(portfolio, org)
        items = self._list_docs(portfolio, org, "schd_heartbeats")
        items.sort(key=lambda h: int(h.get("interval_seconds") or 0))
        return {"success": True, "action": action, "created": created, "heartbeats": items}

    def _resync_schedule_rules(self, portfolio, org) -> None:
        """Re-attach cloud EventBridge and this machine's ebe rules."""
        self._ensure_fanout_rule(portfolio, org)
        for hb in self._list_docs(portfolio, org, "schd_heartbeats"):
            handle = str(hb.get("handle") or hb.get("_id") or "")
            if handle:
                self._refresh_heartbeat_subscribers(portfolio, org, handle)
        for job in self._list_docs(portfolio, org, "schd_jobs"):
            if str(job.get("schedule_kind") or "") != "custom":
                continue
            if not _truthy(job.get("enabled", "true")):
                continue
            self._sync_custom_rule(portfolio, org, job, True)

    @authorize()
    def list_heartbeats(self, portfolio, org) -> dict:
        items = self._list_docs(portfolio, org, "schd_heartbeats")
        items.sort(key=lambda h: int(h.get("interval_seconds") or 0))
        return {"success": True, "action": "list_heartbeats", "items": items}

    @authorize()
    def set_heartbeat_status(self, portfolio, org, heartbeat_id, status: str) -> dict:
        hb = self._get_doc(portfolio, org, "schd_heartbeats", heartbeat_id)
        if not hb:
            return {"success": False, "message": "Heartbeat not found"}
        enable = str(status or "").strip().lower() == "enabled"
        if heartbeat_id == DUE_CHECK_HANDLE:
            enable = True
        self._patch_doc(
            portfolio,
            org,
            "schd_heartbeats",
            heartbeat_id,
            {"status": "enabled" if enable else "disabled"},
        )
        sync = self._refresh_heartbeat_subscribers(portfolio, org, heartbeat_id)
        return {"success": True, "action": "set_heartbeat_status", "heartbeat_id": heartbeat_id, "enabled": enable, "sync": sync}

    def _job_payload(self, extra: dict) -> dict:
        return {
            "name": extra.get("name") or extra.get("handler") or "job",
            "status": extra.get("status") or "available",
            "version": extra.get("version") or "1.0.0",
            "handler": extra.get("handler") or "",
            "description": extra.get("description") or "",
            "type": extra.get("type") or "control",
            "schedule_kind": extra.get("schedule_kind") or "none",
            "heartbeat_id": extra.get("heartbeat_id") or "",
            "schedule_expression": extra.get("schedule_expression") or "",
            "run_at": str(extra.get("run_at") or ""),
            "handler_payload": extra.get("handler_payload")
            if isinstance(extra.get("handler_payload"), str)
            else json.dumps(extra.get("handler_payload") or {}),
            "enabled": "true" if _truthy(extra.get("enabled", "true")) else "false",
            "last_run_at": extra.get("last_run_at") or "",
            "last_run_status": extra.get("last_run_status") or "",
            "last_run_error": extra.get("last_run_error") or "",
            "run_lease_until": extra.get("run_lease_until") or "",
            "schedule_origin": extra.get("schedule_origin") or ORIGIN_CLOUD,
            "schd_machine_id": extra.get("schd_machine_id") or "",
        }

    @authorize()
    def subscribe(self, portfolio, org, payload) -> dict:
        payload = dict(payload or {})
        handler = str(payload.get("handler") or "").strip()
        heartbeat_id = str(payload.get("heartbeat_id") or "").strip()
        if not handler or not heartbeat_id:
            return {"success": False, "message": "handler and heartbeat_id are required"}
        hb = self._get_doc(portfolio, org, "schd_heartbeats", heartbeat_id)
        if not hb:
            seeded = self.ensure_heartbeats(portfolio, org)
            hb = self._get_doc(portfolio, org, "schd_heartbeats", heartbeat_id)
            if not hb:
                return {"success": False, "message": f"Unknown heartbeat: {heartbeat_id}", "output": seeded}
        job_id = str(payload.get("schd_jobs_id") or payload.get("_id") or "").strip()
        origin = _requested_origin(payload)
        machine_id = self._machine_id() if origin == ORIGIN_LOCAL else ""
        fields = self._job_payload(
            {
                **payload,
                "schedule_kind": "heartbeat",
                "heartbeat_id": heartbeat_id,
                "handler": handler,
                "schedule_origin": origin,
                "schd_machine_id": machine_id,
            }
        )
        if job_id and self._get_doc(portfolio, org, "schd_jobs", job_id):
            self._patch_doc(portfolio, org, "schd_jobs", job_id, fields)
        else:
            created = self._create_doc(portfolio, org, "schd_jobs", fields)
            if not created.get("success"):
                return {"success": False, "action": "subscribe", "output": created}
            job_id = created["_id"]
        refresh = self._refresh_heartbeat_subscribers(portfolio, org, heartbeat_id)
        if origin == ORIGIN_LOCAL:
            local_sync = (refresh or {}).get("local") or {}
            if not local_sync.get("success") and not local_sync.get("skipped"):
                return {
                    "success": False,
                    "action": "subscribe",
                    "message": "Local scheduler (ebe) is not running on this machine",
                    "output": local_sync,
                    "schd_jobs_id": job_id,
                }
        return {"success": True, "action": "subscribe", "schd_jobs_id": job_id, "job": self._get_doc(portfolio, org, "schd_jobs", job_id)}

    @authorize()
    def unsubscribe(self, portfolio, org, job_id) -> dict:
        job = self._get_doc(portfolio, org, "schd_jobs", job_id)
        if not job:
            return {"success": False, "message": "Job not found"}
        heartbeat_id = str(job.get("heartbeat_id") or "")
        if str(job.get("schedule_kind") or "") == "custom":
            self._sync_custom_rule(portfolio, org, job, False)
        deleted = self._delete_doc(portfolio, org, "schd_jobs", job_id)
        if heartbeat_id:
            self._refresh_heartbeat_subscribers(portfolio, org, heartbeat_id)
        return {"success": True, "action": "unsubscribe", "schd_jobs_id": job_id, "output": deleted}

    @authorize()
    def schedule_once(self, portfolio, org, payload) -> dict:
        payload = dict(payload or {})
        handler = str(payload.get("handler") or "").strip()
        run_at = str(payload.get("run_at") or "").strip()
        if not handler or not run_at:
            return {"success": False, "message": "handler and run_at are required"}
        self.ensure_heartbeats(portfolio, org)
        origin = _requested_origin(payload)
        machine_id = self._machine_id() if origin == ORIGIN_LOCAL else ""
        fields = self._job_payload(
            {
                **payload,
                "schedule_kind": "once",
                "handler": handler,
                "run_at": run_at,
                "schedule_origin": origin,
                "schd_machine_id": machine_id,
            }
        )
        created = self._create_doc(portfolio, org, "schd_jobs", fields)
        if not created.get("success"):
            return {"success": False, "action": "schedule_once", "output": created}
        if origin == ORIGIN_LOCAL:
            refresh = self._refresh_heartbeat_subscribers(portfolio, org, DUE_CHECK_HANDLE)
            local_sync = (refresh or {}).get("local") or {}
            if not local_sync.get("success") and not local_sync.get("skipped"):
                return {
                    "success": False,
                    "action": "schedule_once",
                    "message": "Local scheduler (ebe) is not running on this machine",
                    "output": local_sync,
                    "schd_jobs_id": created["_id"],
                }
        return {"success": True, "action": "schedule_once", "schd_jobs_id": created["_id"], "job": self._get_doc(portfolio, org, "schd_jobs", created["_id"])}

    @authorize()
    def schedule_custom(self, portfolio, org, payload) -> dict:
        payload = dict(payload or {})
        handler = str(payload.get("handler") or "").strip()
        expression = str(payload.get("schedule_expression") or "").strip()
        if not handler or not expression:
            return {"success": False, "message": "handler and schedule_expression are required"}
        origin = _requested_origin(payload)
        machine_id = self._machine_id() if origin == ORIGIN_LOCAL else ""
        fields = self._job_payload(
            {
                **payload,
                "schedule_kind": "custom",
                "handler": handler,
                "schedule_expression": expression,
                "schedule_origin": origin,
                "schd_machine_id": machine_id,
            }
        )
        created = self._create_doc(portfolio, org, "schd_jobs", fields)
        if not created.get("success"):
            return {"success": False, "action": "schedule_custom", "output": created}
        job = self._get_doc(portfolio, org, "schd_jobs", created["_id"])
        sync = self._sync_custom_rule(portfolio, org, job, True)
        if origin == ORIGIN_LOCAL and not sync.get("success") and not sync.get("skipped"):
            return {
                "success": False,
                "action": "schedule_custom",
                "message": "Local scheduler (ebe) is not running on this machine",
                "output": sync,
                "schd_jobs_id": created["_id"],
            }
        return {"success": True, "action": "schedule_custom", "schd_jobs_id": created["_id"], "job": job, "sync": sync}

    @authorize()
    def pause_job(self, portfolio, org, job_id) -> dict:
        return self._set_job_enabled(portfolio, org, job_id, False)

    @authorize()
    def resume_job(self, portfolio, org, job_id) -> dict:
        return self._set_job_enabled(portfolio, org, job_id, True)

    def _set_job_enabled(self, portfolio, org, job_id, enabled: bool) -> dict:
        job = self._get_doc(portfolio, org, "schd_jobs", job_id)
        if not job:
            return {"success": False, "message": "Job not found"}
        self._patch_doc(portfolio, org, "schd_jobs", job_id, {"enabled": "true" if enabled else "false"})
        kind = str(job.get("schedule_kind") or "")
        if kind == "heartbeat" and job.get("heartbeat_id"):
            self._refresh_heartbeat_subscribers(portfolio, org, job["heartbeat_id"])
        elif kind == "custom":
            self._sync_custom_rule(portfolio, org, {**job, "_id": job_id}, enabled)
        return {"success": True, "action": "resume_job" if enabled else "pause_job", "schd_jobs_id": job_id, "enabled": enabled}

    @authorize()
    def run_now(self, portfolio, org, job_id, payload=None) -> dict:
        extra = dict(payload or {})
        extra["schd_jobs_id"] = job_id
        extra["trigger"] = extra.get("trigger") or "manual"
        extra["author"] = extra.get("author") or "schd"
        result, status = self.execute_job(portfolio, org, extra)
        return result if isinstance(result, dict) else {"success": status == 200, "output": result}

    @authorize()
    def list_jobs(self, portfolio, org, origin: str = ORIGIN_CLOUD) -> dict:
        origin = ORIGIN_LOCAL if str(origin or "").strip().lower() == ORIGIN_LOCAL else ORIGIN_CLOUD
        items = self._list_docs(portfolio, org, "schd_jobs")
        machine_id = self._machine_id()
        if origin == ORIGIN_LOCAL:
            items = [
                job
                for job in items
                if _is_local_origin(job) and str(job.get("schd_machine_id") or "") == machine_id
            ]
        else:
            items = [job for job in items if not _is_local_origin(job)]
        return {
            "success": True,
            "action": "list_jobs",
            "items": items,
            "schedule_origin": origin,
            "schd_machine_id": machine_id if origin == ORIGIN_LOCAL else "",
        }

    @authorize()
    def get_job(self, portfolio, org, job_id) -> dict:
        job = self._get_doc(portfolio, org, "schd_jobs", job_id)
        if not job:
            return {"success": False, "message": "Job not found"}
        return {"success": True, "action": "get_job", "job": job}

    @authorize()
    def list_activity(self, portfolio, org, days=7, limit=100, event_type="", schd_jobs_id="", origin: str = ORIGIN_CLOUD) -> dict:
        origin = ORIGIN_LOCAL if str(origin or "").strip().lower() == ORIGIN_LOCAL else ORIGIN_CLOUD
        items = self._activity(portfolio, org).list_recent(
            days=days,
            limit=limit,
            event_type=event_type,
            schd_jobs_id=schd_jobs_id,
            schedule_origin=origin,
            schd_machine_id=self._machine_id() if origin == ORIGIN_LOCAL else "",
        )
        return {
            "success": True,
            "action": "list_activity",
            "items": items,
            "count": len(items),
            "schedule_origin": origin,
            "schd_machine_id": self._machine_id() if origin == ORIGIN_LOCAL else "",
        }

    @authorize()
    def get_activity(self, portfolio, org, event_id, days=14, origin: str = ORIGIN_CLOUD) -> dict:
        origin = ORIGIN_LOCAL if str(origin or "").strip().lower() == ORIGIN_LOCAL else ORIGIN_CLOUD
        log = self._activity(portfolio, org)
        if origin == ORIGIN_LOCAL:
            match = log.get_local(event_id)
            if not match:
                return {"success": False, "message": "event not found"}
            return {
                "success": True,
                "action": "get_activity",
                "entry": match,
                "detail": match.get("detail"),
            }
        items = log.list_recent(days=days, limit=500)
        match = next((i for i in items if str(i.get("event_id")) == str(event_id)), None)
        if not match:
            return {"success": False, "message": "event not found"}
        detail = None
        if match.get("detail_s3_path"):
            detail = log.load_detail(match["detail_s3_path"])
        return {"success": True, "action": "get_activity", "entry": match, "detail": detail}

    def dispatch_heartbeat(self, portfolio, org, heartbeat_id, detail=None) -> tuple[dict, int]:
        action = "dispatch_heartbeat"
        detail = dict(detail or {})
        origin = _requested_origin(detail)
        machine_id = str(detail.get("schd_machine_id") or "")
        if origin == ORIGIN_LOCAL and not machine_id:
            machine_id = self._machine_id()
        hb = self._get_doc(portfolio, org, "schd_heartbeats", heartbeat_id)
        if not hb:
            return {"success": False, "action": action, "message": "Heartbeat not found"}, 404
        if str(hb.get("status") or "") == "disabled" and heartbeat_id != DUE_CHECK_HANDLE and origin != ORIGIN_LOCAL:
            return {"success": True, "action": action, "dispatched": 0, "skipped": 0, "message": "disabled"}, 200

        due = []
        for job in self._list_docs(portfolio, org, "schd_jobs"):
            if not _truthy(job.get("enabled", "true")):
                continue
            if origin == ORIGIN_LOCAL:
                if not _is_local_origin(job) or str(job.get("schd_machine_id") or "") != machine_id:
                    continue
            elif _is_local_origin(job):
                continue
            kind = str(job.get("schedule_kind") or "")
            if kind == "heartbeat" and str(job.get("heartbeat_id") or "") == heartbeat_id:
                due.append(job)
            elif heartbeat_id == DUE_CHECK_HANDLE and kind == "once":
                try:
                    run_at = int(float(job.get("run_at") or 0))
                except (TypeError, ValueError):
                    run_at = 0
                if run_at and run_at <= _now_ts():
                    due.append(job)

        dispatched = 0
        skipped = 0
        now = _now_ts()
        to_run = []
        for job in due:
            lease = str(job.get("run_lease_until") or "").strip()
            try:
                lease_until = int(float(lease)) if lease else 0
            except (TypeError, ValueError):
                lease_until = 0
            if lease_until > now:
                skipped += 1
                self._activity(portfolio, org).append(
                    event_type="skipped",
                    summary=f"Skipped {job.get('name') or job.get('_id')} (lease)",
                    trigger="heartbeat",
                    schd_jobs_id=job.get("_id"),
                    heartbeat_id=heartbeat_id,
                    refs={"reason": "lease"},
                    schedule_origin=origin,
                    schd_machine_id=machine_id if origin == ORIGIN_LOCAL else "",
                )
                continue
            event_id = str(uuid.uuid4())
            self._patch_doc(
                portfolio,
                org,
                "schd_jobs",
                job["_id"],
                {"run_lease_until": str(now + LEASE_SECONDS)},
            )
            to_run.append(
                {
                    "type": "schd_job",
                    "portfolio": portfolio,
                    "org": org,
                    "schd_jobs_id": job["_id"],
                    "trigger": "once" if job.get("schedule_kind") == "once" else "heartbeat",
                    "heartbeat_id": heartbeat_id,
                    "event_id": event_id,
                    "author": "schd",
                    "schedule_origin": origin,
                    "schd_machine_id": machine_id if origin == ORIGIN_LOCAL else "",
                }
            )
            dispatched += 1
            if job.get("schedule_kind") == "once":
                self._patch_doc(
                    portfolio,
                    org,
                    "schd_jobs",
                    job["_id"],
                    {"enabled": "false", "schedule_kind": "none"},
                )
        self._enqueue_execute(portfolio, org, to_run)

        self._patch_doc(
            portfolio,
            org,
            "schd_heartbeats",
            heartbeat_id,
            {"last_fired_at": str(now)},
        )
        return {
            "success": True,
            "action": action,
            "heartbeat_id": heartbeat_id,
            "dispatched": dispatched,
            "skipped": skipped,
        }, 200

    def _enqueue_execute(self, portfolio, org, details) -> None:
        if isinstance(details, dict):
            details = [details]
        details = [item for item in (details or []) if item]
        if not details:
            return
        if self.config.get("SCHD_INLINE_EXECUTE"):
            for detail in details:
                self.execute_job(portfolio, org, detail)
            return
        origin = str(details[0].get("schedule_origin") or ORIGIN_CLOUD)
        backend = "local" if origin == ORIGIN_LOCAL else "cloud"
        if backend == "cloud" and not self._eventbridge_ready():
            for detail in details:
                self.execute_job(portfolio, org, detail)
            return
        put = self.SHM.put_schd_events(details, backend=backend)
        if not put.get("success"):
            self.logger.warning("schd fanout PutEvents failed, executing inline: %s", put)
            for detail in details:
                self.execute_job(portfolio, org, detail)

    def execute_job(self, portfolio, org, payload) -> tuple[dict, int]:
        action = "execute_job"
        payload = dict(payload or {})
        job_id = str(payload.get("schd_jobs_id") or "").strip()
        if not job_id:
            return {"success": False, "action": action, "message": "No Job Id", "input": payload}, 400
        job = self._get_doc(portfolio, org, "schd_jobs", job_id)
        if not job:
            return {"success": False, "action": action, "message": "Error getting job", "input": payload}, 400

        trigger = str(payload.get("trigger") or "manual")
        if trigger not in VALID_TRIGGERS:
            return {"success": False, "action": action, "message": "Invalid trigger value"}, 400

        if trigger in ("heartbeat", "cron", "once"):
            payload_local = _is_local_origin(payload)
            job_local = _is_local_origin(job)
            if payload_local != job_local:
                return {"success": True, "action": action, "skipped": True, "message": "origin mismatch"}, 200
            if job_local:
                expected = str(payload.get("schd_machine_id") or self._machine_id())
                if str(job.get("schd_machine_id") or "") != expected:
                    return {"success": True, "action": action, "skipped": True, "message": "machine mismatch"}, 200

        handler_name = str(job.get("handler") or "").strip()
        if not handler_name:
            return {"success": False, "action": action, "message": "No handler in the job document"}, 400

        event_id = str(payload.get("event_id") or uuid.uuid4())
        heartbeat_id = str(payload.get("heartbeat_id") or job.get("heartbeat_id") or "")
        extra = _parse_payload(job.get("handler_payload"))
        handler_input = {
            "portfolio": portfolio,
            "org": org,
            "handler": handler_name,
            "schd_jobs_id": job_id,
            "event_id": event_id,
            "trigger": trigger,
            "heartbeat_id": heartbeat_id,
            "handler_payload": extra,
            **extra,
        }

        try:
            response = self.SHL.load_and_run(handler_name, payload=handler_input)
        except Exception as exc:
            response = {"success": False, "output": str(exc), "error": str(exc)}

        ok = bool(response.get("success"))
        event_type = "executed" if ok else "failed"
        summary = f"{handler_name} {event_type}"
        job_detail = {
            "schd_jobs_id": job_id,
            "event_id": event_id,
            "trigger": trigger,
            "heartbeat_id": heartbeat_id,
            "name": job.get("name") or "",
            "handler": handler_name,
            "schedule_kind": str(job.get("schedule_kind") or ""),
            "schedule_expression": str(job.get("schedule_expression") or ""),
            "run_at": str(job.get("run_at") or ""),
            "handler_payload": extra,
            "enabled": str(job.get("enabled") or ""),
            "schedule_origin": str(job.get("schedule_origin") or ORIGIN_CLOUD),
            "schd_machine_id": str(job.get("schd_machine_id") or ""),
        }
        activity_detail = {
            "success": ok,
            "action": action,
            "input": handler_input,
            "output": response,
            "job": job_detail,
        }
        self._activity(portfolio, org).append(
            event_type=event_type,
            summary=summary,
            trigger=trigger,
            schd_jobs_id=job_id,
            heartbeat_id=heartbeat_id,
            refs={"handler": handler_name},
            detail=activity_detail,
            event_id=event_id,
            schedule_origin=str(job.get("schedule_origin") or ORIGIN_CLOUD),
            schd_machine_id=str(job.get("schd_machine_id") or ""),
        )
        self._patch_doc(
            portfolio,
            org,
            "schd_jobs",
            job_id,
            {
                "last_run_at": str(_now_ts()),
                "last_run_status": event_type,
                "last_run_error": "" if ok else str(response.get("error") or response.get("output") or "failed"),
                "run_lease_until": "",
            },
        )
        status = 200 if ok else 400
        return {
            "success": ok,
            "action": action,
            "handler": handler_name,
            "schd_jobs_id": job_id,
            "event_id": event_id,
            "input": handler_input,
            "output": response,
        }, status
