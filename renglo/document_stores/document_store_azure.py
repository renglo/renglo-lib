"""Cosmos document store.

Azure submodels import this module. It is the only place that imports the
Azure database SDK.
"""

from __future__ import annotations

import hashlib
import os
import shutil
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

from renglo.config_names import setting
from renglo.document_stores.errors import Conflict, PreconditionFailed, StoreError


SYSTEM_FIELDS = ("id", "_rid", "_self", "_etag", "_attachments", "_ts")
_ILLEGAL_ID_CHARS = frozenset("/\\?#")
_clients: Dict[str, Any] = {}


def cosmos_id(sort_key: str) -> str:
    """Cosmos document ids cannot contain / \\ ? #. Legal sort keys are used as-is."""
    if not isinstance(sort_key, str):
        sort_key = str(sort_key)
    if 1 <= len(sort_key) <= 255 and not any(char in sort_key for char in _ILLEGAL_ID_CHARS):
        return sort_key
    return hashlib.sha256(sort_key.encode("utf-8")).hexdigest()


def resume_after(lastkey, *fields):
    """Read an exclusive start value from a previous page key."""
    if lastkey in (None, ""):
        return None
    if isinstance(lastkey, str):
        return lastkey
    if isinstance(lastkey, dict):
        for field in fields:
            value = lastkey.get(field)
            if value not in (None, ""):
                return value
    return None


def strip_system(document: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not isinstance(document, dict):
        return document
    return {key: value for key, value in document.items() if key not in SYSTEM_FIELDS}


def _safe_name(path: str) -> str:
    if not isinstance(path, str) or not path:
        raise ValueError(f"unsafe document path: {path!r}")
    first = path[0]
    if not (first.isalpha() or first == "_"):
        raise ValueError(f"unsafe document path: {path!r}")
    if any(not (char.isalnum() or char == "_") for char in path):
        raise ValueError(f"unsafe document path: {path!r}")
    return path


def build_query(
    path: str,
    *,
    prefix=None,
    equals=None,
    low=None,
    high=None,
    after=None,
    before=None,
    descending: bool = False,
    fields: Optional[Iterable[str]] = None,
    filter_field=None,
    filter_value=None,
) -> Tuple[str, Optional[List[Dict[str, Any]]]]:
    """Return a partition-scoped SQL query and its parameters."""
    quoted = _safe_name(path)
    clauses = []
    parameters: List[Dict[str, Any]] = []

    def add(name: str, value: Any, expression: str) -> None:
        parameters.append({"name": name, "value": value})
        clauses.append(expression)

    if prefix is not None:
        add("@prefix", prefix, f'STARTSWITH(c["{quoted}"], @prefix)')
    if equals is not None:
        add("@equals", equals, f'c["{quoted}"] = @equals')
    if low is not None:
        add("@low", low, f'c["{quoted}"] >= @low')
    if high is not None:
        add("@high", high, f'c["{quoted}"] <= @high')
    if after is not None:
        add("@after", after, f'c["{quoted}"] > @after')
    if before is not None:
        add("@before", before, f'c["{quoted}"] < @before')
    if filter_field is not None:
        add("@filter", filter_value, f'c["{_safe_name(filter_field)}"] = @filter')

    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    direction = "DESC" if descending else "ASC"
    if fields:
        selected = ", ".join(f"c.{_safe_name(field)}" for field in fields)
    else:
        selected = "*"
    sql = f'SELECT {selected} FROM c{where} ORDER BY c["{quoted}"] {direction}'
    return sql, parameters or None


def _status(exc: BaseException):
    return getattr(exc, "status_code", None)


def _missing(exc: BaseException) -> bool:
    return type(exc).__name__ == "CosmosResourceNotFoundError" or _status(exc) == 404


def _conflict(exc: BaseException) -> bool:
    return type(exc).__name__ == "CosmosResourceExistsError" or _status(exc) == 409


def _match(etag):
    if not etag:
        return {}
    kwargs = {"etag": etag}
    try:
        from azure.cosmos import MatchConditions
    except ModuleNotFoundError:
        kwargs["match_condition"] = "IfNotModified"
    else:
        kwargs["match_condition"] = MatchConditions.IfNotModified
    return kwargs


def _precondition(exc: BaseException) -> bool:
    return type(exc).__name__ == "CosmosAccessConditionFailedError" or _status(exc) == 412


def _connection_string(value: str) -> Tuple[str, str]:
    parts = {}
    for piece in str(value or "").split(";"):
        if "=" in piece:
            key, item = piece.split("=", 1)
            parts[key] = item
    endpoint = parts.get("AccountEndpoint")
    key = parts.get("AccountKey")
    if not endpoint or not key:
        raise RuntimeError(
            "AZURE_COSMOS_CONNECTION_STRING must include AccountEndpoint and AccountKey."
        )
    return endpoint, key


def _azure_cli_directories():
    """Directories that may hold a local `az` when it is not on PATH.

    The API virtualenv does not install the Azure CLI. This checkout keeps
    one in the infra virtualenv and one beside the Function App.
    """
    found = []
    configured = os.environ.get("AZURE_CLI_PATH", "").strip()
    if configured:
        found.append(Path(configured))
    for parent in Path(__file__).resolve().parents:
        found.append(parent / "azure_poc" / "infra" / ".venv" / "bin")
        found.append(parent / "azure_poc" / "app" / ".cli-venv" / "bin")
    return found


def _ensure_azure_cli():
    """Put a local Azure CLI on PATH so AzureCliCredential can run `az`."""
    if shutil.which("az"):
        return
    for candidate in _azure_cli_directories():
        directory = candidate.parent if candidate.name == "az" else candidate
        if (directory / "az").is_file():
            os.environ["PATH"] = str(directory) + os.pathsep + os.environ.get("PATH", "")
            return


def _credential(config):
    from azure.identity import AzureCliCredential, ManagedIdentityCredential

    client_id = str(setting(config, "AZURE_CLIENT_ID", "") or "").strip()
    if client_id and (os.environ.get("IDENTITY_ENDPOINT") or os.environ.get("WEBSITE_INSTANCE_ID")):
        return ManagedIdentityCredential(client_id=client_id)
    _ensure_azure_cli()
    return AzureCliCredential()


def cosmos_client(config):
    """Return a cached Cosmos client for this config."""
    from azure.cosmos import CosmosClient

    endpoint = str(setting(config, "AZURE_COSMOS_ACCOUNT_ENDPOINT", "") or "").strip()
    key = str(setting(config, "AZURE_COSMOS_KEY", "") or "").strip()
    connection = str(setting(config, "AZURE_COSMOS_CONNECTION_STRING", "") or "").strip()
    if not endpoint and connection:
        endpoint, key = _connection_string(connection)
    if not endpoint:
        raise RuntimeError(
            "Set AZURE_COSMOS_ACCOUNT_ENDPOINT or AZURE_COSMOS_CONNECTION_STRING."
        )
    cache_key = endpoint + "\n" + ("key" if key else "identity")
    cached = _clients.get(cache_key)
    if cached is not None:
        return cached
    if key:
        client = CosmosClient(endpoint, credential=key)
    else:
        client = CosmosClient(endpoint, credential=_credential(config))
    _clients[cache_key] = client
    return client


def open_container(config, table_name: str):
    database_name = str(setting(config, "AZURE_COSMOS_DATABASE_NAME", "") or "").strip()
    if not database_name:
        raise RuntimeError("AZURE_COSMOS_DATABASE_NAME configuration is required but not found")
    if not table_name:
        raise RuntimeError("A container name is required")
    database = cosmos_client(config).get_database_client(database_name)
    return database.get_container_client(table_name)


def _first_page(result, continuation, limit):
    if hasattr(result, "by_page"):
        pager = result.by_page(continuation)
        try:
            page = list(next(pager))
        except StopIteration:
            return [], None
        token = getattr(pager, "continuation_token", None)
        if limit is not None:
            page = page[:limit]
        return page, token
    page = list(result)
    if limit is not None:
        page = page[:limit]
    return page, None


class DocumentStore:
    def __init__(
        self,
        config=None,
        table_name=None,
        partition_key=None,
        sort_key=None,
        *,
        container=None,
    ):
        self.config = config or {}
        self.table_name = table_name
        self.partition_key = partition_key
        self.sort_key = sort_key
        self.container = container
        self.database_name = str(setting(self.config, "AZURE_COSMOS_DATABASE_NAME", "") or "").strip()

    def _container(self):
        if self.container is None:
            self.container = open_container(self.config, self.table_name)
        return self.container

    def prepare(self, document: Dict[str, Any]) -> Dict[str, Any]:
        body = {key: value for key, value in dict(document).items() if key not in SYSTEM_FIELDS}
        partition = body.get(self.partition_key)
        sort_value = body.get(self.sort_key)
        if not isinstance(partition, str) or partition == "":
            raise ValueError(f"{self.partition_key} must be a non-empty string")
        if not isinstance(sort_value, str) or sort_value == "":
            raise ValueError(f"{self.sort_key} must be a non-empty string")
        body["id"] = cosmos_id(sort_value)
        return body

    def get(self, partition, sort):
        document, _etag = self.read(partition, sort)
        return document

    def read(self, partition, sort):
        try:
            item = self._container().read_item(item=cosmos_id(sort), partition_key=partition)
        except Exception as exc:
            if _missing(exc):
                return None, None
            raise
        if not isinstance(item, dict):
            return None, None
        return strip_system(item), item.get("_etag")

    def _page(self, partition, sql, parameters, continuation, limit):
        result = self._container().query_items(
            query=sql,
            parameters=parameters,
            partition_key=partition,
            max_item_count=limit,
        )
        return _first_page(result, continuation, limit)

    def query(
        self,
        partition,
        *,
        path=None,
        prefix=None,
        equals=None,
        low=None,
        high=None,
        after=None,
        before=None,
        limit=None,
        continuation=None,
        descending=False,
        fields=None,
        index=None,
        filter_field=None,
        filter_value=None,
    ):
        """Return ``(documents, continuation)``.

        ``limit`` is the maximum number of documents in this result. The
        continuation is set when more documents match. A dict continuation
        is the last returned key and resumes strictly after that sort value.
        ``index`` is accepted so callers can name a DynamoDB index; Cosmos
        queries the attribute path directly.
        """
        sort_name = path or self.sort_key
        if isinstance(continuation, dict):
            marker = continuation.get(sort_name)
            if marker in (None, ""):
                marker = continuation.get(self.sort_key)
            if marker not in (None, ""):
                if descending and before is None:
                    before = marker
                elif after is None:
                    after = marker
            continuation = None
        sql, parameters = build_query(
            sort_name,
            prefix=prefix,
            equals=equals,
            low=low,
            high=high,
            after=after,
            before=before,
            descending=descending,
            fields=fields,
            filter_field=filter_field,
            filter_value=filter_value,
        )
        if limit is None:
            page, token = self._page(partition, sql, parameters, continuation, None)
            return [strip_system(item) for item in page], token

        collected: List[Dict[str, Any]] = []
        token = continuation
        remaining = limit
        while remaining > 0:
            page, token = self._page(partition, sql, parameters, token, remaining)
            if not page:
                return collected, None
            collected.extend(strip_system(item) for item in page)
            remaining -= len(page)
            if remaining <= 0:
                return collected[:limit], token
            if not token:
                return collected, None
        return collected[:limit], token

    def put(self, document, *, exclusive=False, absent_or_less=None):
        if exclusive:
            self.create(document)
            return 200
        if absent_or_less is not None:
            return self._put_absent_or_less(document, absent_or_less)
        self._container().upsert_item(body=self.prepare(document))
        return 200

    def _put_absent_or_less(self, document, absent_or_less):
        field, now = absent_or_less
        partition = document[self.partition_key]
        sort_value = document[self.sort_key]
        for _attempt in range(12):
            current, etag = self.read(partition, sort_value)
            if current is None:
                try:
                    self.create(document)
                    return 200
                except Conflict:
                    continue
            if current.get(field, 0) < now:
                current.update(document)
                try:
                    self.replace(current, etag=etag)
                    return 200
                except PreconditionFailed:
                    continue
            raise PreconditionFailed(
                "continuation lock is held",
                status=400,
                code="ConditionalCheckFailedException",
            )
        raise PreconditionFailed(
            "continuation lock could not be acquired",
            status=400,
            code="ConditionalCheckFailedException",
        )

    def create(self, document):
        try:
            self._container().create_item(body=self.prepare(document))
        except Exception as exc:
            if _conflict(exc):
                raise Conflict(str(exc), status=409, code="Conflict") from exc
            raise
        return 200

    def replace(self, document, etag=None):
        body = self.prepare(document)
        kwargs = _match(etag)
        try:
            self._container().replace_item(item=body["id"], body=body, **kwargs)
        except Exception as exc:
            if _precondition(exc) or _conflict(exc):
                raise PreconditionFailed(str(exc), status=412, code="PreconditionFailed") from exc
            raise
        return 200

    def delete(self, partition, sort, etag=None, *, key=None, equals=None, return_old=False):
        if isinstance(key, dict):
            if self.partition_key in key and self.sort_key in key:
                partition = key[self.partition_key]
                sort = key[self.sort_key]
            else:
                partition = key.get("irn", partition)
                sort = key.get("time", sort)
        if equals is not None:
            field, value = equals
            current, current_etag = self.read(partition, sort)
            if current is None or current.get(field) != value:
                raise PreconditionFailed(
                    "condition failed",
                    status=400,
                    code="ConditionalCheckFailedException",
                )
            etag = current_etag
        kwargs = _match(etag)
        try:
            self._container().delete_item(item=cosmos_id(sort), partition_key=partition, **kwargs)
        except Exception as exc:
            if _missing(exc):
                return False if return_old else 200
            if _precondition(exc):
                raise PreconditionFailed(str(exc), status=412, code="PreconditionFailed") from exc
            raise
        return True if return_old else 200

    def append(self, partition, sort, field, values):
        for _attempt in range(12):
            current, etag = self.read(partition, sort)
            if current is None:
                raise StoreError("Document not found", status=404, code="NotFound")
            items = list(current.get(field) or [])
            items.extend(list(values))
            current[field] = items
            try:
                self.replace(current, etag=etag)
                return
            except PreconditionFailed:
                continue
        raise PreconditionFailed("document changed while appending", status=409, code="PreconditionFailed")

    def upsert(self, document, *, preserve=()):
        partition = document[self.partition_key]
        sort_value = document[self.sort_key]
        current, etag = self.read(partition, sort_value)
        body = dict(document)
        if current is not None:
            for field in preserve:
                if field in current:
                    body[field] = current[field]
            self.replace(body, etag=etag)
            return
        self.create(body)

    def merge_map(self, partition, sort, map_name, changes, *, also=None):
        current, etag = self.read(partition, sort)
        if current is None:
            raise StoreError("Document not found", status=404, code="NotFound")
        mapped = dict(current.get(map_name) or {})
        mapped.update(changes or {})
        current[map_name] = mapped
        for key, value in (also or {}).items():
            current[key] = value
        self.replace(current, etag=etag)
        updated = {map_name: mapped}
        updated.update(also or {})
        return updated

    def append_checked(
        self,
        partition,
        sort,
        field,
        item,
        *,
        sets,
        expected_version,
        cap,
        version_field="version",
        idempotency_key=None,
        idempotency_keys=None,
        idempotency_field="idempotency_keys",
    ):
        current, etag = self.read(partition, sort)
        version = int((current or {}).get(version_field) or 0)
        if current is not None and version != int(expected_version):
            raise PreconditionFailed("version", status=412, code="PreconditionFailed")
        if current is None and int(expected_version) != 0:
            raise PreconditionFailed("version", status=412, code="PreconditionFailed")
        items = list((current or {}).get(field) or [])
        if len(items) >= int(cap):
            raise PreconditionFailed("full", status=412, code="PreconditionFailed")
        if idempotency_key and idempotency_key in set((current or {}).get(idempotency_field) or []):
            raise PreconditionFailed("idempotency", status=412, code="PreconditionFailed")
        items.append(item)
        body = dict(current or {})
        body[self.partition_key] = partition
        body[self.sort_key] = sort
        body[field] = items
        body[version_field] = int(expected_version) + 1
        body.update(sets or {})
        if idempotency_key:
            body[idempotency_field] = sorted(set(idempotency_keys or []))
        if current is None:
            self.create(body)
        else:
            self.replace(body, etag=etag)

    def batch_get(self, keys):
        found = []
        for key in keys:
            item = self.get(key[self.partition_key], key[self.sort_key])
            if item is not None:
                found.append(item)
        return found

    def batch_put(self, documents) -> int:
        count = 0
        for document in documents:
            self.put(document)
            count += 1
        return count

    def batch_delete(self, keys) -> int:
        count = 0
        for key in keys:
            self.delete(key.get(self.partition_key), key.get(self.sort_key), key=key)
            count += 1
        return count

    def ensure_resource(self):
        raise RuntimeError("This document store does not expose a DynamoDB resource")

    def scan(self, fields: Optional[Iterable[str]] = None) -> Iterator[Dict[str, Any]]:
        names = list(fields or [])
        if names:
            selected = ", ".join(f"c.{_safe_name(field)}" for field in names)
        else:
            selected = "*"
        result = self._container().query_items(
            query=f"SELECT {selected} FROM c",
            enable_cross_partition_query=True,
        )
        for item in result:
            if isinstance(item, dict):
                yield strip_system(item)

    def describe(self, logical_name: str = "Document", hint: str = "") -> None:
        try:
            self._container().read()
        except Exception as exc:
            if _missing(exc):
                raise RuntimeError(
                    f"{logical_name} container '{self.table_name}' was not found "
                    f"in database '{self.database_name}'."
                ) from exc
            raise RuntimeError(
                f"Failed to describe {logical_name} container '{self.table_name}': {exc}"
            ) from exc
