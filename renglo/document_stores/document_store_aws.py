"""DynamoDB document store.

Method names match ``document_store_azure.DocumentStore``. This is the only
document module that imports boto3. One ``query`` call is one DynamoDB query
page; callers loop on the returned continuation, which is a ``LastEvaluatedKey``.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, Iterable, Iterator, Optional

from renglo.config_names import setting
from renglo.document_stores.errors import Conflict, PreconditionFailed, StoreError


def _numbers(value):
    """DynamoDB rejects bare floats. Leave every other value as the caller wrote it."""
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, dict):
        return {key: _numbers(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_numbers(item) for item in value]
    if isinstance(value, tuple):
        return [_numbers(item) for item in value]
    if isinstance(value, set):
        return {_numbers(item) for item in value}
    return value


def _error_response(exc):
    response = getattr(exc, "response", None)
    if isinstance(response, dict) and isinstance(response.get("Error"), dict):
        return response
    return None


def _raise_service(exc, response):
    error = response.get("Error") or {}
    code = error.get("Code") or ""
    message = error.get("Message") or str(exc)
    status = (response.get("ResponseMetadata") or {}).get("HTTPStatusCode", 500)
    if code == "ConditionalCheckFailedException":
        raise PreconditionFailed(message, status=status, code=code) from exc
    raise StoreError(message, status=status, code=code) from exc


def _status(response, default=200) -> int:
    if isinstance(response, dict):
        return int((response.get("ResponseMetadata") or {}).get("HTTPStatusCode", default))
    return default


def open_resource(config, region, *, use_profile=True):
    import boto3

    kwargs = {}
    if region:
        kwargs["region_name"] = region
    profile = str(setting(config, "PROFILE") or "").strip() if use_profile else ""
    if profile:
        return boto3.session.Session(profile_name=profile).resource("dynamodb", **kwargs)
    return boto3.resource("dynamodb", **kwargs)


class DocumentStore:
    def __init__(
        self,
        config=None,
        table_name=None,
        partition_key=None,
        sort_key=None,
        *,
        container=None,
        table=None,
        resource=None,
        region_name=None,
        use_profile=True,
    ):
        self.config = config or {}
        self.table_name = table_name
        self.partition_key = partition_key
        self.sort_key = sort_key
        self.container = container
        self.table = table if table is not None else container
        self.resource = resource
        self.region = region_name
        self.use_profile = use_profile

    def ensure_resource(self):
        self._table()
        return self.resource

    def _table(self):
        if self.table is None:
            self.resource = self.resource or open_resource(
                self.config, self.region, use_profile=self.use_profile
            )
            self.table = self.resource.Table(self.table_name)
        return self.table

    def _call(self, fn):
        try:
            return fn()
        except Exception as exc:
            response = _error_response(exc)
            if response is not None:
                _raise_service(exc, response)
            if type(exc).__module__.startswith("botocore"):
                raise StoreError(str(exc), status=400) from exc
            raise

    def get(self, partition, sort):
        document, _etag = self.read(partition, sort)
        return document

    def read(self, partition, sort):
        response = self._call(
            lambda: self._table().get_item(
                Key={self.partition_key: partition, self.sort_key: sort}
            )
        )
        item = response.get("Item") if isinstance(response, dict) else None
        return item, None

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
        """Return ``(documents, continuation)`` for one DynamoDB query page."""
        from boto3.dynamodb.conditions import Attr, Key

        sort_name = path or self.sort_key
        condition = Key(self.partition_key).eq(partition)
        if prefix is not None:
            condition = condition & Key(sort_name).begins_with(prefix)
        elif equals is not None:
            condition = condition & Key(sort_name).eq(equals)
        elif low is not None and high is not None:
            condition = condition & Key(sort_name).between(low, high)
        elif low is not None:
            condition = condition & Key(sort_name).gte(low)
        elif high is not None:
            condition = condition & Key(sort_name).lte(high)

        kwargs: Dict[str, Any] = {
            "KeyConditionExpression": condition,
            "ScanIndexForward": not descending,
        }
        if index:
            kwargs["IndexName"] = index
        if limit is not None:
            kwargs["Limit"] = limit
        if fields:
            names = {f"#p{i}": field for i, field in enumerate(fields)}
            kwargs["ProjectionExpression"] = ", ".join(names)
            kwargs["ExpressionAttributeNames"] = names
        if filter_field is not None:
            kwargs["FilterExpression"] = Attr(filter_field).eq(filter_value)
        if continuation:
            kwargs["ExclusiveStartKey"] = continuation
        elif after is not None or before is not None:
            marker = after if after is not None else before
            kwargs["ExclusiveStartKey"] = {self.partition_key: partition, sort_name: marker}

        response = self._call(lambda: self._table().query(**kwargs))
        items = response.get("Items", []) if isinstance(response, dict) else []
        token = response.get("LastEvaluatedKey") if isinstance(response, dict) else None
        return items, token

    def put(self, document, *, exclusive=False, absent_or_less=None):
        item = _numbers(dict(document))
        kwargs: Dict[str, Any] = {"Item": item}
        if exclusive:
            kwargs["ConditionExpression"] = "attribute_not_exists(#pk) AND attribute_not_exists(#sk)"
            kwargs["ExpressionAttributeNames"] = {
                "#pk": self.partition_key,
                "#sk": self.sort_key,
            }
        elif absent_or_less is not None:
            field, value = absent_or_less
            kwargs["ConditionExpression"] = "attribute_not_exists(#pk) OR #field < :now"
            kwargs["ExpressionAttributeNames"] = {"#pk": self.partition_key, "#field": field}
            kwargs["ExpressionAttributeValues"] = {":now": _numbers(value)}
        response = self._call(lambda: self._table().put_item(**kwargs))
        return _status(response)

    def create(self, document):
        try:
            return self.put(document, exclusive=True)
        except PreconditionFailed as exc:
            raise Conflict(exc.message, status=409, code=exc.code) from exc

    def replace(self, document, etag=None):
        return self.put(document)

    def delete(self, partition, sort, etag=None, *, key=None, equals=None, return_old=False):
        kwargs: Dict[str, Any] = {
            "Key": key or {self.partition_key: partition, self.sort_key: sort}
        }
        if equals is not None:
            field, value = equals
            kwargs["ConditionExpression"] = "#field = :value"
            kwargs["ExpressionAttributeNames"] = {"#field": field}
            kwargs["ExpressionAttributeValues"] = {":value": _numbers(value)}
        if return_old:
            kwargs["ReturnValues"] = "ALL_OLD"
        response = self._call(lambda: self._table().delete_item(**kwargs))
        if return_old:
            return isinstance(response, dict) and "Attributes" in response
        return _status(response)

    def append(self, partition, sort, field, values):
        self._call(
            lambda: self._table().update_item(
                Key={self.partition_key: partition, self.sort_key: sort},
                UpdateExpression="SET #field = list_append(if_not_exists(#field, :empty), :new)",
                ExpressionAttributeNames={"#field": field},
                ExpressionAttributeValues={
                    ":empty": [],
                    ":new": _numbers(list(values)),
                },
            )
        )

    def upsert(self, document, *, preserve=()):
        """Set every non-key attribute. ``preserve`` keeps an existing value."""
        body = _numbers(dict(document))
        kept = set(preserve)
        names: Dict[str, str] = {}
        values: Dict[str, Any] = {}
        assignments = []
        for index, (key, value) in enumerate(body.items()):
            if key in {self.partition_key, self.sort_key}:
                continue
            name = f"#n{index}"
            token = f":v{index}"
            names[name] = key
            values[token] = value
            if key in kept:
                assignments.append(f"{name} = if_not_exists({name}, {token})")
            else:
                assignments.append(f"{name} = {token}")
        if not assignments:
            return self.put(body)
        self._call(
            lambda: self._table().update_item(
                Key={
                    self.partition_key: body[self.partition_key],
                    self.sort_key: body[self.sort_key],
                },
                UpdateExpression="SET " + ", ".join(assignments),
                ExpressionAttributeNames=names,
                ExpressionAttributeValues=values,
            )
        )

    def merge_map(self, partition, sort, map_name, changes, *, also=None):
        """Set entries inside a map attribute, plus any top-level fields in ``also``."""
        names = {"#map": map_name}
        values: Dict[str, Any] = {}
        assignments = []
        for index, (key, value) in enumerate((changes or {}).items()):
            names[f"#k{index}"] = str(key)
            values[f":v{index}"] = _numbers(value)
            assignments.append(f"#map.#k{index} = :v{index}")
        for index, (key, value) in enumerate((also or {}).items()):
            names[f"#a{index}"] = str(key)
            values[f":a{index}"] = _numbers(value)
            assignments.append(f"#a{index} = :a{index}")
        if not assignments:
            return {}
        response = self._call(
            lambda: self._table().update_item(
                Key={self.partition_key: partition, self.sort_key: sort},
                UpdateExpression="SET " + ", ".join(assignments),
                ExpressionAttributeNames=names,
                ExpressionAttributeValues=values,
                ReturnValues="UPDATED_NEW",
            )
        )
        return response.get("Attributes") if isinstance(response, dict) else {}

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
        """Append one list element when the version matches and the list is under ``cap``."""
        names = {"#list": field, "#version": version_field}
        values: Dict[str, Any] = {
            ":empty": [],
            ":new": [_numbers(item)],
            ":expected_version": int(expected_version),
            ":new_version": int(expected_version) + 1,
            ":cap": int(cap),
        }
        assignments = [
            "#list = list_append(if_not_exists(#list, :empty), :new)",
            "#version = :new_version",
        ]
        for index, (key, value) in enumerate((sets or {}).items()):
            name = f"#s{index}"
            token = f":s{index}"
            names[name] = key
            values[token] = _numbers(value)
            assignments.append(f"{name} = {token}")
        condition = (
            "(attribute_not_exists(#version) OR #version = :expected_version) "
            "AND (attribute_not_exists(#list) OR size(#list) < :cap)"
        )
        if idempotency_key:
            names["#idem"] = idempotency_field
            values[":idem"] = idempotency_key
            values[":idems"] = _numbers(set(idempotency_keys or []))
            assignments.append("#idem = :idems")
            condition += " AND (attribute_not_exists(#idem) OR NOT contains(#idem, :idem))"
        self._call(
            lambda: self._table().update_item(
                Key={self.partition_key: partition, self.sort_key: sort},
                UpdateExpression="SET " + ", ".join(assignments),
                ConditionExpression=condition,
                ExpressionAttributeNames=names,
                ExpressionAttributeValues=values,
            )
        )

    def batch_get(self, keys):
        found = []
        table = self._table()
        client = table.meta.client
        name = table.name

        def run(pending):
            return client.batch_get_item(RequestItems={name: {"Keys": pending}})

        for start in range(0, len(keys), 100):
            pending = keys[start : start + 100]
            while pending:
                response = self._call(lambda pending=pending: run(pending))
                found.extend((response.get("Responses") or {}).get(name, []))
                pending = ((response.get("UnprocessedKeys") or {}).get(name) or {}).get("Keys") or []
        return found

    def batch_put(self, documents) -> int:
        docs = [_numbers(dict(document)) for document in documents]
        def run():
            with self._table().batch_writer() as batch:
                for document in docs:
                    batch.put_item(Item=document)
        self._call(run)
        return len(docs)

    def batch_delete(self, keys) -> int:
        key_list = list(keys)
        def run():
            with self._table().batch_writer() as batch:
                for key in key_list:
                    batch.delete_item(Key=key)
        self._call(run)
        return len(key_list)

    def scan(self, fields: Optional[Iterable[str]] = None) -> Iterator[Dict[str, Any]]:
        names = list(fields or [])
        token = None
        while True:
            kwargs: Dict[str, Any] = {}
            if names:
                aliases = {f"#f{i}": field for i, field in enumerate(names)}
                kwargs["ProjectionExpression"] = ", ".join(aliases)
                kwargs["ExpressionAttributeNames"] = aliases
            if token:
                kwargs["ExclusiveStartKey"] = token
            response = self._call(lambda kwargs=kwargs: self._table().scan(**kwargs))
            for item in response.get("Items", []) if isinstance(response, dict) else []:
                yield item
            token = response.get("LastEvaluatedKey") if isinstance(response, dict) else None
            if not token:
                break

    def describe(self, logical_name: str = "Document", hint: str = "") -> None:
        table = self._table()
        name = getattr(table, "name", None) or self.table_name
        try:
            self._call(lambda: table.meta.client.describe_table(TableName=name))
        except StoreError as exc:
            region = f" in region '{self.region}'" if self.region else ""
            extra = f" {hint}" if hint else ""
            if exc.code == "ResourceNotFoundException":
                raise RuntimeError(
                    f"{logical_name} table '{name}' was not found{region}.{extra}"
                ) from exc
            raise RuntimeError(
                f"Failed to describe {logical_name} table '{name}'{region}: {exc.code} {exc.message}"
            ) from exc


def document_id(sort_key: Optional[str] = None):
    return sort_key
