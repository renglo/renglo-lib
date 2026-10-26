"""TimeSeries appends, segments, rollups, and the authenticated route."""

import copy
import threading
from datetime import datetime, timezone

import pytest

from renglo.llm import LlmController
from renglo.llm.providers.bedrock_converse import BedrockConverseAdapter
from renglo.llm.registry import INPUT_PER_MILLION, OUTPUT_PER_MILLION
from renglo.timeseries.keys import READING_LIMIT, make_index
from renglo.timeseries.timeseries_controller import TimeSeriesController
from renglo.timeseries.timeseries_model import TimeSeriesModel


class ConditionalCheckFailed(Exception):
    def __init__(self):
        super().__init__("conditional check failed")
        self.response = {"Error": {"Code": "ConditionalCheckFailedException"}}


class MemoryTable:
    def __init__(self):
        self.items = {}
        self.lock = threading.Lock()

    def get_item(self, Key):
        with self.lock:
            item = self.items.get((Key["index"], Key["metric_index"]))
            if item is None:
                return {}
            copied = dict(item)
            if "statistics" in item:
                copied["statistics"] = dict(item["statistics"])
            if "dimensions" in item:
                copied["dimensions"] = list(item["dimensions"])
            if "readings" in item:
                copied["readings"] = list(item["readings"])
            if "idempotency_keys" in item:
                copied["idempotency_keys"] = set(item["idempotency_keys"])
            return {"Item": copied}

    def put_item(self, Item):
        with self.lock:
            self.items[(Item["index"], Item["metric_index"])] = copy.deepcopy(Item)

    def update_item(
        self,
        Key,
        UpdateExpression,
        ConditionExpression,
        ExpressionAttributeNames,
        ExpressionAttributeValues,
    ):
        del UpdateExpression, ConditionExpression, ExpressionAttributeNames
        with self.lock:
            key = (Key["index"], Key["metric_index"])
            current = self.items.get(key)
            values = ExpressionAttributeValues
            expected = int(values[":expected_version"])
            version = 0 if current is None else int(current.get("version") or 0)
            if version != expected:
                raise ConditionalCheckFailed()
            readings = [] if current is None else list(current.get("readings") or [])
            if len(readings) >= int(values[":cap"]):
                raise ConditionalCheckFailed()
            idempotency_key = values.get(":idempotency_key")
            if idempotency_key and idempotency_key in set((current or {}).get("idempotency_keys") or []):
                raise ConditionalCheckFailed()
            stored = {
                "index": Key["index"],
                "metric_index": Key["metric_index"],
                "resolution": values[":resolution"],
                "period": values[":period"],
                "dimensions": list(values[":dimensions"]),
                "segment": int(values[":segment"]),
                "readings": readings + [list(values[":reading"][0])],
                "statistics": copy.deepcopy(values[":statistics"]),
                "reading_count": int(values[":reading_count"]),
                "payload_bytes": int(values[":payload_bytes"]),
                "version": int(values[":new_version"]),
            }
            if idempotency_key:
                stored["idempotency_keys"] = set(values[":idempotency_keys"])
            elif current and current.get("idempotency_keys"):
                stored["idempotency_keys"] = set(current["idempotency_keys"])
            self.items[key] = stored

    def query(
        self,
        KeyConditionExpression,
        ExclusiveStartKey=None,
        ProjectionExpression=None,
        ExpressionAttributeNames=None,
    ):
        del ExclusiveStartKey
        expression = KeyConditionExpression.get_expression()
        equals, bound = expression["values"]
        partition = equals.get_expression()["values"][1]
        operator = bound.get_expression()["operator"]
        values = bound.get_expression()["values"]
        with self.lock:
            matched = []
            for (index, sort_key), item in self.items.items():
                if index != partition:
                    continue
                if operator == "begins_with" and str(sort_key).startswith(values[1]):
                    matched.append(_project(item, ProjectionExpression, ExpressionAttributeNames))
                elif operator == "BETWEEN" and values[1] <= sort_key <= values[2]:
                    matched.append(_project(item, ProjectionExpression, ExpressionAttributeNames))
            matched.sort(key=lambda item: item["metric_index"])
            return {"Items": matched}


def _project(item, expression, names=None):
    if not expression:
        return copy.deepcopy(item)
    fields = [part.strip() for part in expression.split(",")]
    aliases = names or {}
    projected = {
        "index": item.get("index"),
        "metric_index": item.get("metric_index"),
    }
    for field in fields:
        field = aliases.get(field, field)
        if field in item:
            value = item[field]
            projected[field] = list(value) if isinstance(value, list) else value
    return projected


class AllowAuth:
    def authorize(self, portfolio, org, **_kwargs):
        if not str(portfolio or "").strip() or not str(org or "").strip():
            return {
                "success": False,
                "status": 400,
                "message": "portfolio and org are required",
            }
        return {"success": True, "roles": ["owner"]}


def clock(year, month, day, hour=12):
    return int(datetime(year, month, day, hour, tzinfo=timezone.utc).timestamp())


def store():
    table = MemoryTable()
    model = TimeSeriesModel(table=table)
    controller = TimeSeriesController(model=model, auth=AllowAuth())
    return table, model, controller


def test_concurrent_puts_keep_both_readings_and_the_sum():
    table, _model, controller = store()

    class RacingTable:
        def __init__(self):
            self.inner = table
            self.barrier = threading.Barrier(2)
            self.reads = 0
            self.gate = threading.Lock()

        def get_item(self, **kwargs):
            item = self.inner.get_item(**kwargs)
            with self.gate:
                self.reads += 1
                count = self.reads
            if count <= 2:
                self.barrier.wait(timeout=3)
            return item

        def update_item(self, **kwargs):
            return self.inner.update_item(**kwargs)

        def query(self, **kwargs):
            return self.inner.query(**kwargs)

        def put_item(self, **kwargs):
            return self.inner.put_item(**kwargs)

    controller.TSM.table = RacingTable()
    moment = clock(2026, 10, 20)
    errors = []

    def write(value):
        try:
            result = controller.put(
                "portfolio_y",
                "org_x",
                "room_temperature",
                value,
                dimensions=["zone37", "sensor34"],
                timestamp=moment + value,
                resolution="day",
            )
            if not result.get("success"):
                errors.append(result)
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=write, args=(value,)) for value in (1, 2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert errors == []
    loaded = controller.get(
        "portfolio_y",
        "org_x",
        "room_temperature",
        "2026-10-20",
        dimensions=["zone37", "sensor34"],
    )
    readings = loaded["output"]["readings"]
    assert sorted(pair[1] for pair in readings) == [1, 2]
    assert loaded["output"]["statistics"]["count"] == 2
    assert loaded["output"]["statistics"]["sum"] == 3


def test_put_keeps_millisecond_timestamp_from_the_caller():
    _table, _model, controller = store()
    moment = clock(2026, 10, 20) + 0.123
    stored = controller.put(
        "portfolio_y",
        "org_x",
        "room_temperature",
        22.3,
        dimensions=["zone37"],
        timestamp=moment,
    )
    reading_time = stored["output"]["readings"][-1][0]
    assert reading_time != int(reading_time)
    assert abs(reading_time - moment) < 0.0005
    assert stored["output"]["statistics"]["last_timestamp"] == reading_time


def test_min_and_max_move_only_for_a_new_extreme_and_avg_is_computed():
    _table, model, controller = store()
    moment = clock(2026, 10, 20)
    for offset, value in ((0, 10), (1, 3), (2, 7)):
        controller.put(
            "portfolio_y",
            "org_x",
            "room_temperature",
            value,
            dimensions=["zone37", "sensor34"],
            timestamp=moment + offset,
        )
    loaded = controller.get(
        "portfolio_y",
        "org_x",
        "room_temperature",
        "2026-10-20",
        dimensions=["zone37", "sensor34"],
    )
    stats = loaded["output"]["statistics"]
    assert stats["min"] == 3
    assert stats["max"] == 10
    assert stats["count"] == 3
    assert stats["sum"] == 20
    assert stats["avg"] == stats["sum"] / stats["count"]
    assert stats["first"] == 10
    assert stats["last"] == 7
    stored = next(iter(model.table.items.values()))
    assert "avg" not in stored["statistics"]


def test_idempotency_key_appends_once():
    _table, _model, controller = store()
    kwargs = dict(
        portfolio="portfolio_y",
        org="org_x",
        key="room_temperature",
        value=22.3,
        dimensions=["zone37", "sensor34"],
        timestamp=clock(2026, 10, 20),
        idempotency_key="obs-1",
    )
    assert controller.put(**kwargs)["success"]
    assert controller.put(**kwargs)["success"]
    loaded = controller.get(
        "portfolio_y", "org_x", "room_temperature", "2026-10-20", ["zone37", "sensor34"]
    )
    assert loaded["output"]["statistics"]["count"] == 1
    assert len(loaded["output"]["readings"]) == 1


def test_dimensions_keep_caller_order():
    table, _model, controller = store()
    controller.put(
        "portfolio_y",
        "org_x",
        "room_temperature",
        1,
        dimensions=["b", "a#b"],
        timestamp=clock(2026, 10, 20),
    )
    item = next(iter(table.items.values()))
    assert item["dimensions"] == ["b", "a#b"]
    assert item["metric_index"] == "2026-10-20#b#a%23b"


def test_prefix_query_returns_one_branch_and_segments():
    table, model, controller = store()
    model.reading_limit = 2
    moment = clock(2026, 10, 20)
    for value in (1, 2, 3):
        controller.put(
            "portfolio_y",
            "org_x",
            "room_temperature",
            value,
            dimensions=["zone37", "sensor34"],
            timestamp=moment + value,
        )
    controller.put(
        "portfolio_y",
        "org_x",
        "room_temperature",
        9,
        dimensions=["zone99", "sensor34"],
        timestamp=moment,
    )
    keys = [
        sort_key
        for (_index, sort_key) in table.items
        if str(sort_key).startswith("2026-10-20#zone37#")
    ]
    assert "2026-10-20#zone37#sensor34" in keys
    assert "2026-10-20#zone37#sensor34#1" in keys
    matched = model.query_documents(
        "portfolio_y", "org_x", "room_temperature", "2026-10-20#zone37#"
    )
    assert {item["metric_index"] for item in matched} == set(keys)
    assert all(not item["metric_index"].startswith("2026-10-20#zone99") for item in matched)
    queried = controller.query(
        "portfolio_y",
        "org_x",
        "room_temperature",
        period="2026-10-20",
        dimensions_prefix=["zone37"],
    )
    series = queried["output"]["series"]
    assert len(series) == 1
    assert series[0]["metric_index"] == "2026-10-20#zone37#sensor34"
    assert series[0]["statistics"]["count"] == 3
    assert [pair[1] for pair in series[0]["readings"]] == [1, 2, 3]


def test_segment_limit_and_idempotent_weighted_rollups():
    _table, _model, controller = store()
    day = clock(2026, 10, 20)
    for index in range(READING_LIMIT):
        value = 1 if index == 0 else 5
        controller.put(
            "portfolio_y",
            "org_x",
            "room_temperature",
            value,
            dimensions=["zone37", "sensor34"],
            timestamp=day + index,
        )
    controller.put(
        "portfolio_y",
        "org_x",
        "room_temperature",
        9,
        dimensions=["zone37", "sensor34"],
        timestamp=day + READING_LIMIT,
    )
    stored_keys = [
        item["metric_index"]
        for item in controller.TSM.query_documents(
            "portfolio_y", "org_x", "room_temperature", "2026-10-20#zone37#sensor34"
        )
    ]
    assert "2026-10-20#zone37#sensor34#1" in stored_keys
    loaded = controller.get(
        "portfolio_y",
        "org_x",
        "room_temperature",
        "2026-10-20",
        dimensions=["zone37", "sensor34"],
    )
    assert loaded["output"]["statistics"]["count"] == READING_LIMIT + 1
    assert len(loaded["output"]["readings"]) == READING_LIMIT + 1
    assert loaded["output"]["metric_index"] == "2026-10-20#zone37#sensor34"

    other = clock(2026, 10, 1)
    controller.put(
        "portfolio_y",
        "org_x",
        "room_temperature",
        10,
        dimensions=["zone37", "sensor34"],
        timestamp=other,
    )
    controller.put(
        "portfolio_y",
        "org_x",
        "room_temperature",
        30,
        dimensions=["zone37", "sensor34"],
        timestamp=other + 10,
    )
    controller.put(
        "portfolio_y",
        "org_x",
        "room_temperature",
        0,
        dimensions=["zone37", "sensor34"],
        timestamp=clock(2026, 10, 2),
    )
    first = controller.reconcile(
        "portfolio_y",
        "org_x",
        "room_temperature",
        dimensions=["zone37", "sensor34"],
        period="2026-10-20",
    )
    month = first["output"]["month"]
    assert month["metric_index"] == "2026-10#zone37#sensor34"
    assert month["statistics"]["min"] == 0
    assert month["statistics"]["max"] == 30
    assert month["statistics"]["count"] == READING_LIMIT + 1 + 3
    day_points = {point[0]: point[1] for point in month["readings"]}
    assert day_points["2026-10-01"] == 20
    assert day_points["2026-10-02"] == 0
    october_20_sum = 1 + (5 * (READING_LIMIT - 1)) + 9
    assert month["statistics"]["sum"] == october_20_sum + 40
    weighted = month["statistics"]["sum"] / month["statistics"]["count"]
    assert month["statistics"]["avg"] == weighted
    unweighted = sum(day_points.values()) / len(day_points)
    assert month["statistics"]["avg"] != unweighted

    second = controller.reconcile(
        "portfolio_y",
        "org_x",
        "room_temperature",
        dimensions=["zone37", "sensor34"],
        period="2026-10-20",
    )
    assert second["output"]["month"] == month
    assert second["output"]["year"] == first["output"]["year"]
    assert first["output"]["year"]["metric_index"] == "2026#zone37#sensor34"
    assert first["output"]["year"]["statistics"]["count"] == month["statistics"]["count"]

    controller.put(
        "portfolio_y",
        "org_x",
        "room_temperature",
        100,
        dimensions=["zone37", "sensor34"],
        timestamp=day + READING_LIMIT + 5,
    )
    third = controller.reconcile(
        "portfolio_y",
        "org_x",
        "room_temperature",
        dimensions=["zone37", "sensor34"],
        period="2026-10-20",
    )
    assert third["output"]["month"]["statistics"]["max"] == 100
    assert third["output"]["month"]["statistics"]["count"] == month["statistics"]["count"] + 1


def test_month_range_query_reads_rollup_documents():
    _table, _model, controller = store()
    controller.put(
        "portfolio_y",
        "org_x",
        "room_temperature",
        4,
        dimensions=["zone37", "sensor34"],
        timestamp=clock(2026, 10, 3),
    )
    controller.reconcile(
        "portfolio_y",
        "org_x",
        "room_temperature",
        dimensions=["zone37", "sensor34"],
        period="2026-10-03",
    )
    result = controller.query(
        "portfolio_y",
        "org_x",
        "room_temperature",
        start="2026-01",
        end="2026-12",
        resolution="month",
        dimensions=["zone37", "sensor34"],
    )
    series = result["output"]["series"]
    assert [item["period"] for item in series] == ["2026-10"]
    assert series[0]["resolution"] == "month"


def test_llm_usage_is_recorded_per_measure_and_cannot_fail_generate():
    class Recorder:
        def __init__(self):
            self.calls = []

        def put(self, portfolio, org, key, value, dimensions=None, resolution="day", idempotency_key=None):
            self.calls.append(
                {
                    "portfolio": portfolio,
                    "org": org,
                    "key": key,
                    "value": value,
                    "dimensions": dimensions,
                    "resolution": resolution,
                    "idempotency_key": idempotency_key,
                }
            )
            return {"success": True}

    class Broken:
        def put(self, **_kwargs):
            raise RuntimeError("timeseries down")

    class Converse:
        def __init__(self, payload):
            self.payload = payload
            self.calls = []

        def converse(self, **kwargs):
            self.calls.append(kwargs)
            if isinstance(self.payload, Exception):
                raise self.payload
            return self.payload

    class BadRequest(Exception):
        def __init__(self):
            super().__init__("bad request")
            self.status_code = 400

    payload = {
        "output": {"message": {"role": "assistant", "content": [{"text": "ok"}]}},
        "usage": {
            "inputTokens": 11,
            "outputTokens": 6,
            "reasoningTokens": 2,
            "cached_tokens": None,
            "cacheReadInputTokens": 4,
        },
    }
    config = {"BEDROCK_MODEL": "openai.gpt-5.5", "OPENAI_API_KEY": "sk-test"}
    recorder = Recorder()
    controller = LlmController(
        config,
        adapters={"bedrock": BedrockConverseAdapter(client=Converse(payload))},
        sleep=lambda _seconds: None,
        timeseries=recorder,
    )
    result = controller.generate(
        messages=[{"role": "user", "content": "hi"}],
        context={"portfolio_id": "portfolio_y", "org_id": "org_x", "extension_id": "lab"},
    )
    assert result["success"]
    recorded = {call["key"]: call for call in recorder.calls}
    assert set(recorded) == {
        "llm.requests",
        "llm.latency_ms",
        "llm.input_tokens",
        "llm.output_tokens",
        "llm.reasoning_tokens",
        "llm.cached_tokens",
        "llm.cost_usd",
    }
    assert "llm.errors" not in recorded
    assert recorded["llm.requests"]["value"] == 1
    assert recorded["llm.input_tokens"]["value"] == 11
    assert recorded["llm.output_tokens"]["value"] == 6
    assert recorded["llm.reasoning_tokens"]["value"] == 2
    assert recorded["llm.cached_tokens"]["value"] == 4
    assert recorded["llm.latency_ms"]["value"] >= 0
    assert recorded["llm.cost_usd"]["value"] == (
        (11 / 1_000_000) * INPUT_PER_MILLION + (6 / 1_000_000) * OUTPUT_PER_MILLION
    )
    dimensions = ["bedrock", "openai.gpt-5.5", "completed", "lab"]
    assert recorded["llm.input_tokens"]["dimensions"] == dimensions
    assert recorded["llm.requests"]["dimensions"] == dimensions
    assert recorded["llm.input_tokens"]["portfolio"] == "portfolio_y"
    assert recorded["llm.input_tokens"]["org"] == "org_x"
    assert recorded["llm.input_tokens"]["resolution"] == "day"
    assert recorded["llm.input_tokens"]["idempotency_key"].startswith("llm_attempt_")
    assert recorded["llm.input_tokens"]["idempotency_key"].endswith(":llm.input_tokens")

    recorder.calls.clear()
    assert controller.generate(messages=[{"role": "user", "content": "hi"}])["success"]
    assert recorder.calls == []

    failed = LlmController(
        config,
        adapters={"bedrock": BedrockConverseAdapter(client=Converse(BadRequest()))},
        sleep=lambda _seconds: None,
        timeseries=recorder,
    )
    failure = failed.generate(
        messages=[{"role": "user", "content": "hi"}],
        context={"portfolio_id": "portfolio_y", "org_id": "org_x", "extension_id": "lab"},
    )
    assert failure["success"] is False
    failed_keys = {call["key"]: call for call in recorder.calls}
    assert set(failed_keys) == {"llm.requests", "llm.errors", "llm.latency_ms"}
    assert failed_keys["llm.errors"]["value"] == 1
    assert failed_keys["llm.errors"]["dimensions"] == ["bedrock", "openai.gpt-5.5", "failed", "lab"]

    broken = LlmController(
        config,
        adapters={"bedrock": BedrockConverseAdapter(client=Converse(payload))},
        sleep=lambda _seconds: None,
        timeseries=Broken(),
    )
    assert broken.generate(
        messages=[{"role": "user", "content": "hi"}],
        context={"portfolio_id": "portfolio_y", "org_id": "org_x", "extension_id": "lab"},
    )["success"]


def test_route_rejects_missing_org_and_returns_points_for_an_authenticated_caller():
    import importlib.util
    from pathlib import Path

    flask = pytest.importorskip("flask")
    flask_cognito = pytest.importorskip("flask_cognito")
    route_path = (
        Path(__file__).resolve().parents[2]
        / "renglo-api"
        / "renglo_api"
        / "routes"
        / "timeseries_routes.py"
    )
    spec = importlib.util.spec_from_file_location("timeseries_routes_under_test", route_path)
    routes = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(routes)

    _table, _model, controller = store()
    moment = clock(2026, 10, 20)
    controller.put(
        "portfolio_y",
        "org_x",
        "room_temperature",
        21.5,
        dimensions=["zone37", "sensor34"],
        timestamp=moment,
    )
    app = flask.Flask(__name__)
    app.renglo_config = {}
    app.config["COGNITO_REGION"] = "us-east-1"
    app.config["COGNITO_USERPOOL_ID"] = "us-east-1_test"
    app.config["COGNITO_APP_CLIENT_ID"] = "client"
    flask_cognito.CognitoAuth(app)
    app.register_blueprint(routes.app_timeseries)
    routes.TSC = controller
    client = app.test_client()
    denied = client.get("/_timeseries/portfolio_y/org_x/room_temperature?date=2026-10-20")
    assert denied.status_code == 401

    with app.test_request_context("/_timeseries/portfolio_y/_all/room_temperature"):
        missing, status = routes.route_get.__wrapped__("portfolio_y", " ", "room_temperature")
    assert status == 400
    assert missing.get_json()["success"] is False

    with app.test_request_context(
        "/_timeseries/portfolio_y/org_x/room_temperature?date=2026-10-20&dimensions=zone37,sensor34"
    ):
        body, status = routes.route_get.__wrapped__("portfolio_y", "org_x", "room_temperature")
    assert status == 200
    payload = body.get_json()
    assert payload["success"]
    assert payload["output"]["readings"][0][1] == 21.5
    assert make_index("portfolio_y", "org_x", "room_temperature") == payload["output"]["index"]
