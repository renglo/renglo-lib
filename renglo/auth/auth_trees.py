"""User, team, and portfolio auth trees stored beside the old monolithic key.

Keys are auth/tree/user/{id}, auth/tree/team/{id}, and auth/tree/portfolio/{id}.
Nothing here reads or writes auth/tree/{user_id}. A missing object is rebuilt
from DynamoDB and stored. Writes coalesce inside a funnel and flush each dirty
tree once.
"""
from __future__ import annotations

import copy
import io
import json
from contextlib import contextmanager
from functools import wraps

import boto3

from renglo.auth.entity_status import entity_is_deleted

TREE_KINDS = ("user", "team", "portfolio")
INVITE_REL_TYPES = frozenset({
    "email:hash:ttl",
    "hash:team",
    "team:invite",
    "team:accept",
    "team:join",
    "join:team",
    "team:access",
})
TEAM_GRANT_REL_TYPES = frozenset({
    "team:tool",
    "team:extension",
    "team/tool:role",
    "team/tool:org",
    "team/extension:role",
    "team/extension:org",
})


class _MissingTree(Exception):
    response = {"Error": {"Code": "NoSuchKey", "Message": "Not found"}}


class MemoryTreeS3:
    """In-memory stand-in for the auth bucket. Tests inject this as store._client."""

    def __init__(self, fail_put=False):
        self.objects = {}
        self.gets = []
        self.puts = []
        self.fail_put = fail_put

    def get_object(self, Bucket, Key):
        self.gets.append(Key)
        body = self.objects.get(Key)
        if body is None:
            raise _MissingTree()
        return {"Body": io.BytesIO(body)}

    def put_object(self, Bucket, Key, Body):
        if self.fail_put:
            raise RuntimeError("s3 put failed")
        if isinstance(Body, str):
            Body = Body.encode()
        self.objects[Key] = bytes(Body)
        self.puts.append(Key)
        return {}


def coalesced_trees(fn):
    """Mark trees dirty for the whole funnel and flush each one once on the way out."""

    @wraps(fn)
    def wrapper(self, *args, **kwargs):
        self.trees.begin()
        try:
            result = fn(self, *args, **kwargs)
        finally:
            flushed = self.trees.end()
        if (
            isinstance(result, dict)
            and result.get("success")
            and not flushed.get("success")
        ):
            return flushed
        return result

    return wrapper


class AuthTreeStore:
    def __init__(self, controller):
        self.controller = controller
        self._depth = 0
        self._dirty = set()
        self._team_ops = []
        self._client = None

    @staticmethod
    def key(kind, ident):
        return f"auth/tree/{kind}/{ident}"

    def begin(self):
        self._depth += 1

    def end(self):
        self._depth = max(0, self._depth - 1)
        if self._depth == 0:
            return self.flush()
        return {"success": True, "status": 200}

    @contextmanager
    def writes(self):
        self.begin()
        try:
            yield
        finally:
            self._last_flush = self.end()

    def note(self, kind, ident):
        self._mark(kind, ident)
        if self._depth == 0:
            return self.flush()
        return {"success": True, "status": 200}

    def note_rel(self, reltype, data, removing=False):
        data = data or {}
        if reltype in INVITE_REL_TYPES:
            return {"success": True, "status": 200}
        if reltype in ("user:team", "team:user"):
            return self.note("user", data.get("user_id"))
        if reltype == "team:portfolio":
            self._team_ops.append((
                "remove" if removing else "add",
                data.get("team_id"),
                data.get("portfolio_id"),
            ))
            self._mark("team", data.get("team_id"))
            self._mark("portfolio", data.get("portfolio_id"))
            if self._depth == 0:
                return self.flush()
            return {"success": True, "status": 200}
        if reltype in TEAM_GRANT_REL_TYPES:
            return self.note("team", data.get("team_id"))
        return {"success": True, "status": 200}

    def note_entity(self, entity_type, document, lookup, deleted=False):
        document = document or {}
        lookup = lookup or {}
        if entity_type == "user":
            return self.note("user", lookup.get("user_id") or document.get("_id"))
        if entity_type == "team":
            return self.note("team", lookup.get("team_id") or document.get("_id"))
        if entity_type == "portfolio":
            portfolio_id = lookup.get("portfolio_id") or document.get("_id")
            if deleted:
                return self.note_portfolio_and_teams(portfolio_id)
            return self.note("portfolio", portfolio_id)
        if entity_type in ("org", "tool", "extension"):
            portfolio_id = lookup.get("portfolio_id")
            if deleted:
                return self.note_portfolio_and_teams(portfolio_id)
            return self.note("portfolio", portfolio_id)
        return {"success": True, "status": 200}

    def note_portfolio_and_teams(self, portfolio_id):
        if not portfolio_id:
            return {"success": True, "status": 200}
        current, err = self._get_raw("portfolio", portfolio_id)
        if err:
            return err
        self._mark("portfolio", portfolio_id)
        for team_id in (current or {}).get("team_ids") or []:
            self._mark("team", team_id)
        if self._depth == 0:
            return self.flush()
        return {"success": True, "status": 200}

    def flush(self):
        dirty = list(self._dirty)
        ops = list(self._team_ops)
        self._dirty = set()
        self._team_ops = []
        if not dirty:
            return {"success": True, "status": 200, "flushed": []}
        built = []
        for kind, ident in dirty:
            previous = None
            if kind == "portfolio":
                previous, err = self._get_raw(kind, ident)
                if err:
                    return err
            document, err = self._build(kind, ident, previous=previous, team_ops=ops)
            if err:
                return err
            built.append((kind, ident, document))
        flushed = []
        for kind, ident, document in built:
            stored = self._put(kind, ident, document)
            if not stored.get("success"):
                return stored
            flushed.append({"kind": kind, "id": ident})
        return {"success": True, "status": 200, "flushed": flushed}

    def load(self, kind, ident, cache):
        """Return one tree, rebuilding it from Dynamo when the object is absent."""
        if not ident:
            return None, _failure("Missing tree id", 400)
        key = (kind, ident)
        if key in cache:
            return cache[key], None
        raw, err = self._get_raw(kind, ident)
        if err:
            return None, err
        if raw is None:
            raw, err = self._build(kind, ident)
            if err:
                return None, err
            stored = self._put(kind, ident, raw)
            if not stored.get("success"):
                return None, stored
        cache[key] = raw
        return raw, None

    def assemble(self, user_id):
        """Frontend tree. Grants are unioned here and are not stored on a portfolio."""
        if not user_id:
            return _failure("User identity required", 401)
        cache = {}
        user, err = self.load("user", user_id, cache)
        if err:
            return err
        tree = {
            "user_id": user_id,
            "preferences": (user or {}).get("preferences") or {},
            "portfolios": {},
        }
        for affiliation in (user or {}).get("teams") or []:
            team_id = affiliation.get("team_id")
            portfolio_id = affiliation.get("portfolio_id")
            if not team_id or not portfolio_id:
                continue
            if portfolio_id not in tree["portfolios"]:
                portfolio, err = self.load("portfolio", portfolio_id, cache)
                if err:
                    return err
                if (portfolio or {}).get("deleted"):
                    continue
                tree["portfolios"][portfolio_id] = _portfolio_shell(portfolio_id, portfolio)
            team, err = self.load("team", team_id, cache)
            if err:
                return err
            node = tree["portfolios"].get(portfolio_id)
            if not node:
                continue
            node["teams"][team_id] = _team_shell(team_id, team)
            _union_grants(node, team)
        self.controller.normalize_auth_tree(tree)
        return {"success": True, "document": tree, "status": 200}

    def repair(self, user_id):
        """Rebuild this caller's user tree and the team and portfolio trees they use."""
        if not user_id:
            return _failure("User identity required", 400)
        user, err = self._build("user", user_id)
        if err:
            return err
        stored = self._put("user", user_id, user)
        if not stored.get("success"):
            return stored
        by_portfolio = {}
        for affiliation in user.get("teams") or []:
            team_id = affiliation.get("team_id")
            portfolio_id = affiliation.get("portfolio_id")
            if not team_id:
                continue
            team, err = self._build("team", team_id)
            if err:
                return err
            stored = self._put("team", team_id, team)
            if not stored.get("success"):
                return stored
            if portfolio_id:
                by_portfolio.setdefault(portfolio_id, []).append(team_id)
        for portfolio_id, team_ids in by_portfolio.items():
            previous, err = self._get_raw("portfolio", portfolio_id)
            if err:
                return err
            ops = [("add", team_id, portfolio_id) for team_id in team_ids]
            document, err = self._build(
                "portfolio", portfolio_id, previous=previous, team_ops=ops
            )
            if err:
                return err
            stored = self._put("portfolio", portfolio_id, document)
            if not stored.get("success"):
                return stored
        return self.assemble(user_id)

    def scope(self, user_id, portfolio_id):
        """User tree plus the team trees on one portfolio, and that portfolio tree."""
        cache = {}
        user, err = self.load("user", user_id, cache)
        if err:
            return None, err
        affiliations = [
            item
            for item in (user or {}).get("teams") or []
            if item.get("portfolio_id") == portfolio_id and item.get("team_id")
        ]
        teams = {}
        for item in affiliations:
            team, err = self.load("team", item["team_id"], cache)
            if err:
                return None, err
            teams[item["team_id"]] = team
        portfolio = None
        if affiliations:
            portfolio, err = self.load("portfolio", portfolio_id, cache)
            if err:
                return None, err
        return {"user": user, "teams": teams, "portfolio": portfolio}, None

    def build_user(self, user_id):
        entity = self.controller.get_entity("user", user_id=user_id)
        document = entity.get("document") if entity.get("success") else {}
        listed = self.controller.AUM.list_rel(
            "irn:rel:user:team:" + user_id + ":*",
            limit=1000,
        )
        if not listed or not listed.get("success"):
            return None, _failure("Failed to read team membership", 500)
        teams = []
        for item in ((listed.get("document") or {}).get("items") or []):
            team_id = item.get("rel")
            if not team_id:
                continue
            teams.append({
                "team_id": team_id,
                "portfolio_id": self.controller._portfolio_id_for_team(team_id) or "",
            })
        return {
            "user_id": user_id,
            "preferences": (document or {}).get("preferences") or {},
            "teams": teams,
        }, None

    def build_team(self, team_id):
        portfolio_id = self.controller._portfolio_id_for_team(team_id) or ""
        document = {
            "team_id": team_id,
            "portfolio_id": portfolio_id,
            "name": "",
            "preferences": {},
            "tools": {},
            "extensions": {},
            "tools_access": [],
            "extensions_access": [],
        }
        if not portfolio_id:
            return document, None
        team_entity = self.controller.get_entity(
            "team", portfolio_id=portfolio_id, team_id=team_id
        )
        if not team_entity.get("success"):
            return document, None
        entity = team_entity.get("document") or {}
        document["name"] = entity.get("name") or ""
        document["preferences"] = entity.get("preferences") or {}
        document["tools_access"] = self.controller._rel_item_rels(
            "irn:rel:team:tool:" + team_id + ":*"
        )
        document["extensions_access"] = self.controller._rel_item_rels(
            "irn:rel:team:extension:" + team_id + ":*"
        )
        for kind in self.controller.INSTALLABLE_TYPES:
            index = self.controller._installable_entity_index(kind, portfolio_id)
            listed = self.controller.AUM.list_entity(index)
            if listed is None or (
                isinstance(listed, dict) and listed.get("success") is False
            ):
                return None, _failure("Failed to read installables", 500)
        scratch = {
            "portfolios": {
                portfolio_id: {
                    "teams": {team_id: document},
                    "tools": {},
                    "extensions": {},
                    "orgs": {},
                }
            }
        }
        hidden = self.controller._deleted_org_ids(portfolio_id, {})
        for kind in self.controller.INSTALLABLE_TYPES:
            self.controller._project_installables_for_team(
                portfolio_id,
                team_id,
                kind,
                scratch,
                {"tools": {}, "extensions": {}},
                [],
                hidden_org_ids=hidden,
            )
        return document, None

    def build_portfolio(self, portfolio_id, previous=None, team_ops=None):
        team_ids = _apply_team_ops(
            (previous or {}).get("team_ids") or [],
            portfolio_id,
            team_ops or [],
        )
        entity = self.controller.get_entity(
            "portfolio",
            portfolio_id=portfolio_id,
            include_deleted=True,
        )
        if not entity.get("success") or entity_is_deleted(entity.get("document")):
            return {
                "portfolio_id": portfolio_id,
                "deleted": True,
                "name": ((entity.get("document") or {}).get("name") or ""),
                "preferences": {},
                "orgs": {},
                "tools": {},
                "extensions": {},
                "team_ids": team_ids,
            }, None
        source = entity.get("document") or {}
        orgs, err = self._org_records(portfolio_id)
        if err:
            return None, err
        tools, err = self._catalog(portfolio_id, "tool")
        if err:
            return None, err
        extensions, err = self._catalog(portfolio_id, "extension")
        if err:
            return None, err
        return {
            "portfolio_id": portfolio_id,
            "name": source.get("name") or "",
            "preferences": source.get("preferences") or {},
            "orgs": orgs,
            "tools": tools,
            "extensions": extensions,
            "team_ids": team_ids,
        }, None

    def _org_records(self, portfolio_id):
        index = "irn:entity:portfolio/org:" + portfolio_id + "/*"
        listed = self.controller.AUM.list_entity(index)
        if not listed or not listed.get("success"):
            return None, _failure("Failed to read orgs", 500)
        orgs = {}
        for org in ((listed.get("document") or {}).get("items") or []):
            if entity_is_deleted(org) or not org.get("_id"):
                continue
            org_id = org["_id"]
            orgs[org_id] = {
                "org_id": org_id,
                "name": org.get("name") or "",
                "handle": org.get("handle") or "",
                "tags": org.get("tags") or {},
                "preferences": org.get("preferences") or {},
            }
        return orgs, None

    def _catalog(self, portfolio_id, kind):
        index = self.controller._installable_entity_index(kind, portfolio_id)
        listed = self.controller.AUM.list_entity(index)
        if not listed or not listed.get("success"):
            return None, _failure("Failed to read installables", 500)
        catalog = {}
        for entity in ((listed.get("document") or {}).get("items") or []):
            if entity_is_deleted(entity) or not entity.get("_id"):
                continue
            entity_id = entity["_id"]
            node = {
                "name": entity.get("name"),
                "handle": entity.get("handle"),
                "roles": self.controller._normalize_roles_list(entity.get("roles")),
                "entity_type": kind,
                "preferences": entity.get("preferences") or {},
            }
            if kind == "extension":
                node["extension_id"] = entity_id
            else:
                node["tool_id"] = entity_id
            catalog[entity_id] = node
        return catalog, None

    def _build(self, kind, ident, previous=None, team_ops=None):
        if kind == "user":
            return self.build_user(ident)
        if kind == "team":
            return self.build_team(ident)
        if kind == "portfolio":
            return self.build_portfolio(ident, previous=previous, team_ops=team_ops)
        return None, _failure("Unknown auth tree", 500)

    def _mark(self, kind, ident):
        if kind not in TREE_KINDS or not ident:
            return
        self._dirty.add((kind, str(ident)))

    def _bucket(self):
        return (self.controller.config or {}).get("S3_BUCKET_NAME")

    def _s3(self):
        if self._client is not None:
            return self._client
        return boto3.client("s3")

    def _get_raw(self, kind, ident):
        bucket = self._bucket()
        if not bucket:
            return None, _failure("S3_BUCKET_NAME not configured", 500)
        try:
            response = self._s3().get_object(Bucket=bucket, Key=self.key(kind, ident))
            body = response["Body"].read()
            return json.loads(body), None
        except Exception as exc:
            if _is_missing(exc):
                return None, None
            self.controller.logger.error(
                "Failed to read auth tree %s/%s: %s", kind, ident, exc
            )
            return None, _failure("Failed to read auth tree", 500)

    def _put(self, kind, ident, document):
        bucket = self._bucket()
        if not bucket:
            return _failure("S3_BUCKET_NAME not configured", 500)
        try:
            self._s3().put_object(
                Bucket=bucket,
                Key=self.key(kind, ident),
                Body=json.dumps(document, default=str),
            )
        except Exception as exc:
            self.controller.logger.error(
                "Failed to upload auth tree %s/%s: %s", kind, ident, exc
            )
            return _failure("Failed to upload to S3", 500)
        return {"success": True, "status": 200, "document": document}


def _failure(message, status):
    return {"success": False, "message": message, "status": status}


def _is_missing(exc):
    response = getattr(exc, "response", None) or {}
    code = str((response.get("Error") or {}).get("Code") or "")
    return code in ("NoSuchKey", "404", "NotFound")


def _apply_team_ops(existing, portfolio_id, ops):
    team_ids = []
    seen = set()
    for team_id in existing:
        if team_id and team_id not in seen:
            seen.add(team_id)
            team_ids.append(team_id)
    for op, team_id, target in ops:
        if target != portfolio_id or not team_id or team_id in (None, ""):
            continue
        if op == "add" and team_id not in seen:
            seen.add(team_id)
            team_ids.append(team_id)
        elif op == "remove" and team_id in seen:
            seen.remove(team_id)
            team_ids = [item for item in team_ids if item != team_id]
    return team_ids


def _portfolio_shell(portfolio_id, portfolio):
    portfolio = portfolio or {}
    orgs = {}
    for org_id, org in (portfolio.get("orgs") or {}).items():
        node = dict(org or {})
        node["tools"] = []
        node["extensions"] = []
        orgs[org_id] = node
    return {
        "portfolio_id": portfolio_id,
        "name": portfolio.get("name") or "",
        "preferences": portfolio.get("preferences") or {},
        "teams": {},
        "orgs": orgs,
        "tools": {
            entity_id: dict(node or {})
            for entity_id, node in (portfolio.get("tools") or {}).items()
        },
        "extensions": {
            entity_id: dict(node or {})
            for entity_id, node in (portfolio.get("extensions") or {}).items()
        },
    }


def _team_shell(team_id, team):
    team = team or {}
    return {
        "team_id": team.get("team_id") or team_id,
        "name": team.get("name") or "",
        "preferences": team.get("preferences") or {},
        "tools": copy.deepcopy(team.get("tools") or {}),
        "extensions": copy.deepcopy(team.get("extensions") or {}),
        "tools_access": list(team.get("tools_access") or []),
        "extensions_access": list(team.get("extensions_access") or []),
    }


def _union_grants(portfolio_node, team):
    for bag in ("tools", "extensions"):
        for entity_id, access in ((team or {}).get(bag) or {}).items():
            for org_id in (access or {}).get("orgs") or []:
                org_node = (portfolio_node.get("orgs") or {}).get(org_id)
                if not org_node:
                    continue
                org_node.setdefault(bag, [])
                if entity_id not in org_node[bag]:
                    org_node[bag].append(entity_id)
                org_node["active"] = True
                catalog = (portfolio_node.get(bag) or {}).get(entity_id)
                if catalog is not None:
                    catalog["active"] = True
