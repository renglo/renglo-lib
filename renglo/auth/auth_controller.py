#auth_controller.py
import copy
import logging
from datetime import datetime
from ..common import *
import uuid
from decimal import Decimal
from renglo.auth.auth_model import AuthModel
from renglo.auth.auth_trees import AuthTreeStore, coalesced_trees
from renglo.auth.federated_identity import (
    USER_IDENTITY_INDEX,
    identities_from_claims,
    identity_document,
    identity_key,
)
from renglo.auth.entity_status import (
    ENTITY_STATUS_ACTIVE,
    ENTITY_STATUS_DELETED,
    HARD_DELETE_REFUSED,
    entity_forbids_hard_delete,
    entity_is_deleted,
)
from renglo.common import sanitize_entity_preferences, sanitize_entity_tags
from renglo.wl import invite_inline_images, render_invite_email
import re
import secrets
import time
from urllib.parse import quote
from validate_email import validate_email
from renglo.runtime import get_current_jwt_claims


class AuthController:
    # Sentinel org id for portfolio-scoped data rings (config, schd_tools, etc.).
    # Not a real org entity — must never be treated as "any org".
    PORTFOLIO_SCOPE_ORG = "_all"
    # New installs write 'extension'. Legacy 'tool' rows stay readable until retired.
    INSTALLABLE_TYPES = ("extension", "tool")

    def __init__(self, config=None, tid=False, ip=False):
        self.config = config or {}
        self.AUM = AuthModel(config=self.config)
        self.trees = AuthTreeStore(self)
        self._invocation_user_id = None
        self._invocation_jwt_claims = None
        # Set up logger
        self.logger = logging.getLogger(self.__class__.__name__)
        # Configure logger if not already configured (prevents duplicate handlers)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.DEBUG)
        
        
    def refresh_tree(self):
        return self.refresh_user_tree(self.get_current_user())

    def refresh_user_tree(self, user_id):
        """Rebuild this caller's user tree and the team and portfolio trees they use."""
        if not user_id:
            return {"success": False, "message": "User identity required", "status": 400}
        self.logger.error("Refreshing tree")
        return self.trees.repair(user_id)

    def get_cached_tree(self, user_id):
        """Assemble the frontend tree from the user, team, and portfolio trees."""
        if not user_id:
            return {
                "success": False,
                "message": "User identity required",
                "status": 401,
            }
        return self.trees.assemble(user_id)

    @staticmethod
    def _normalize_roles_list(raw):
        """Normalize a roles catalog/assignment value to a list of non-empty strings."""
        if raw is None:
            return []
        if isinstance(raw, str):
            raw = [raw]
        if not isinstance(raw, (list, tuple)):
            return []
        out = []
        seen = set()
        for item in raw:
            role = str(item).strip()
            if role and role not in seen:
                seen.add(role)
                out.append(role)
        return out

    @staticmethod
    def _installable_entity_index(kind, portfolio_id, archived=False):
        prefix = "unentity" if archived else "entity"
        return f"irn:{prefix}:portfolio/{kind}:{portfolio_id}/*"

    @staticmethod
    def _installable_rel_types(kind):
        return {
            "assign": f"team:{kind}",
            "role": f"team/{kind}:role",
            "org": f"team/{kind}:org",
        }

    @staticmethod
    def _installable_id(data):
        return (data or {}).get("extension_id") or (data or {}).get("tool_id")

    def get_installable(self, portfolio_id, entity_id):
        """Look up an installable by id, preferring extension over legacy tool."""
        if not portfolio_id or not entity_id:
            return None
        for kind in self.INSTALLABLE_TYPES:
            response = self.get_entity(
                kind,
                portfolio_id=portfolio_id,
                tool_id=entity_id,
                extension_id=entity_id,
            )
            if response.get("success"):
                return {"kind": kind, "document": response.get("document") or {}}
        return None

    def find_installable_by_handle(self, portfolio_id, handle):
        """Look up an installable by handle, preferring extension over legacy tool."""
        handle = str(handle or "").strip()
        if not portfolio_id or not handle:
            return None
        for kind in self.INSTALLABLE_TYPES:
            response = self.list_entity(kind, portfolio_id=portfolio_id)
            items = ((response or {}).get("document") or {}).get("items") or []
            for item in items:
                if str((item or {}).get("handle") or "").strip() == handle:
                    return {"kind": kind, "document": item}
        return None

    def _portfolio_id_for_team(self, team_id):
        if not team_id:
            return None
        index = f"irn:rel:team:portfolio:{team_id}:*"
        items = ((self.AUM.list_rel(index) or {}).get("document") or {}).get("items") or []
        if not items:
            return None
        return items[0].get("rel")

    def _rel_item_rels(self, index):
        items = ((self.AUM.list_rel(index) or {}).get("document") or {}).get("items") or []
        return [item.get("rel") for item in items if item.get("rel")]

    @classmethod
    def _tool_handle_catalog(cls, portfolio_node):
        catalog = []
        seen = set()
        for bag_name in ("tools", "extensions"):
            bag = (portfolio_node or {}).get(bag_name) or {}
            for tid, tdoc in bag.items():
                if tid in seen:
                    continue
                seen.add(tid)
                catalog.append({
                    "tool_id": tid,
                    "handle": str((tdoc or {}).get("handle") or "").strip(),
                    "name": str((tdoc or {}).get("name") or "").strip(),
                    "entity_type": str((tdoc or {}).get("entity_type") or bag_name.rstrip("s")),
                })
        return catalog

    @staticmethod
    def _resolve_tool_ref(portfolio_node, tool_ref):
        """
        Resolve a tool/extension id or handle using the portfolio tree node.
        Returns the entity id or None if not found.
        """
        if not portfolio_node or not tool_ref:
            return None
        ref = str(tool_ref).strip()
        for bag_name in ("tools", "extensions"):
            bag = portfolio_node.get(bag_name) or {}
            if ref in bag:
                return ref
            for tid, tdoc in bag.items():
                if str((tdoc or {}).get("handle") or "").strip() == ref:
                    return tid
        return None

    @classmethod
    def is_portfolio_scope_org(cls, org) -> bool:
        """True when org is the portfolio-scoped pseudo-org (_all)."""
        return str(org or "").strip() == cls.PORTFOLIO_SCOPE_ORG

    @staticmethod
    def _team_installable_access(team, entity_id):
        """Grant node for an id on a team (extension first, then legacy tool)."""
        if not team or not entity_id:
            return {}
        return (
            ((team.get("extensions") or {}).get(entity_id) or {})
            or ((team.get("tools") or {}).get(entity_id) or {})
        )

    def _ensure_portfolio_scope_org(self, portfolio_node):
        """
        Inject the portfolio-scoped org (_all) into the auth tree.

        Unlike entity-backed orgs, _all is not stored in DynamoDB. Team/tool/org
        rels may reference org_id=_all; active is derived from those grants.
        """
        org_id = self.PORTFOLIO_SCOPE_ORG
        active = False
        tools_for_all = []
        extensions_for_all = []
        for team in (portfolio_node.get("teams") or {}).values():
            for bag, dest in (("tools", tools_for_all), ("extensions", extensions_for_all)):
                for entity_id, access in ((team or {}).get(bag) or {}).items():
                    if org_id in (access.get("orgs") or []):
                        active = True
                        dest.append(entity_id)

        if "orgs" not in portfolio_node:
            portfolio_node["orgs"] = {}

        portfolio_node["orgs"][org_id] = {
            "org_id": org_id,
            "name": "ALL",
            "handle": org_id,
            "tags": {},
            "preferences": {},
            "tools": list(set(tools_for_all)),
            "extensions": list(set(extensions_for_all)),
            "active": active,
            "portfolio_scope": True,
        }

    def normalize_auth_tree(self, tree):
        """Ensure portfolio-scoped pseudo-org (_all) exists on every portfolio node."""
        if not isinstance(tree, dict):
            return tree
        for portfolio_node in (tree.get("portfolios") or {}).values():
            self._ensure_portfolio_scope_org(portfolio_node)
        return tree

    def _has_active_entity_org(self, portfolio_node) -> bool:
        """True when the user has at least one real (non-_all) active org."""
        for org_id, org_node in (portfolio_node.get("orgs") or {}).items():
            if self.is_portfolio_scope_org(org_id):
                continue
            if (org_node or {}).get("active") is True:
                return True
        return False

    @staticmethod
    def _collect_roles_for_tool_org(portfolio_node, org, tool_id):
        """
        Union of roles assigned to any of the user's teams for tool_id,
        limited to teams that also have org access for that tool.
        """
        if not portfolio_node or not org or not tool_id:
            return []
        roles = []
        seen = set()
        teams = portfolio_node.get("teams") or {}
        for team in teams.values():
            tool_access = AuthController._team_installable_access(team, tool_id)
            orgs = tool_access.get("orgs") or []
            if org not in orgs:
                continue
            for role in tool_access.get("roles") or []:
                role_id = str(role).strip()
                if role_id and role_id not in seen:
                    seen.add(role_id)
                    roles.append(role_id)
        return roles

    @staticmethod
    def _org_granted(teams, org_id):
        for team in (teams or {}).values():
            for bag in ("tools", "extensions"):
                for access in ((team or {}).get(bag) or {}).values():
                    if org_id in ((access or {}).get("orgs") or []):
                        return True
        return False

    @classmethod
    def _granted_entity_org(cls, teams, portfolio_orgs):
        for org_id in portfolio_orgs or {}:
            if cls.is_portfolio_scope_org(org_id):
                continue
            if cls._org_granted(teams, org_id):
                return True
        return False

    @staticmethod
    def _tool_granted(teams, tool_id, org_id):
        for team in (teams or {}).values():
            access = AuthController._team_installable_access(team, tool_id)
            if org_id in (access.get("orgs") or []):
                return True
        return False

    def resolve_tool_roles(self, portfolio, org, tool_id, user_id=None):
        """
        Resolve the caller's roles for a tool in an org from the team trees
        on that portfolio. tool_id may be an id or a handle.
        """
        resolved_user_id = user_id if user_id is not None else self.get_current_user()
        if not resolved_user_id:
            return []
        scope, err = self.trees.scope(resolved_user_id, portfolio)
        if err or not scope or not scope.get("teams"):
            return []
        portfolio_node = scope.get("portfolio") or {}
        if portfolio_node.get("deleted"):
            return []
        resolved_tool_id = self._resolve_tool_ref(portfolio_node, tool_id)
        if not resolved_tool_id:
            return []
        return self._collect_roles_for_tool_org(
            {"teams": scope["teams"]}, org, resolved_tool_id
        )

    def authorize(
        self,
        portfolio,
        org,
        *,
        resource="org",
        action="access",
        tool_id=None,
        user_id=None,
    ):
        """
        Authorize the current (or given) user from the user tree and the team
        trees on this portfolio. The portfolio tree is read to confirm the org
        and to resolve a handle. Other portfolios are not loaded.

        resource='org': user may access the org when one of those teams grants it.
        org='_all' is a portfolio-scoped pseudo-org (not in DynamoDB). For
        resource='org', _all is allowed when the user has any active entity org
        in the portfolio (extension grants on real orgs). For resource='tool',
        _all still requires an explicit team/tool:org rel with org_id=_all.
        resource='tool': also requires a team grant of tool_id in that org
        (team/tool:org). Returns roles for the tool in that org when tool_id is set.

        action is reserved for future role→action checks inside extensions.
        """
        resolved_user_id = user_id if user_id is not None else self.get_current_user()
        if not resolved_user_id:
            return {
                "success": False,
                "message": "Authentication required",
                "status": 401,
                "user_id": None,
                "roles": [],
            }

        if resource not in ("org", "tool", "extension"):
            return {
                "success": False,
                "message": f"Authorization for resource '{resource}' is not implemented",
                "status": 501,
                "user_id": resolved_user_id,
                "roles": [],
            }

        scope, err = self.trees.scope(resolved_user_id, portfolio)
        if err:
            return {
                "success": False,
                "message": err.get("message", "Failed to load auth tree"),
                "status": err.get("status", 500),
                "user_id": resolved_user_id,
                "roles": [],
            }

        teams = (scope or {}).get("teams") or {}
        portfolio_node = (scope or {}).get("portfolio") or {}
        if not teams or portfolio_node.get("deleted"):
            self.logger.debug(
                "Auth deny: user %s has no access to portfolio %s",
                resolved_user_id,
                portfolio,
            )
            return {
                "success": False,
                "message": "Access denied to organization",
                "status": 403,
                "user_id": resolved_user_id,
                "roles": [],
            }

        org_key = str(org or "").strip()
        if not org_key:
            self.logger.debug(
                "Auth deny: user %s missing org for %s",
                resolved_user_id,
                portfolio,
            )
            return {
                "success": False,
                "message": "Access denied to organization",
                "status": 403,
                "user_id": resolved_user_id,
                "roles": [],
            }

        portfolio_orgs = portfolio_node.get("orgs") or {}
        if self.is_portfolio_scope_org(org_key):
            # Org-level operations (files, data rings) succeed when the user
            # already has an active entity org via an extension.
            if resource == "org" and not self._granted_entity_org(teams, portfolio_orgs):
                self.logger.debug(
                    "Auth deny: user %s has no active org in portfolio %s for _all",
                    resolved_user_id,
                    portfolio,
                )
                return {
                    "success": False,
                    "message": "Access denied to organization",
                    "status": 403,
                    "user_id": resolved_user_id,
                    "roles": [],
                }
        elif org_key not in portfolio_orgs or not self._org_granted(teams, org_key):
            self.logger.debug(
                "Auth deny: user %s has no active access to org %s/%s",
                resolved_user_id,
                portfolio,
                org_key,
            )
            return {
                "success": False,
                "message": "Access denied to organization",
                "status": 403,
                "user_id": resolved_user_id,
                "roles": [],
            }

        resolved_tool_id = None
        roles = []
        if tool_id:
            catalog = self._tool_handle_catalog(portfolio_node)
            resolved_tool_id = self._resolve_tool_ref(portfolio_node, tool_id)
            self.logger.info(
                "Auth tool resolve: user=%s portfolio=%s org=%s ref=%s resolved=%s catalog=%s",
                resolved_user_id,
                portfolio,
                org_key,
                tool_id,
                resolved_tool_id,
                catalog,
            )
            if resource in ("tool", "extension"):
                if not resolved_tool_id:
                    self.logger.info(
                        "Auth deny: user %s tool ref %r did not match any tool in portfolio %s (catalog=%s)",
                        resolved_user_id,
                        tool_id,
                        portfolio,
                        catalog,
                    )
                    return {
                        "success": False,
                        "message": "Access denied to tool",
                        "status": 403,
                        "user_id": resolved_user_id,
                        "roles": [],
                    }
                if not self._tool_granted(teams, resolved_tool_id, org_key):
                    self.logger.info(
                        "Auth deny: user %s has no tool %s access in org %s/%s",
                        resolved_user_id,
                        resolved_tool_id,
                        portfolio,
                        org_key,
                    )
                    return {
                        "success": False,
                        "message": "Access denied to tool",
                        "status": 403,
                        "user_id": resolved_user_id,
                        "roles": [],
                    }
            if resolved_tool_id:
                roles = self._collect_roles_for_tool_org(
                    {"teams": teams}, org_key, resolved_tool_id
                )

        # action reserved for future extension-level checks
        _ = action

        return {
            "success": True,
            "message": "Authorized",
            "status": 200,
            "user_id": resolved_user_id,
            "roles": roles,
            "tool_id": resolved_tool_id,
        }

    def require_org_access(self, portfolio, org, user_id=None):
        """Round-1 helper: require org-level access for portfolio/org."""
        return self.authorize(
            portfolio, org, resource="org", action="access", user_id=user_id
        )
        
        
        
    
    def set_invocation_user(self, user_id):
        """Set the current user for this invocation (e.g. from payload in Lambda/Docker)."""
        self._invocation_user_id = user_id

    def set_invocation_jwt_claims(self, jwt_claims):
        """Set JWT claims for this invocation; get_current_user() will derive user_id from them."""
        self._invocation_jwt_claims = jwt_claims

    @staticmethod
    def _user_id_from_claims(claims):
        if not claims:
            return None
        if "cognito:username" in claims:
            return create_md5_hash(claims["cognito:username"], 9)
        if "username" in claims:
            return create_md5_hash(claims["username"], 9)
        return None

    def get_current_user(self, jwt_claims=None):
        """
        Return the current user id. Precedence:
        1. jwt_claims argument (if provided)
        2. _invocation_user_id (set by set_invocation_user for Lambda/Docker)
        3. _invocation_jwt_claims (set from payload)
        4. Runtime adapter claims (if available)
        """
        if jwt_claims is not None:
            uid = self._user_id_from_claims(jwt_claims)
            if uid is not None:
                return uid
        if getattr(self, "_invocation_user_id", None) is not None:
            return self._invocation_user_id
        if getattr(self, "_invocation_jwt_claims", None) is not None:
            uid = self._user_id_from_claims(self._invocation_jwt_claims)
            if uid is not None:
                return uid
        claims = get_current_jwt_claims()
        uid = self._user_id_from_claims(claims)
        if uid is not None:
            return uid
        return None

    def record_federated_identities(self, claims):
        """Store each identities-claim pair against the username-derived user id.

        A token with no identities claim is a native login and writes nothing.
        An existing pair that already points at this user is refreshed.
        A pair that points at a different user is left unchanged.
        """
        identities = identities_from_claims(claims)
        if not identities:
            return {"success": True, "message": "No federated identity", "status": 200}
        user_id = self._user_id_from_claims(claims)
        if not user_id:
            return {"success": False, "message": "Missing username claim", "status": 400}
        cognito_sub = str((claims or {}).get("sub") or "")
        email = str((claims or {}).get("email") or "")
        now = datetime.now().isoformat()
        for item in identities:
            outcome = self._record_one_identity(
                provider_name=item["provider_name"],
                provider_subject=item["provider_subject"],
                user_id=user_id,
                cognito_sub=cognito_sub,
                email=email,
                now=now,
            )
            if not outcome.get("success"):
                return outcome
        return {"success": True, "message": "Identity recorded", "status": 200}

    def _record_one_identity(
        self,
        *,
        provider_name,
        provider_subject,
        user_id,
        cognito_sub,
        email,
        now,
    ):
        key = identity_key(provider_name, provider_subject)
        existing = self.AUM.get_entity(USER_IDENTITY_INDEX, key)
        if existing.get("success"):
            return self._refresh_or_reject_identity(
                existing.get("document") or {},
                user_id=user_id,
                now=now,
                provider_name=provider_name,
            )
        document = identity_document(
            provider_name=provider_name,
            provider_subject=provider_subject,
            user_id=user_id,
            cognito_sub=cognito_sub,
            email=email,
            linked_at=now,
        )
        created = self.AUM.put_entity_exclusive(document)
        if created.get("success"):
            self.logger.info(
                "Federated identity recorded provider=%s user_id=%s",
                provider_name,
                user_id,
            )
            return {"success": True, "message": "Identity recorded", "status": 200}
        if created.get("status") != 409:
            return created
        raced = self.AUM.get_entity(USER_IDENTITY_INDEX, key)
        if raced.get("success"):
            return self._refresh_or_reject_identity(
                raced.get("document") or {},
                user_id=user_id,
                now=now,
                provider_name=provider_name,
            )
        return {
            "success": False,
            "message": "Could not record federated identity",
            "status": created.get("status") or 409,
        }

    def _refresh_or_reject_identity(self, document, *, user_id, now, provider_name):
        current = str(document.get("user_id") or "")
        if current != user_id:
            self.logger.error(
                "Federated identity already belongs to another user provider=%s",
                provider_name,
            )
            return {
                "success": False,
                "message": "This identity is already linked to another user.",
                "status": 409,
            }
        updated = dict(document)
        updated["last_authenticated_at"] = now
        updated["modified"] = now
        self.AUM.update_entity(updated)
        return {"success": True, "message": "Identity refreshed", "status": 200}


    INVITE_READY_MESSAGE = "Invitation is ready."

    @staticmethod
    def _normalize_invite_email(email):
        cleaned = str(email or '').strip().lower()
        if not cleaned or any(ch in cleaned for ch in ' \t\r\n') or cleaned.count('@') != 1:
            return ''
        return cleaned

    @staticmethod
    def _user_has_opened_app(document):
        """True only after a real console sign-in, not entity creation.

        Creating a user row stamps last_login with a timestamp. The console
        sets the boolean True when that person actually opens the app.
        """
        return bool(document) and document.get('last_login') is True

    def _invite_still_valid(self, ttl):
        return str(self.generate_ttl(0)) < str(ttl)

    def _invite_code_accepted(self, email, ttl, code):
        """Accept a live stored invite code.

        Older invites use a 6-digit code derived from the server secret.
        New invites use a long random token. The lookup already matched
        email and code, so the derived check only guards the short codes.
        """
        code = str(code or '')
        if not code:
            return False
        if len(code) <= 12:
            return code == self.generate_invite_hash(email, ttl)
        return True

    def _invite_link(self, email, code):
        base = resolve_invite_fe_base_url(self.config).rstrip('/')
        return (
            f"{base}/invite?code={quote(code, safe='')}"
            f"&email={quote(email, safe='')}"
        )

    def _invite_public_document(self, email, row):
        document = {
            'email': email,
            'expires_at': int(row['ttl']),
            'expired': bool(row.get('expired')),
            'membership': 'invited',
        }
        if not document['expired']:
            document['link'] = self._invite_link(email, row['code'])
        return document

    def _caller_on_team(self, user_id, team_id):
        response = self.list_rel('user:team', user_id=user_id)
        items = ((response or {}).get('document') or {}).get('items') or []
        return any(item.get('rel') == team_id for item in items)

    def _invite_context(self, portfolio_id, team_id, sender_id):
        if not self._caller_on_team(sender_id, team_id):
            return {
                "success": False,
                "message": "You are not a member of this team.",
                "status": 403,
            }
        team = self.get_entity('team', portfolio_id=portfolio_id, team_id=team_id)
        if not team.get('success'):
            return team
        portfolio = self.get_entity('portfolio', portfolio_id=portfolio_id)
        if not portfolio.get('success'):
            return portfolio
        sender = self.get_entity('user', user_id=sender_id)
        if not sender.get('success'):
            return sender
        return {
            "success": True,
            "team": team['document'],
            "portfolio": portfolio['document'],
            "sender": sender['document'],
        }

    @staticmethod
    def _parse_team_invite_rel(rel):
        email, code, ttl = str(rel or '').rsplit('|', 2)
        return email, code, ttl

    def _team_invite_rows(self, team_id):
        response = self.list_rel('team:invite', team_id=team_id)
        items = ((response or {}).get('document') or {}).get('items') or []
        rows = []
        for item in items:
            try:
                email, code, ttl = self._parse_team_invite_rel(item.get('rel'))
            except ValueError:
                continue
            rows.append({
                'email': email,
                'code': code,
                'ttl': ttl,
                'expired': not self._invite_still_valid(ttl),
            })
        return rows

    def _delete_team_invite_row(self, team_id, row):
        self.delete_rel(
            'email:hash:ttl',
            email=row['email'],
            hash=row['code'],
            ttl=row['ttl'],
        )
        self.delete_rel('hash:team', hash=row['code'], team_id=team_id)
        self.delete_rel(
            'team:invite',
            team_id=team_id,
            email=row['email'],
            hash=row['code'],
            ttl=row['ttl'],
        )

    def _issue_team_invite(self, email, team_id, replace=False):
        existing = [row for row in self._team_invite_rows(team_id) if row['email'] == email]
        live = [row for row in existing if not row['expired']]
        if live and not replace:
            return live[0]
        for row in existing:
            self._delete_team_invite_row(team_id, row)
        code = secrets.token_urlsafe(32)
        ttl = self.generate_ttl(offset_min=1440)
        created = self.create_rel('email:hash:ttl', email=email, hash=code, ttl=ttl)
        if not created.get('success'):
            return created
        created = self.create_rel('hash:team', hash=code, team_id=team_id)
        if not created.get('success'):
            return created
        created = self.create_rel(
            'team:invite',
            team_id=team_id,
            email=email,
            hash=code,
            ttl=ttl,
        )
        if not created.get('success'):
            return created
        return {'email': email, 'code': code, 'ttl': ttl, 'expired': False}

    def _invitation_result(self, email, row):
        if not isinstance(row, dict) or 'code' not in row:
            return row
        return {
            "success": True,
            "message": self.INVITE_READY_MESSAGE,
            "status": 200,
            "document": self._invite_public_document(email, row),
        }

    def invite_user(self,email,team_id,portfolio_id,sender_id,sender_profile=None):
        '''
        Create a team invitation and return a link the inviter can share.

        The response does not say whether this email already has an account.
        Email is sent only when the inviter asks for it.
        '''
        email = self._normalize_invite_email(email)
        if not email:
            return {"success": False, "message": "Email is not valid", "status": 400}
        context = self._invite_context(portfolio_id, team_id, sender_id)
        if not context.get('success'):
            return context
        row = self._issue_team_invite(email, team_id, replace=False)
        if not isinstance(row, dict) or 'code' not in row:
            return row
        return self._invitation_result(email, row)

    def send_team_invite_email(self, email, team_id, portfolio_id, sender_id, sender_profile=None):
        email = self._normalize_invite_email(email)
        if not email:
            return {"success": False, "message": "Email is not valid", "status": 400}
        context = self._invite_context(portfolio_id, team_id, sender_id)
        if not context.get('success'):
            return context
        live = [
            row for row in self._team_invite_rows(team_id)
            if row['email'] == email and not row['expired']
        ]
        if not live:
            expired = any(row['email'] == email for row in self._team_invite_rows(team_id))
            if expired:
                return {
                    "success": False,
                    "message": "This invitation has expired.",
                    "status": 410,
                }
            return {
                "success": False,
                "message": "There is no invitation to send.",
                "status": 404,
            }
        row = live[0]
        sender = dict(context['sender'])
        profile = sender_profile or {}
        for field, key in (('name', 'name'), ('slot_a', 'slot_a'), ('email', 'email')):
            if not (sender.get(field) or '').strip():
                fallback = (profile.get(key) or '').strip()
                if fallback:
                    sender[field] = fallback
        from_email = (self.config.get('FROM_EMAIL') or '').strip()
        if not from_email:
            return {
                "success": False,
                "message": (
                    "FROM_EMAIL is not configured. Set it in env_config / SSM "
                    "(application SES identity from customer-config email_from)."
                ),
                "status": 500,
            }
        link = self._invite_link(email, row['code'])
        team_label = context['portfolio'].get('name', '') + '/' + context['team'].get('name', '')
        invite_email = render_invite_email(
            inviter=self._inviter_display_name(sender),
            team=team_label,
            code=row['code'],
            link=link,
        )
        sent = self.AUM.send_email(
            sender=from_email,
            recipient=email,
            subject=invite_email.subject,
            body_text=invite_email.body_text,
            body_html=invite_email.body_html,
            inline_images=invite_inline_images(invite_email),
        )
        if not sent.get('success'):
            ses_msg = sent.get('message') or 'Could not send the invite'
            sent['message'] = f'Could not send the invite: {ses_msg}'
            return sent
        result = self._invitation_result(email, row)
        result['message'] = "Invitation email sent."
        return result

    def regenerate_team_invite(self, email, team_id, portfolio_id, sender_id, sender_profile=None):
        email = self._normalize_invite_email(email)
        if not email:
            return {"success": False, "message": "Email is not valid", "status": 400}
        context = self._invite_context(portfolio_id, team_id, sender_id)
        if not context.get('success'):
            return context
        row = self._issue_team_invite(email, team_id, replace=True)
        return self._invitation_result(email, row)

    JOIN_CODE_ALPHABET = '23456789ABCDEFGHJKLMNPQRSTUVWXYZ'
    JOIN_TOKEN_INVALID = 'That invitation token is not valid.'

    @staticmethod
    def admin_team_name(portfolio_name):
        cleaned = str(portfolio_name or '').strip()
        return f'{cleaned} Admin' if cleaned else 'Admin'

    @staticmethod
    def staff_team_name(portfolio_name):
        cleaned = str(portfolio_name or '').strip()
        return f'{cleaned} Staff' if cleaned else 'Staff'

    @classmethod
    def _pick_core_teams(cls, portfolio_name, teams):
        """Choose this portfolio's admin and staff teams by name.

        `teams` must already be limited to one portfolio. A team named Admin
        on another portfolio is not in this list, so it cannot be selected.
        """
        qualified_admin = cls.admin_team_name(portfolio_name)
        qualified_staff = cls.staff_team_name(portfolio_name)
        admin = None
        staff = None
        for team in teams or []:
            if entity_is_deleted(team):
                continue
            name = str(team.get('name') or '').strip()
            if name == qualified_admin:
                admin = team
            elif name == 'Admin' and admin is None:
                admin = team
            elif name == qualified_staff:
                staff = team
            elif name == 'Staff' and staff is None:
                staff = team
        return admin, staff

    def _portfolio_team_documents(self, portfolio_id):
        index = 'irn:entity:portfolio/team:' + portfolio_id + '/*'
        listed = self.AUM.list_entity(index, limit=500)
        if not listed or not listed.get('success'):
            return []
        return ((listed.get('document') or {}).get('items') or [])

    def _caller_in_portfolio(self, user_id, portfolio_id):
        response = self.list_rel('user:team', user_id=user_id)
        items = ((response or {}).get('document') or {}).get('items') or []
        for item in items:
            team_id = item.get('rel')
            if team_id and self._portfolio_id_for_team(team_id) == portfolio_id:
                return True
        return False

    def core_teams_for_member(self, portfolio_id, user_id):
        """Admin and staff ids for a portfolio the caller already belongs to.

        Membership is not changed. A staff member can learn the admin team id
        so a new org can be granted to that team, and still is not added to it.
        """
        if not self._caller_in_portfolio(user_id, portfolio_id):
            return {
                "success": False,
                "message": "You are not a member of this portfolio.",
                "status": 403,
            }
        portfolio = self.get_entity('portfolio', portfolio_id=portfolio_id)
        if not portfolio.get('success'):
            return portfolio
        portfolio_name = (portfolio.get('document') or {}).get('name') or ''
        admin, staff = self._pick_core_teams(
            portfolio_name,
            self._portfolio_team_documents(portfolio_id),
        )

        def public(team):
            if not team or not team.get('_id'):
                return None
            return {'team_id': team['_id'], 'name': team.get('name') or ''}

        return {
            "success": True,
            "status": 200,
            "document": {
                "portfolio_name": portfolio_name,
                "admin": public(admin),
                "staff": public(staff),
            },
        }

    def _new_join_code(self):
        def block():
            return ''.join(secrets.choice(self.JOIN_CODE_ALPHABET) for _ in range(4))
        return f'FRM-{block()}-{block()}'

    @classmethod
    def _normalize_join_code(cls, token):
        cleaned = re.sub(r'[^A-Za-z0-9]', '', str(token or '')).upper()
        if len(cleaned) != 11 or not cleaned.startswith('FRM'):
            return ''
        return f'{cleaned[:3]}-{cleaned[3:7]}-{cleaned[7:]}'

    def _join_code_result(self, code, team_id):
        return {
            "success": True,
            "message": "Open invitation is ready.",
            "status": 200,
            "document": {"code": code, "team_id": team_id},
        }

    def _team_join_rows(self, team_id):
        response = self.list_rel('team:join', team_id=team_id)
        items = ((response or {}).get('document') or {}).get('items') or []
        rows = []
        for item in items:
            code = str(item.get('rel') or '').strip()
            if code:
                rows.append({'code': code, 'team_id': team_id})
        return rows

    def _delete_team_join_row(self, team_id, code):
        self.delete_rel('team:join', team_id=team_id, code=code)
        self.delete_rel('join:team', code=code, team_id=team_id)

    def _team_portfolio_or_error(self, team_id):
        portfolio_id = self._portfolio_id_for_team(team_id)
        if not portfolio_id:
            return None, {
                "success": False,
                "message": "Team was not found.",
                "status": 404,
            }
        return portfolio_id, None

    def get_team_join_code(self, team_id, portfolio_id, sender_id):
        context = self._invite_context(portfolio_id, team_id, sender_id)
        if not context.get('success'):
            return context
        rows = self._team_join_rows(team_id)
        if not rows:
            return {
                "success": True,
                "message": "This team has no open invitation.",
                "status": 200,
                "document": None,
            }
        return self._join_code_result(rows[0]['code'], team_id)

    def issue_team_join_code(self, team_id, portfolio_id, sender_id, replace=False):
        context = self._invite_context(portfolio_id, team_id, sender_id)
        if not context.get('success'):
            return context
        existing = self._team_join_rows(team_id)
        if existing and not replace:
            return self._join_code_result(existing[0]['code'], team_id)
        for row in existing:
            self._delete_team_join_row(team_id, row['code'])
        code = self._new_join_code()
        created = self.create_rel('team:join', team_id=team_id, code=code)
        if not created or not created.get('success'):
            return created
        created = self.create_rel('join:team', code=code, team_id=team_id)
        if not created or not created.get('success'):
            return created
        return self._join_code_result(code, team_id)

    def revoke_team_join_code(self, team_id, portfolio_id, sender_id):
        context = self._invite_context(portfolio_id, team_id, sender_id)
        if not context.get('success'):
            return context
        for row in self._team_join_rows(team_id):
            self._delete_team_join_row(team_id, row['code'])
        return {
            "success": True,
            "message": "Open invitation revoked.",
            "status": 200,
            "document": None,
        }

    def _access_user_ids(self, team_id):
        response = self.list_rel('team:access', team_id=team_id)
        items = ((response or {}).get('document') or {}).get('items') or []
        return [item.get('rel') for item in items if item.get('rel')]

    def redeem_join_code(self, user_id, token):
        code = self._normalize_join_code(token)
        if not code:
            return {"success": False, "message": self.JOIN_TOKEN_INVALID, "status": 404}
        response = self.list_rel('join:team', code=code)
        items = ((response or {}).get('document') or {}).get('items') or []
        team_id = items[0].get('rel') if items else ''
        if not team_id:
            return {"success": False, "message": self.JOIN_TOKEN_INVALID, "status": 404}
        names = self._invite_place_names(team_id)
        document = {
            "status": "access-requested",
            "team_id": team_id,
            "team_name": names['team_name'],
            "portfolio_name": names['portfolio_name'],
        }
        if self._caller_on_team(user_id, team_id):
            document['status'] = 'member'
            return {
                "success": True,
                "message": "You are already on this team.",
                "status": 200,
                "document": document,
            }
        if user_id not in self._access_user_ids(team_id):
            created = self.create_rel('team:access', team_id=team_id, user_id=user_id)
            if not created or not created.get('success'):
                return created
        return {
            "success": True,
            "message": "Access requested.",
            "status": 200,
            "document": document,
        }

    def approve_team_access(self, team_id, user_id, approver_id):
        portfolio_id, error = self._team_portfolio_or_error(team_id)
        if error:
            return error
        context = self._invite_context(portfolio_id, team_id, approver_id)
        if not context.get('success'):
            return context
        if user_id not in self._access_user_ids(team_id):
            return {
                "success": False,
                "message": "That access request is no longer waiting.",
                "status": 404,
            }
        if not self._caller_on_team(user_id, team_id):
            added = self.add_user_to_team_funnel(user_id=user_id, team_id=team_id)
            if not added.get('success'):
                return added
        self.delete_rel('team:access', team_id=team_id, user_id=user_id)
        return {
            "success": True,
            "message": "Access approved.",
            "status": 200,
        }

    def reject_team_access(self, team_id, user_id, approver_id):
        portfolio_id, error = self._team_portfolio_or_error(team_id)
        if error:
            return error
        context = self._invite_context(portfolio_id, team_id, approver_id)
        if not context.get('success'):
            return context
        if user_id not in self._access_user_ids(team_id):
            return {
                "success": False,
                "message": "That access request is no longer waiting.",
                "status": 404,
            }
        self.delete_rel('team:access', team_id=team_id, user_id=user_id)
        return {
            "success": True,
            "message": "Access rejected.",
            "status": 200,
        }

    def _live_email_invites(self, email):
        """Live invitations stored for this address. Membership is not implied."""
        email = self._normalize_invite_email(email)
        if not email:
            return []
        found = self.AUM.list_rel_prefix('irn:rel:email:hash:ttl:*:*:*', email + ':')
        rows = found.get('document') if found.get('success') else []
        invites = []
        for invitation in rows or []:
            parts = str(invitation.get('rel') or '').split(':')
            if len(parts) < 3:
                continue
            ttl = parts[-1]
            code = parts[-2]
            row_email = ':'.join(parts[:-2])
            if row_email != email or not self._invite_still_valid(ttl):
                continue
            if not self._invite_code_accepted(email, ttl, code):
                continue
            teams = self.list_rel('hash:team', hash=code)
            items = ((teams or {}).get('document') or {}).get('items') or []
            for item in items:
                team_id = item.get('rel')
                if team_id:
                    invites.append({
                        'email': email,
                        'code': code,
                        'ttl': ttl,
                        'team_id': team_id,
                        'expired': False,
                    })
        return invites

    def _invite_place_names(self, team_id):
        portfolios = self.list_rel('team:portfolio', team_id=team_id)
        items = ((portfolios or {}).get('document') or {}).get('items') or []
        portfolio_id = items[0].get('rel') if items else ''
        portfolio_name = ''
        team_name = ''
        if portfolio_id:
            portfolio = self.get_entity('portfolio', portfolio_id=portfolio_id)
            if portfolio.get('success'):
                portfolio_name = (portfolio.get('document') or {}).get('name') or ''
            team = self.get_entity('team', portfolio_id=portfolio_id, team_id=team_id)
            if team.get('success'):
                team_name = (team.get('document') or {}).get('name') or ''
        return {
            'team_name': team_name or 'a team',
            'portfolio_name': portfolio_name,
        }

    def list_pending_invites(self, email):
        """Invitations this signed-in address can accept. The code stays on the server."""
        public = []
        for row in self._live_email_invites(email):
            names = self._invite_place_names(row['team_id'])
            expires = row['ttl']
            public.append({
                'team_id': row['team_id'],
                'team_name': names['team_name'],
                'portfolio_name': names['portfolio_name'],
                'expires_at': int(expires) if str(expires).isdigit() else expires,
            })
        return {"success": True, "document": public, "status": 200}

    def _invite_for_team(self, email, team_id):
        matches = [
            row for row in self._live_email_invites(email)
            if row['team_id'] == team_id
        ]
        return matches[0] if matches else None

    def accept_team_invite(self, email, user_id, team_id):
        """Add this person only after they agree, and record who accepted when."""
        email = self._normalize_invite_email(email)
        team_id = str(team_id or '').strip()
        if not email or not user_id or not team_id:
            return {"success": False, "message": "Missing attributes", "status": 400}
        row = self._invite_for_team(email, team_id)
        if not row:
            return {
                "success": False,
                "message": "This invitation is no longer available.",
                "status": 404,
            }
        memberships = self.list_rel('user:team', user_id=user_id)
        already = any(
            item.get('rel') == team_id
            for item in (((memberships or {}).get('document') or {}).get('items') or [])
        )
        if not already:
            added = self.add_user_to_team_funnel(user_id=user_id, team_id=team_id)
            if not added.get('success'):
                return added
        recorded = self.create_rel(
            'team:accept',
            team_id=team_id,
            user_id=user_id,
            email=email,
            accepted_at=str(int(time.time())),
        )
        if not recorded.get('success'):
            self.logger.error('Acceptance was not recorded for %s on %s', user_id, team_id)
        self._delete_team_invite_row(team_id, row)
        refreshed = self.refresh_user_tree(user_id)
        if not refreshed.get('success'):
            self.logger.error('Could not refresh tree after accepting invite for %s', user_id)
        return {"success": True, "message": "You joined the team.", "status": 200}

    def decline_team_invite(self, email, team_id):
        email = self._normalize_invite_email(email)
        team_id = str(team_id or '').strip()
        if not email or not team_id:
            return {"success": False, "message": "Missing attributes", "status": 400}
        row = self._invite_for_team(email, team_id)
        if not row:
            return {
                "success": False,
                "message": "This invitation is no longer available.",
                "status": 404,
            }
        self._delete_team_invite_row(team_id, row)
        return {"success": True, "message": "Invitation declined.", "status": 200}




    def _choose_cognito_match(self, matches):
        """Pick the Cognito user whose Username hashes to this app's user id.

        Prefer a profile that already has a user entity. Otherwise prefer a
        password user over an external one, so a later sign-in lands on the
        same id.
        """
        ranked = []
        for match in matches or []:
            username = str(match.get('username') or '').strip()
            if not username:
                continue
            user_id = create_md5_hash(username, 9)
            has_entity = bool(self.get_entity('user', user_id=user_id).get('success'))
            native = str(match.get('status') or '') != 'EXTERNAL_PROVIDER'
            ranked.append((has_entity, native, user_id, match))
        if not ranked:
            return None
        ranked.sort(key=lambda item: (item[0], item[1]), reverse=True)
        return ranked[0][2], ranked[0][3]

    def _drop_phantom_sub_membership(self, *, team_id, username, sub, user_id):
        """Remove a team link that was stored under the Cognito sub hash.

        Password users are unaffected: their username and sub are the same
        string. A Google user id is the hash of Google_<subject>, so a link
        stored under the sub hash never resolves to a user entity.
        """
        username = str(username or '').strip()
        sub = str(sub or '').strip()
        if not team_id or not username or not sub or username == sub:
            return
        phantom_id = create_md5_hash(sub, 9)
        if not phantom_id or phantom_id == user_id:
            return
        if self.get_entity('user', user_id=phantom_id).get('success'):
            return
        for reltype, payload in (
            ('team:user', {'team_id': team_id, 'user_id': phantom_id}),
            ('user:team', {'user_id': phantom_id, 'team_id': team_id}),
        ):
            response = self.delete_rel(reltype, **payload)
            self.logger.debug('Dropped mismatched invite membership: %s', response)

    def get_user_id(self,email):
        # DO NOT EXPOSE THIS FUNCTION TO API
        response = self.AUM.check_user_by_email(email)
        self.logger.debug('Calling Identity Pool:'+str(response))

        if not response.get('success'):
            return response

        document = response.get('document') or {}
        matches = document.get('matches')
        if not matches and document.get('cognito_username'):
            matches = [{
                'username': document.get('cognito_username'),
                'sub': document.get('cognito_user_id'),
                'status': '',
            }]
        chosen = self._choose_cognito_match(matches)
        if not chosen:
            return {
                "success": False,
                "message": "User not found",
                "status": 404,
            }
        user_id, match = chosen
        document['user_id'] = user_id
        document['cognito_username'] = match.get('username')
        document['cognito_user_id'] = match.get('sub')
        response['document'] = document
        return response



    def user_portfolios(self,user_id):
        '''
        input:
        'user_id'
        '''
        user_portfolio_list = []
 
        index = 'irn:rel:user:team:' + user_id + ':*'
        rels_user_teams = self.AUM.list_rel(index)     
        for team in rels_user_teams['document']['items']:
            rel_team_id = team['rel']
 
            index = 'irn:rel:team:portfolio:' + rel_team_id  + ':*'
            rels_team_portfolio = self.AUM.list_rel(index)
            for portfolio in rels_team_portfolio['document']['items']:
                portfolio_id = portfolio['rel']
                if portfolio_id in user_portfolio_list:
                    continue
                if self._portfolio_is_deleted(portfolio_id):
                    continue
                user_portfolio_list.append(portfolio_id)

        return user_portfolio_list


    def is_user_team_org_same_portfolio(self,**kwargs):

        '''
        input:
        'user_id'
        'team_id'
        'org_id'
        '''
        
        user_portfolio_list = self.user_portfolios(kwargs['user_id'])

        for portfolio_id in user_portfolio_list:

            #Check if the team belongs to the portfolio
            response_1 = self.get_rel('team:portfolio',team_id=kwargs['team_id'],portfolio_id=portfolio_id)

            #Check if the org belongs to the portfolio
            response_2 = self.get_entity('org',portfolio_id=portfolio_id,org_id=kwargs['org_id'])

            if response_1['success'] and response_2['success'] :
                #user,team and org belong to portfolio
                return True
            
        return False
    

    def is_user_team_same_portfolio(self,**kwargs):

        '''
        input:
        'user_id'
        'team_id'
        '''
        
        user_portfolio_list = self.user_portfolios(kwargs['user_id'])

        for portfolio_id in user_portfolio_list:

            #Check if the team belongs to the portfolio
            response_1 = self.get_rel('team:portfolio',team_id=kwargs['team_id'],portfolio_id=portfolio_id)

            
            if response_1['success'] :
                #user and team belong to same portfolio
                return True
            
        return False
            


    def _deleted_org_ids(self, portfolio_id, cache=None):
        """Org ids marked deleted. Relationships to them are left in place."""
        cache = cache if cache is not None else {}
        if portfolio_id in cache:
            return cache[portfolio_id]
        index = 'irn:entity:portfolio/org:' + portfolio_id + '/*'
        items = ((self.AUM.list_entity(index) or {}).get('document') or {}).get('items') or []
        deleted = {
            item.get('_id')
            for item in items
            if item.get('_id') and entity_is_deleted(item)
        }
        cache[portfolio_id] = deleted
        return deleted

    def _portfolio_is_deleted(self, portfolio_id):
        if not portfolio_id:
            return False
        entity = self.get_entity(
            'portfolio',
            portfolio_id=portfolio_id,
            include_deleted=True,
        )
        return bool(entity.get('success') and entity_is_deleted(entity.get('document')))

    def _filter_deleted_entities(self, response):
        document = (response or {}).get('document')
        if not isinstance(document, dict) or not isinstance(document.get('items'), list):
            return response
        document['items'] = [
            item for item in document['items'] if not entity_is_deleted(item)
        ]
        return response

    def _project_installables_for_team(
        self, portfolio_id, team_id, kind, tree, org_grants, active_orgs,
        hidden_org_ids=None,
    ):
        """
        Project one installable type into its own tree bag.

        tool → portfolios[p].tools / teams[t].tools / orgs[o].tools
        extension → portfolios[p].extensions / teams[t].extensions / orgs[o].extensions
        """
        bag = "extensions" if kind == "extension" else "tools"
        index = self._installable_entity_index(kind, portfolio_id)
        items = ((self.AUM.list_entity(index) or {}).get("document") or {}).get("items") or []
        rels = self._installable_rel_types(kind)
        portfolio_node = tree["portfolios"][portfolio_id]
        portfolio_node.setdefault(bag, {})
        org_grants.setdefault(bag, {})

        for entity in items:
            entity_id = entity["_id"]
            node = {
                "name": entity.get("name"),
                "handle": entity.get("handle"),
                "roles": self._normalize_roles_list(entity.get("roles")),
                "entity_type": kind,
                "preferences": entity.get("preferences") or {},
            }
            if kind == "extension":
                node["extension_id"] = entity_id
            else:
                node["tool_id"] = entity_id
            existing = portfolio_node[bag].get(entity_id) or {}
            merged = {**existing, **node}
            if existing.get("active"):
                merged["active"] = True
            portfolio_node[bag][entity_id] = merged

            team_node = portfolio_node["teams"][team_id]
            team_node.setdefault(bag, {}).setdefault(entity_id, {})

            roles = self._rel_item_rels(
                f"irn:rel:{rels['role']}:{team_id}/{entity_id}:*"
            )
            prev_roles = team_node[bag][entity_id].get("roles") or []
            team_node[bag][entity_id]["roles"] = list(
                dict.fromkeys(prev_roles + roles)
            )

            granted_orgs = self._rel_item_rels(
                f"irn:rel:{rels['org']}:{team_id}/{entity_id}:*"
            )
            # Keep the rel rows. Omit deleted orgs from the tree so a grant
            # to a hidden org does not make that org or the grant look live.
            hidden_org_ids = hidden_org_ids or set()
            visible_orgs = [
                org_id for org_id in granted_orgs if org_id not in hidden_org_ids
            ]
            for org_id in visible_orgs:
                active_orgs.append(org_id)
                portfolio_node[bag][entity_id]["active"] = True
                org_grants[bag].setdefault(org_id, [])
                if entity_id not in org_grants[bag][org_id]:
                    org_grants[bag][org_id].append(entity_id)
            prev_orgs = team_node[bag][entity_id].get("orgs") or []
            team_node[bag][entity_id]["orgs"] = list(
                dict.fromkeys(prev_orgs + visible_orgs)
            )

    def get_tree_full(self,**kwargs):
        # Auth Tree after resolving each document ID 
        # Instead of creating a function to query each entity separately (many functions), 
        # We provide a single endpoint to get all the data the app needs to operate in one call. 
        tree = {}
        tree['user_id'] = kwargs['user_id']
        tree['portfolios'] = {}
        user_entity = self.get_entity('user', user_id=kwargs['user_id'])
        user_doc = user_entity.get('document') if user_entity.get('success') else {}
        tree['preferences'] = (user_doc or {}).get('preferences') or {}
        deleted_org_cache = {}

        self.logger.debug('GENERATING TREE')

        # 1. Get all the user:team rels. 
        index = 'irn:rel:user:team:' + kwargs['user_id'] + ':*'
        rels_user_teams = self.AUM.list_rel(index, limit=1000)
        #self.logger.debug('User Teams:'+str(rels_user_teams))

        # 2. For each rel, get its team:portfolio rel. Each rel will show a Portfolio_id
        # Check if rels_user_teams has the expected structure and is not empty
        if (rels_user_teams and 
            'document' in rels_user_teams and 
            'items' in rels_user_teams['document'] and 
            rels_user_teams['document']['items']):
            
            for team in rels_user_teams['document']['items']:
                
                team_id =  team['rel'] 
                self.logger.debug('FLAG0>>TEAM:'+team_id)     
                
                index = 'irn:rel:team:portfolio:' + team['rel'] + ':*'
                rels_team_portfolio = self.AUM.list_rel(index)
                #self.logger.debug('User Portfolios:'+str(rels_team_portfolio))

                # Check if rels_team_portfolio has the expected structure and is not empty
                if (rels_team_portfolio and 
                    'document' in rels_team_portfolio and 
                    'items' in rels_team_portfolio['document'] and 
                    rels_team_portfolio['document']['items']):

                    for portfolio in rels_team_portfolio['document']['items']:

                        portfolio_id = portfolio['rel']
                        self.logger.debug('FLAG1>>TEAM:'+team_id+'PORTFOLIO:'+portfolio_id)

                        if portfolio_id not in tree['portfolios']:

                            #RESOLVE: Get Portfolio entity document.
                            # A deleted portfolio keeps its team rels, but it
                            # is not part of the tree.
                            portfolio_entity = self.get_entity(
                                'portfolio',
                                portfolio_id=portfolio_id
                                )
                            if not portfolio_entity.get('success'):
                                continue

                            portfolio_doc = {}  
                            portfolio_doc['portfolio_id'] = portfolio_id      
                            portfolio_doc['name'] = portfolio_entity['document']['name']
                            portfolio_doc['preferences'] = portfolio_entity['document'].get('preferences') or {}
                            portfolio_doc['teams'] = {}
                            portfolio_doc['orgs'] = {}
                            portfolio_doc['tools'] = {}
                            portfolio_doc['extensions'] = {}

                            #self.logger.debug('Tree: '+str(tree))

                            #self.logger.debug('Inserting Portfolio '+portfolio_id+' in tree'+str(portfolio_doc))
                            tree['portfolios'][portfolio_id] = portfolio_doc

                            #self.logger.debug('Tree: '+str(tree))
                            

                        #Teams
                        #Include Team Document in the tree under the current portfolio

                        #RESOLVE: Get Team entity document
                        team_entity = self.get_entity(
                            'team',
                            portfolio_id=portfolio_id,
                            team_id=team_id
                            )
                        
                        if not team_entity['success']:
                            # Entity doesn't exist
                            # skip the team relationships
                            continue
                        
                        team_doc = {}
                        team_doc['team_id'] = team_id
                        team_doc['name'] = team_entity['document']['name']
                        team_doc['preferences'] = team_entity['document'].get('preferences') or {}
                        team_doc['tools'] = {}
                        team_doc['extensions'] = {}

                        #self.logger.debug('Inserting Team '+team_id+'in portfolio '+portfolio_id+':'+str(team_doc))
                        tree['portfolios'][portfolio_id]['teams'][team_id] = team_doc


                        tree['portfolios'][portfolio_id]['teams'][team_id]['tools_access'] = (
                            self._rel_item_rels('irn:rel:team:tool:' + team_id + ':*')
                        )
                        tree['portfolios'][portfolio_id]['teams'][team_id]['extensions_access'] = (
                            self._rel_item_rels('irn:rel:team:extension:' + team_id + ':*')
                        )

                        active_orgs = []
                        org_grants = {'tools': {}, 'extensions': {}}
                        hidden_org_ids = self._deleted_org_ids(
                            portfolio_id, deleted_org_cache
                        )
                        for kind in self.INSTALLABLE_TYPES:
                            self._project_installables_for_team(
                                portfolio_id,
                                team_id,
                                kind,
                                tree,
                                org_grants,
                                active_orgs,
                                hidden_org_ids=hidden_org_ids,
                            )

                        self.logger.debug('ORG_GRANTS:')
                        self.logger.debug(org_grants) 
                        
                          
                        #Orgs
                        index = 'irn:entity:portfolio/org:'+portfolio_id+'/*'
                        entities_orgs = self.AUM.list_entity(index)

                        # Check if entities_orgs has the expected structure and is not empty
                        if (entities_orgs and 
                            'document' in entities_orgs and 
                            'items' in entities_orgs['document'] and 
                            entities_orgs['document']['items']):

                            for org in entities_orgs['document']['items']:

                                if entity_is_deleted(org):
                                    continue

                                org_id = org['_id']
                                
                               
                                # Assemble the orgs object
                                if 'orgs' not in tree['portfolios'][portfolio_id]:
                                    tree['portfolios'][portfolio_id]['orgs'] = {}
                                    
                                    
                                if org_id not in tree['portfolios'][portfolio_id]['orgs']:
                                    tree['portfolios'][portfolio_id]['orgs'][org_id] = {}
                                    
                                
                                org_node = tree['portfolios'][portfolio_id]['orgs'][org_id]
                                org_node.setdefault('tools', [])
                                org_node.setdefault('extensions', [])
                                for bag in ('tools', 'extensions'):
                                    extra = (org_grants.get(bag) or {}).get(org_id) or []
                                    if extra:
                                        org_node[bag] = list(set(org_node[bag] + extra))
                                
                                tree['portfolios'][portfolio_id]['orgs'][org_id]['org_id'] = org_id
                                tree['portfolios'][portfolio_id]['orgs'][org_id]['name'] = org['name']
                                tree['portfolios'][portfolio_id]['orgs'][org_id]['handle'] = org['handle']
                                tree['portfolios'][portfolio_id]['orgs'][org_id]['tags'] = org.get('tags') or {}
                                tree['portfolios'][portfolio_id]['orgs'][org_id]['preferences'] = org.get('preferences') or {}
                                
                                
                                if org_id in active_orgs:
                                    tree['portfolios'][portfolio_id]['orgs'][org_id]['active'] = True     
                                #else:
                                #    tree['portfolios'][portfolio_id]['orgs'][org_id]['active'] = False 
                                

        self.normalize_auth_tree(tree)

        response = {}
        response['success'] = True
        response['document'] = tree
        response['status'] = 200

        return response



    def list_entity(self,type,**kwargs):

        portfolios = []

        if(type == 'portfolio'): 
            # You can't find the list of portfolios by simply querying the entity table 
            # The reason is that portfolios have no containers. So there is no common index 
            # The only things that groups them are its teams and users. 
            # In order to get a list of Portfolios for a user, you need to:
            # 1. Get all the user:team rels. 
            index = 'irn:rel:user:team:' + kwargs['user_id'] + ':*'
            rel_user_teams = self.AUM.list_rel(index)
            self.logger.debug('User Teams:'+str(rel_user_teams))

            # 2. For each rel, get its team:portfolio rel. Each rel will show a Portfolio_id
            for team in rel_user_teams['document']['items']:
                
                #self.logger.debug('User Teams:'+str(team))
                
                index = 'irn:rel:team:portfolio:' + team['rel'] + ':*'
                rel_team_portfolio = self.AUM.list_rel(index)
                #self.logger.debug('User Portfolios:'+str(rel_team_portfolio))
                items = ((rel_team_portfolio or {}).get('document') or {}).get('items') or []
                kept = []
                for portfolio in items:
                    portfolio_id = portfolio.get('rel')
                    if portfolio_id and self._portfolio_is_deleted(portfolio_id):
                        continue
                    kept.append(portfolio)
                if isinstance((rel_team_portfolio or {}).get('document'), dict):
                    rel_team_portfolio['document']['items'] = kept
                portfolios.append(rel_team_portfolio)

            
            response = {}
            response['success'] = True
            response['document'] = portfolios
            response['status'] = 200

            return response
        
        
        if(type =='org'):
            index = 'irn:entity:portfolio/org:'+kwargs['portfolio_id']+'/*'

        if(type =='team'):
            index = 'irn:entity:portfolio/team:'+kwargs['portfolio_id']+'/*'

        if(type =='app'):         
            index = 'irn:entity:team/app:'+kwargs['team_id']+'/*'
            
        if type in self.INSTALLABLE_TYPES:
            index = self._installable_entity_index(type, kwargs['portfolio_id'])

        response = self.AUM.list_entity(index)
        if type == 'org':
            return self._filter_deleted_entities(response)
        return response




    def get_entity(self,type,**kwargs):

        #if(type== 'user' and 'user_id' in kwargs and 'portfolio_id' in kwargs ):

        # Soft-deleted portfolios and orgs stay in the table but are not
        # readable by id. Callers that must see the row pass include_deleted.
        include_deleted = bool(kwargs.get('include_deleted'))
        missing = False

        if type == 'user':
            if all(key in kwargs for key in ['user_id']):
                index = 'irn:entity:user:*'
                id = kwargs['user_id']
            else:
                missing = True
        
        elif type == 'portfolio':
            if all(key in kwargs for key in ['portfolio_id']):
                index = 'irn:entity:portfolio:*'
                id = kwargs['portfolio_id']
            else:
                missing = True
            
        elif type == 'org':
            if all(key in kwargs for key in ['portfolio_id','org_id']):
                index = 'irn:entity:portfolio/org:'+kwargs['portfolio_id']+'/*'
                id = kwargs['org_id']
            else:
                missing = True

        elif type == 'team':
            if all(key in kwargs for key in ['portfolio_id','team_id']):
                index = 'irn:entity:portfolio/team:'+kwargs['portfolio_id']+'/*'
                id = kwargs['team_id']
            else:
                missing = True

        # The above code snippet is checking if the `type` is equal to 'tool' and if certain keys
        # ('portfolio_id' and 'tool_id') are present in the `kwargs` dictionary. If both conditions
        # are met, it sets the `index` variable to a specific value based on the 'portfolio_id' key,
        # and assigns the value of the 'tool_id' key to the `id` variable. If the conditions are not
        # met, it sets the `missing` variable to True.
        elif type in self.INSTALLABLE_TYPES:
            entity_id = self._installable_id(kwargs)
            if kwargs.get('portfolio_id') and entity_id:
                index = self._installable_entity_index(type, kwargs['portfolio_id'])
                id = entity_id
            else:
                missing = True
     
        if missing:
            return {'success':False,'message':'Some parameters are missing','status':500}

        response = self.AUM.get_entity(index,id) 

        result = {}

        if not response['success']:
            result['success'] = False
            result['message'] = response['message'] 
            result['status'] = response['status']               
            return result

        if (
            not include_deleted
            and type in ('portfolio', 'org')
            and entity_is_deleted(response.get('document'))
        ):
            return {
                'success': False,
                'message': 'Entity not found',
                'status': 404,
            }

        result['success'] = True
        result['message'] = response['message']
        result['document'] = response['document']  
        result['status'] = response['status']   
        #self.logger.debug('Returned object:'+str(result))
        return result

       

    def create_entity(self,type,**kwargs):
        # 'tool' is the retired installable type. New rows are extensions.
        if type == 'tool':
            type = 'extension'

        self.logger.debug('Data:')
        self.logger.debug(kwargs)

        #Creating new ID for entity
        raw_id = str(uuid.uuid4())
        new_id = create_md5_hash(raw_id,12)

        if(type == 'user'): 
            pk = 'irn:entity:user:*'
            sk = kwargs['user_id']
            irn = 'irn:entity:user:' + kwargs['user_id']
        
        if(type == 'portfolio'): 
            pk = 'irn:entity:portfolio:*'
            sk = new_id
            irn = 'irn:entity:portfolio:' + new_id
            
        elif(type =='org'):
            pk = 'irn:entity:portfolio/org:'+kwargs['portfolio_id']+'/*'
            sk = new_id
            irn = 'irn:entity:portfolio/org:'+kwargs['portfolio_id']+'/'+ new_id

        elif(type =='team'):
            pk = 'irn:entity:portfolio/team:'+kwargs['portfolio_id']+'/*'
            sk = new_id
            irn = 'irn:entity:portfolio/team:'+kwargs['portfolio_id']+'/'+ new_id

        elif type in self.INSTALLABLE_TYPES:
            pk = self._installable_entity_index(type, kwargs['portfolio_id'])
            sk = new_id
            irn = 'irn:entity:portfolio/'+type+':'+kwargs['portfolio_id']+'/'+ new_id
 
        '''elif(type =='action'):         
            pk = 'irn:entity:tool/action:'+kwargs['tool_id']+'/*'
            sk = kwargs['action_id']
            irn = 'irn:entity:tool/action:'+kwargs['tool_id']+'/'+ kwargs['action_id']'''
        

        data = {      
            '_id': sk,
            'name': kwargs['name'] if 'name' in kwargs else '',
            'about': kwargs['about'] if 'about' in kwargs else '',
            'handle': kwargs['handle'] if 'handle' in kwargs else '',
            'location': kwargs['location'] if 'location' in kwargs else '',
            'email': kwargs['email'] if 'email' in kwargs else '',
            'slot_a': kwargs['slot_a'] if 'slot_a' in kwargs else '',
            'slot_b': kwargs['slot_b'] if 'slot_b' in kwargs else '',
            'slot_c': kwargs['slot_c'] if 'slot_c' in kwargs else '',
            'slot_d': kwargs['slot_d'] if 'slot_d' in kwargs else '',
            'slot_e': kwargs['slot_e'] if 'slot_e' in kwargs else '',
            'owner_id':kwargs['user_id'] if 'user_id' in kwargs else '',
            'added': datetime.now().isoformat(),
            'is_active': True,
            'type': type,
            'status': ENTITY_STATUS_ACTIVE,
            'last_ip': kwargs['ip'] if 'ip' in kwargs else '',
            'last_login': (
                True
                if isinstance(kwargs.get('payload'), dict) and kwargs['payload'].get('last_login') is True
                else datetime.now().isoformat()
            ),
            'modified': datetime.now().isoformat(), 
            'raw_id': raw_id,
            'index':pk,
            'irn':irn,
            'language':kwargs['lan'] if 'lan' in kwargs else '',
            'tags': kwargs.get('tags') if isinstance(kwargs.get('tags'), dict) else {},
            'preferences': sanitize_entity_preferences(kwargs.get('preferences')),
        }

        if type in self.INSTALLABLE_TYPES:
            data['roles'] = self._normalize_roles_list(kwargs.get('roles'))

    
        self.logger.debug('Entity to Create:'+str(data))
        response = self.AUM.create_entity(data)
        if response.get('success'):
            synced = self.trees.note_entity(type, response.get('document') or data, kwargs)
            if not synced.get('success'):
                return synced

        return response
    



    def unlink_entity(self,**doc):
        '''
        Document is not deleted permanently but marked as unlinked
        Garbage collector should use the "modified" attribute to 
        determine if it is time to delete it permanently
        '''

        self.logger.debug('Document to be unlinked:')
        self.logger.debug(doc)

        if entity_forbids_hard_delete(doc):
            return {
                "success": False,
                "message": HARD_DELETE_REFUSED,
                "status": 400,
            }
 
        # Check if the second position exists and replace "entity" with "unentity"
        parts = doc['index'].split(":")
        if len(parts) > 1 and parts[1] == "entity":
            parts[1] = "unentity"
        
        # Join the parts back together with ":"
        undoc = copy.deepcopy(doc)
        undoc['index'] =  ":".join(parts)
        undoc['modified'] = datetime.now().isoformat()
        
        # Create unlinked entity document
        self.logger.debug('Entity to Create:'+str(undoc))
        response_1 = self.AUM.create_entity(undoc)

        
        if not response_1['success']:
            return{
            "success":False, 
            "message": "Could not create unlinked entity", 
            "status" :400
            }
        
        # Delete the original entity documents
        # We are not just changing the index in the document as Dynamo doesn't allow
        # mutation of Primary Keys.
        self.logger.debug('Entity to Delete:'+str(doc))
        response_2 = self.AUM.delete_entity(**doc)


        if not response_2['success']:
            return{
            "success":False, 
            "message": "Could not delete entity", 
            "status" :400
            }
        
        return response_2





    def update_entity(self,type,**kwargs):
        #data = request.json 
           
        result = {}   
        
        #1. Check if the document exists
        response_1 = self.get_entity(type,**kwargs)
        self.logger.debug('Check if doc exists:'+str(type)+':'+str(kwargs))
        self.logger.debug(response_1)

        if response_1['status'] != 200:          

            if response_1['status'] == 404:    
                # It didn't find the user document, create it

                if type == 'user':
                    return self.create_user_funnel(**kwargs)
                else:
                    return{
                        "success":False, 
                        "message": "Nothing to update.", 
                        "status" :400
                    }

                
            else:
                #Something else is wrong, surface the problem. 
                self.logger.debug("Entity does not exist")   
                return response_1
            
        
            
        self.logger.debug("Document found:"+str(response_1['document']))

        #Get the existing document from DB
        entity_doc = response_1['document']

        if type == 'user':
            for field, key in (('name', 'name'), ('slot_a', 'slot_a'), ('email', 'email')):
                incoming = (kwargs.get(key) or '').strip()
                if incoming and not (entity_doc.get(field) or '').strip():
                    entity_doc[field] = incoming

        #Replace values of existing attributes with the ones in the payload.
        # status is only changed by the portfolio and org soft-delete funnels.
        for key, val in kwargs['payload'].items():
            if key == 'status':
                continue
            if key == 'tags':
                entity_doc[key] = sanitize_entity_tags(val)
            elif key == 'preferences':
                entity_doc[key] = sanitize_entity_preferences(val)
            else:
                entity_doc[key] = val

        #Save the document back to DB
        response = self.AUM.update_entity(entity_doc)

        if not response['success']:
            result['success'] = False
            result['message'] = response['message'] 
            result['status'] = response['status']               
            return result
        

        result['success'] = True
        result['message'] = response['message']
        result['document'] = response['document']  
        result['status'] = response['status']
        synced = self.trees.note_entity(type, result.get('document'), kwargs)
        if not synced.get('success'):
            return synced
        return result


    def update_user_profile(self, *, user_id, cognito_username, first_name, last_name):
        """Update first/last name in DynamoDB user entity and Cognito."""
        first = (first_name or '').strip()
        last = (last_name or '').strip()
        if not first or not last:
            return {
                'success': False,
                'message': 'First and last name are required',
                'status': 400,
            }

        response = self.get_entity('user', user_id=user_id)
        if not response.get('success'):
            return response

        entity_doc = response['document']
        entity_doc['name'] = first
        entity_doc['slot_a'] = last
        entity_doc['modified'] = datetime.now().isoformat()

        db_response = self.AUM.update_entity(entity_doc)
        if not db_response.get('success'):
            return db_response

        cognito_response = self.AUM.cognito_update_user_attributes(
            cognito_username,
            given_name=first,
            family_name=last,
        )
        if not cognito_response.get('success'):
            cognito_response['message'] = (
                f"Saved profile in database but Cognito update failed: "
                f"{cognito_response.get('message', 'unknown error')}"
            )
            return cognito_response

        return {
            'success': True,
            'message': 'Profile updated',
            'document': db_response.get('document'),
            'status': 200,
        }



        #MOCK RESULT. Didn't really update the entity
        result['success'] = True
        result['message'] = 'OK'
        result['document'] = {'message':'Ok'}
        result['status'] = 200
        self.logger.debug('Returned object:'+str(result))

        return result




    def create_rel(self,reltype,**data): 
   
        if reltype == 'team:portfolio': #One to Many
            index = 'irn:rel:team:portfolio:' + data['team_id'] + ':*'
            rel = data['portfolio_id']
            
        elif reltype == 'team:user': #One to Many
            index = 'irn:rel:team:user:' + data['team_id'] + ':*'
            rel = data['user_id']

        elif reltype == 'user:team': #One to Many
            index = 'irn:rel:user:team:' + data['user_id'] + ':*'
            rel = data['team_id']

        #elif reltype == 'team/tool:action': #One to Many
        #    index = 'irn:rel:team/tool:action:' + data['team_id'] + '/' + data['tool_id'] + ':*'
        #    rel = data['action_id']
        
        elif reltype == 'team:tool': #One to Many
            index = 'irn:rel:team:tool:' + data['team_id'] + ':*'
            rel = data['tool_id']
                
        elif reltype == 'team/tool:role': #One to Many
            index = 'irn:rel:team/tool:role:' + data['team_id'] + '/' + data['tool_id'] + ':*'
            rel = data['role_id']
             
        elif reltype == 'team/tool:org': #One to Many
            index = 'irn:rel:team/tool:org:' + data['team_id'] + '/' + data['tool_id'] + ':*'
            rel = data['org_id']

        elif reltype == 'team:extension':
            index = 'irn:rel:team:extension:' + data['team_id'] + ':*'
            rel = self._installable_id(data)

        elif reltype == 'team/extension:role':
            index = (
                'irn:rel:team/extension:role:'
                + data['team_id']
                + '/'
                + self._installable_id(data)
                + ':*'
            )
            rel = data['role_id']

        elif reltype == 'team/extension:org':
            index = (
                'irn:rel:team/extension:org:'
                + data['team_id']
                + '/'
                + self._installable_id(data)
                + ':*'
            )
            rel = data['org_id']

        elif reltype == 'team:org': #One to Many
            index = 'irn:rel:team:org:' + data['team_id'] + ':*'
            rel = data['org_id']

        elif reltype == 'email:hash:ttl': #One to Many
            index = 'irn:rel:email:hash:ttl:*:*:*'
            rel = data['email']+':'+data['hash']+':'+str(data['ttl'])

        elif reltype == 'hash:team': #One to Many
            index = 'irn:rel:hash:team:' + data['hash'] + ':*'
            rel = data['team_id']

        elif reltype == 'team:invite':
            index = 'irn:rel:team:invite:' + data['team_id'] + ':*'
            rel = data['email'] + '|' + data['hash'] + '|' + str(data['ttl'])

        elif reltype == 'team:accept':
            index = 'irn:rel:team:accept:' + data['team_id'] + ':*'
            rel = data['user_id'] + '|' + data['email'] + '|' + str(data['accepted_at'])

        elif reltype == 'team:join':
            index = 'irn:rel:team:join:' + data['team_id'] + ':*'
            rel = data['code']

        elif reltype == 'join:team':
            index = 'irn:rel:join:team:' + data['code'] + ':*'
            rel = data['team_id']

        elif reltype == 'team:access':
            index = 'irn:rel:team:access:' + data['team_id'] + ':*'
            rel = data['user_id']


        rel_document = {
            'index' : index,
            'rel' : rel
        }
            
        logged = dict(rel_document)
        if reltype in ('email:hash:ttl', 'hash:team', 'team:invite', 'team:join', 'join:team'):
            logged['rel'] = '[invite]'
        self.logger.debug('Rel to Create:'+str(logged))
        response = self.AUM.create_rel(**rel_document)
        if not response or not response.get('success'):
            return response
        synced = self.trees.note_rel(reltype, data)
        if not synced.get('success'):
            return synced
        return response
    


    def delete_rel(self,reltype,**data): 
   
        if reltype == 'team:portfolio': #One to Many
            index = 'irn:rel:team:portfolio:' + data['team_id'] + ':*'
            rel = data['portfolio_id']
            
        elif reltype == 'team:user': #One to Many
            index = 'irn:rel:team:user:' + data['team_id'] + ':*'
            rel = data['user_id']

        elif reltype == 'user:team': #One to Many
            index = 'irn:rel:user:team:' + data['user_id'] + ':*'
            rel = data['team_id']

        #elif reltype == 'team/tool:action': #One to Many
         #   index = 'irn:rel:team/tool:action:' + data['team_id'] + '/' + data['tool_id'] + ':*'
          #  rel = data['action_id']
            
        elif reltype == 'team:tool': #One to Many
            index = 'irn:rel:team:tool:' + data['team_id'] + ':*'
            rel = data['tool_id']
        
        elif reltype == 'team/tool:role': #One to Many
            index = 'irn:rel:team/tool:role:' + data['team_id'] + '/' + data['tool_id'] + ':*'
            rel = data['role_id']
            
        elif reltype == 'team/tool:org': #One to Many
            index = 'irn:rel:team/tool:org:' + data['team_id'] + '/' + data['tool_id'] + ':*'
            rel = data['org_id']

        elif reltype == 'team:extension':
            index = 'irn:rel:team:extension:' + data['team_id'] + ':*'
            rel = self._installable_id(data)

        elif reltype == 'team/extension:role':
            index = (
                'irn:rel:team/extension:role:'
                + data['team_id']
                + '/'
                + self._installable_id(data)
                + ':*'
            )
            rel = data['role_id']

        elif reltype == 'team/extension:org':
            index = (
                'irn:rel:team/extension:org:'
                + data['team_id']
                + '/'
                + self._installable_id(data)
                + ':*'
            )
            rel = data['org_id']

        elif reltype == 'team:org': #One to Many
            index = 'irn:rel:team:org:' + data['team_id'] + ':*'
            rel = data['org_id']
        
        elif reltype == 'email:hash:ttl': #One to Many
            index = 'irn:rel:email:hash:ttl:*:*:*'
            rel = data['email']+':'+data['hash']+':'+str(data['ttl'])

        elif reltype == 'hash:team': #One to Many
            index = 'irn:rel:hash:team:' + data['hash'] + ':*'
            rel = data.get('team_id') or data.get('team')

        elif reltype == 'team:invite':
            index = 'irn:rel:team:invite:' + data['team_id'] + ':*'
            rel = data['email'] + '|' + data['hash'] + '|' + str(data['ttl'])

        elif reltype == 'team:accept':
            index = 'irn:rel:team:accept:' + data['team_id'] + ':*'
            rel = data['user_id'] + '|' + data['email'] + '|' + str(data['accepted_at'])

        elif reltype == 'team:join':
            index = 'irn:rel:team:join:' + data['team_id'] + ':*'
            rel = data['code']

        elif reltype == 'join:team':
            index = 'irn:rel:join:team:' + data['code'] + ':*'
            rel = data['team_id']

        elif reltype == 'team:access':
            index = 'irn:rel:team:access:' + data['team_id'] + ':*'
            rel = data['user_id']


        rel_document = {
            'index' : index,
            'rel' : rel
        }
            
        logged = dict(rel_document)
        if reltype in ('email:hash:ttl', 'hash:team', 'team:invite', 'team:join', 'join:team'):
            logged['rel'] = '[invite]'
        self.logger.debug('Rel to Delete:'+str(logged))
        response = self.AUM.delete_rel(**rel_document)
        if not response or not response.get('success'):
            return response
        synced = self.trees.note_rel(reltype, data, removing=True)
        if not synced.get('success'):
            return synced
        return response
    


    def get_rel(self,reltype,**data): 
   
        

        if reltype == 'team:portfolio': #One to One
            index = 'irn:rel:team:portfolio:' + data['team_id'] + ':*'
            rel = data['portfolio_id']
            
        elif reltype == 'team:user': #One to Many
            index = 'irn:rel:team:user:' + data['team_id'] + ':*'
            rel = data['user_id']

        elif reltype == 'user:team': #One to Many
            index = 'irn:rel:user:team:' + data['user_id'] + ':*'
            rel = data['team_id']

        #elif reltype == 'team/tool:action': #One to Many
        #    index = 'irn:rel:team/tool:action:' + data['team_id'] + '/' + data['tool_id'] + ':*'
        #    rel = data['action_id']
            
        elif reltype == 'team:tool': #One to Many
            index = 'irn:rel:team:tool:' + data['team_id'] + ':*'
            rel = data['tool_id']
        
        elif reltype == 'team/tool:role': #One to Many
            index = 'irn:rel:team/tool:role:' + data['team_id'] + '/' + data['tool_id'] + ':*'
            rel = data['role_id']
            
        elif reltype == 'team/tool:org': #One to Many
            index = 'irn:rel:team/tool:org:' + data['team_id'] + '/' + data['tool_id'] + ':*'
            rel = data['org_id']

        elif reltype == 'team:extension':
            index = 'irn:rel:team:extension:' + data['team_id'] + ':*'
            rel = self._installable_id(data)

        elif reltype == 'team/extension:role':
            index = (
                'irn:rel:team/extension:role:'
                + data['team_id']
                + '/'
                + self._installable_id(data)
                + ':*'
            )
            rel = data['role_id']

        elif reltype == 'team/extension:org':
            index = (
                'irn:rel:team/extension:org:'
                + data['team_id']
                + '/'
                + self._installable_id(data)
                + ':*'
            )
            rel = data['org_id']

        elif reltype == 'team:org': #One to Many
            index = 'irn:rel:team:org:' + data['team_id'] + ':*'
            rel = data['org_id']

        elif reltype == 'email:hash:ttl': #One to Many
            index = 'irn:rel:email:hash:ttl:*:*:*'
            rel = data['email']+':'+data['hash']+':'+str(data['ttl'])

        elif reltype == 'hash:team': #One to Many
            index = 'irn:rel:hash:team:' + data['hash'] + ':*'
            rel = data['team_id']

            
        self.logger.debug('Rel to Get > '+index+rel)
        response = self.AUM.get_rel(index,rel)

        return response
    


    def list_rel(self,reltype,**data): 
             

        if reltype == 'team:portfolio': #One to One
            index = 'irn:rel:team:portfolio:' + data['team_id'] + ':*'
            
            
        elif reltype == 'team:user': #One to Many
            index = 'irn:rel:team:user:' + data['team_id'] + ':*'
            

        elif reltype == 'user:team': #One to Many
            index = 'irn:rel:user:team:' + data['user_id'] + ':*'
            

        #elif reltype == 'team/tool:action': #One to Many
         #   index = 'irn:rel:team/tool:action:' + data['team_id'] + '/' + data['tool_id'] + ':*'
            
        elif reltype == 'team:tool': #One to Many
            index = 'irn:rel:team:tool:' + data['team_id'] + ':*'
            
        elif reltype == 'team/tool:role': #One to Many
            index = 'irn:rel:team/tool:role:' + data['team_id'] + '/' + data['tool_id'] + ':*'
            
        elif reltype == 'team/tool:org': #One to Many
            index = 'irn:rel:team/tool:org:' + data['team_id'] + '/' + data['tool_id'] + ':*'

        elif reltype == 'team:extension':
            index = 'irn:rel:team:extension:' + data['team_id'] + ':*'

        elif reltype == 'team/extension:role':
            index = (
                'irn:rel:team/extension:role:'
                + data['team_id']
                + '/'
                + self._installable_id(data)
                + ':*'
            )

        elif reltype == 'team/extension:org':
            index = (
                'irn:rel:team/extension:org:'
                + data['team_id']
                + '/'
                + self._installable_id(data)
                + ':*'
            )

        elif reltype == 'team:org': #One to Many
            index = 'irn:rel:team:org:' + data['team_id'] + ':*'

        elif reltype == 'email:hash:ttl': #One to Many
            index = 'irn:rel:email:hash:ttl:*:*:*'

        elif reltype == 'hash:team': #One to Many
            index = 'irn:rel:hash:team:' + data['hash'] + ':*'

        elif reltype == 'team:invite':
            index = 'irn:rel:team:invite:' + data['team_id'] + ':*'

        elif reltype == 'team:accept':
            index = 'irn:rel:team:accept:' + data['team_id'] + ':*'

        elif reltype == 'team:join':
            index = 'irn:rel:team:join:' + data['team_id'] + ':*'

        elif reltype == 'join:team':
            index = 'irn:rel:join:team:' + data['code'] + ':*'

        elif reltype == 'team:access':
            index = 'irn:rel:team:access:' + data['team_id'] + ':*'


        self.logger.debug('List Rels > '+index)
        response = self.AUM.list_rel(index)

        return response


    #--------------------------------------------------SPECIALIZED FUNCTIONS


    def get_team_users(self,**kwargs):

        userdict = {}

        #1. Retrieve rel >  team:user:
        if 'team_id' in kwargs:
            index = 'irn:rel:team:user:' + kwargs['team_id'] + ':*'
            response_1 = self.AUM.list_rel(index)
            
            #3. Call the entity document of each user to retrieve the email 
            # and name from team members. If there are more than 10 users, 
            # just output the first ten.
            self.logger.debug(f'List of team users:{response_1["document"]["items"]}',)

            for item in response_1['document']['items']:

                type = 'user' 
                response_2 = self.get_entity(type,user_id=item['rel'])
                
                if not response_2['success']:
                    self.logger.debug(f'Could not find entity of user:{item["rel"]}')
                    continue

                team_user_doc = response_2['document']
                self.logger.debug('Team User Document:'+str(team_user_doc))

                userdict[team_user_doc['_id']] = {}
                userdict[team_user_doc['_id']]['user_id'] = team_user_doc['_id']
                userdict[team_user_doc['_id']]['name'] = team_user_doc['name']
                userdict[team_user_doc['_id']]['last'] = team_user_doc['slot_a']
                userdict[team_user_doc['_id']]['email'] = team_user_doc['email']
                userdict[team_user_doc['_id']]['membership'] = (
                    'active' if self._user_has_opened_app(team_user_doc) else 'invited'
                )


            result = {}
            #2. Check whether the requesting user is member of the team.
            if kwargs['user_id'] not in userdict:
                result['success'] = False
                result['message'] = 'User is not in this team'
                result['status'] = 400              
                return result
            

            self._attach_pending_invites(userdict, kwargs['team_id'])
            self._attach_access_requests(userdict, kwargs['team_id'])

            result['success'] = True
            result['message'] = 'Users in the team'
            result['document'] = userdict 
            result['status'] = 200

            return result

    def _attach_pending_invites(self, userdict, team_id):
        active_emails = {
            str(row.get('email') or '').strip().lower()
            for row in userdict.values()
            if row.get('membership') == 'active'
        }
        for invite in self._team_invite_rows(team_id):
            email = invite['email']
            if email in active_emails:
                continue
            public = self._invite_public_document(email, invite)
            match = None
            for row in userdict.values():
                if str(row.get('email') or '').strip().lower() == email:
                    match = row
                    break
            if match is not None:
                match['membership'] = 'invited'
                match['expires_at'] = public['expires_at']
                match['expired'] = public['expired']
                if public.get('link'):
                    match['link'] = public['link']
                continue
            userdict['invite:' + email] = {
                'user_id': 'invite:' + email,
                'name': '',
                'last': '',
                'email': email,
                'membership': 'invited',
                'expires_at': public['expires_at'],
                'expired': public['expired'],
            }
            if public.get('link'):
                userdict['invite:' + email]['link'] = public['link']

    def _attach_access_requests(self, userdict, team_id):
        member_ids = {
            str(row.get('user_id') or '')
            for row in userdict.values()
            if row.get('membership') == 'active'
        }
        for user_id in self._access_user_ids(team_id):
            if user_id in member_ids:
                continue
            user = self.get_entity('user', user_id=user_id)
            document = user.get('document') if user.get('success') else {}
            userdict['access:' + user_id] = {
                'user_id': user_id,
                'name': (document or {}).get('name') or '',
                'last': (document or {}).get('slot_a') or '',
                'email': (document or {}).get('email') or '',
                'membership': 'access-requested',
            }

    def assign_team_orgs(self,**kwargs):

        result = {}
        #Check minimum requirements:
        required_keys = ['user_id','team_id','org_id'] 
        if not all(key in kwargs for key in required_keys):
            result['success'] = False
            result['message'] = 'Missing attributes' 
            result['status'] = 400             
            return result

        #1. Check if user_id making the assignment, the team_id and org_id belong to the same portfolio 
        # Only users that belong to the portfolio should be able to assign orgs to teams. 
        # Only teams and orgs under the same portfolio should be linked
        
        check = self.is_user_team_org_same_portfolio(
            user_id=kwargs['user_id'],
            team_id=kwargs['team_id'],
            org_id=kwargs['org_id']
            )
        
        if not check:
            result['success'] = False
            result['message'] = 'Org does not belong to portfolio' 
            result['status'] = 400             
            return result

        # Checking if user_id belongs to a team that is allowed to access the 
        # route that runs this function is responsibility of auth_check 
        
        
        #2. Create rel
        reltype = 'team:org'
        if kwargs['method'] == 'POST':
            response = self.create_rel(
                reltype,
                team_id=kwargs['team_id'],
                org_id=kwargs['org_id']
                )
        elif kwargs['method'] == 'DELETE':
            response = self.delete_rel(
                reltype,
                team_id=kwargs['team_id'],
                org_id=kwargs['org_id']
                )
            
        return response

        

    def generate_handle(self,name):
        # Use regex to find only capital letters (A-Z) or numbers (0-9)
        handle = re.sub(r'[^A-Z0-9]', '', name)
        
        # Return the first 10 characters
        return handle[:10]



    def generate_numeric_hash(self,input_string,length=20):
        # Create a SHA-256 hash object
        sha256_hash = hashlib.sha256()

        # Update the hash object with the input string encoded as bytes
        sha256_hash.update(input_string.encode('utf-8'))

        # Get the hexadecimal digest of the hash
        hex_hash = sha256_hash.hexdigest()

        # Convert the hexadecimal hash to an integer
        numeric_hash = int(hex_hash, 16)

        # Optionally, truncate the number to a specific length
        numeric_hash_str = str(numeric_hash)[:length]  # Adjust the length as needed

        return numeric_hash_str


    @staticmethod
    def _inviter_display_name(senderdoc: dict) -> str:
        """Human-readable inviter for team-invite email."""
        name = (senderdoc.get('name') or '').strip()
        last = (senderdoc.get('slot_a') or '').strip()
        full = f'{name} {last}'.strip()
        if full:
            return full
        email = (senderdoc.get('email') or '').strip()
        if email:
            return email
        handle = (senderdoc.get('handle') or '').strip()
        if handle:
            return handle
        return 'A team member'

    
    def generate_invite_hash(self,email,ttl):
        secret_key = str(self.config.get('SECRET_KEY') or '')
        string_to_hash = email + secret_key + str(ttl)
        return self.generate_numeric_hash(string_to_hash,6)
    


    # Function to generate TTL timestamp 24 hours from now
    def generate_ttl(self,offset_min=0):
        current_time = int(time.time())  # Current time in UNIX timestamp (seconds)
        ttl = current_time + offset_min * 60  # TTL (in seconds)
        return ttl




        
        


    #--------------------------------------------------ENTITY CREATION FUNNELS


    def create_user_funnel(self,**kwargs):
        result = {}
        transaction = []

        self.logger.debug('Initiating CREATE USER FUNNEL')

        #1. Create User Document
        response_1 = self.create_entity('user',**kwargs)
        self.logger.debug('Step 1: Create User Document')
        if not response_1['success']: 
                               
            return response_1
        else:
            transaction.append(response_1)

        
 
        #All went good, Summarize Transaction Success             
        result['success'] = True
        result['message'] = 'Create User Funnel completed'
        result['status'] = 200 
        result['document'] = transaction  

        self.logger.debug(result)
        return result
    




    @coalesced_trees
    def create_portfolio_funnel(self,**kwargs):
        result = {}
        transaction = []

        self.logger.debug('Initiating CREATE PORTFOLIO FUNNEL')


        #1. Create Porfolio Document
        #Input: kwargs['name'] and kwargs['about'] 
        response_1 = self.create_entity('portfolio',**kwargs)
        
        self.logger.debug('Step 1: Creating Porfolio Document')
        self.logger.debug(response_1)
        
  
        if not response_1['success']:
            response_1['message'] = 'Could not create the Portfolio Entity'                
            return response_1
        else:
            transaction.append(response_1)
        

        
        self.logger.debug('Step 2: SKIPPED')

        

        #3. Create a default Team named for this portfolio, so it cannot be
        # confused with an Admin team that already exists somewhere else.
        portfolio_name = str(kwargs.get('name') or '').strip()
        kwargs['name'] = f'{portfolio_name} Admin' if portfolio_name else 'Admin'
        kwargs['about'] = 'This team enables its users to change portfolio settings.'
        kwargs['portfolio_id'] = response_1['document']['_id'] #This is the portfolio_id
        kwargs.pop('preferences', None)
        response_3 = self.create_entity('team',**kwargs)

        self.logger.debug('Step 3: Creating a default Team')
        self.logger.debug(response_3)
        # Team documents explicitly relate to Portfolios. That's why you don't need a Portfolio:Teams rel.

        if not response_3['success']:
            response_3['message'] = 'Could not create Team'                
            return response_3 
        else:
            transaction.append(response_3)


        #3b. Create a Team to Portfolio Rel
        rel_data = {}
        rel_data['portfolio_id'] = response_1['document']['_id'] #This is the portfolio_id
        rel_data['team_id'] = response_3['document']['_id'] #This is the team_id
        response_3b = self.create_rel('team:portfolio',**rel_data)

        self.logger.debug('Step 3b:Creating Team-Portfolio relationship')
        self.logger.debug(response_3b)

        if not response_3b['success']:
            response_3b['message'] = 'Could not create Team-Portfolio relationship'                
            return response_3b
        else:
            transaction.append(response_3b)


          

        #4. Create Team to User Rel
        rel_data = {}
        rel_data['user_id'] = kwargs['user_id']
        rel_data['team_id'] = response_3['document']['_id'] #This is the team_id
        response_4 = self.create_rel('team:user',**rel_data)

        self.logger.debug('Step 4:Creating Team-User relationship')
        self.logger.debug(response_4)

        if not response_4['success']:
            response_4['message'] = 'Could not create Team-User relationship'                
            return response_4
        else:
            transaction.append(response_4)

        

        #5. Create User to Team Rel (Adding the owner as the first member of the team)
        rel_data = {}
        rel_data['user_id'] = kwargs['user_id']
        rel_data['team_id'] = response_3['document']['_id'] #This is the team_id
        response_5 = self.create_rel('user:team',**rel_data)

        self.logger.debug('Step 5: Creating User to Team relationship ')
        self.logger.debug(response_5)

        if not response_5['success']:
            response_5['message'] = 'Could not create User-Team relationship'                
            return response_5
        else:
            transaction.append(response_5)


        
        
        '''
        #5b. Create a Tool instance entity
        kwargs['name'] = 'Auth'
        kwargs['handle'] = '_auth'
        kwargs['about'] = 'This app allows you to create entities.'
        kwargs['portfolio_id'] = response_1['document']['_id'] #This is the portfolio_id
        response_5b = self.create_entity('tool',**kwargs)

        self.logger.debug('Step 5b: Installing default tool in portfolio')
        self.logger.debug(response_5b)
        # Team documents explicitly relate to Portfolios. That's why you don't need a Portfolio:Teams rel.

        if not response_5b['success']:
            response_5b['message'] = 'Could not install Tool'                
            return response_5b 
        else:
            transaction.append(response_5b)

        
        #CHANGE ACTIONS FOR ROLES.  ROLES ARE MORE GENERAL. 
        #ROLES WILL BE DETERMINED BY THE APP DESIGNER
        #THERE WILL BE NO CUSTOM ROLES
        #EVERY API ENDPOINT WILL AUTHORIZE BASED ON WHAT ACTIONS A ROLE IS ALLOWED TO EXECUTE
        #THERE IS A ROLE-ACTION OBJECT IN THE SOURCE CODE
        #IF A ROLE NEEDS TO ACQUIRE/LOSE ACTIONS, IT WILL BE DONE VIA CODE UPDATE
        #ROLES WILL BE WELL THOUGHT TO COVER ALL ACCESS PATTERNS TO AVOID HAVING TO CREATE CUSTOM ROLES
        #CUSTOM ROLES ARE AN ANTIPATTERN. SAME AS YOU DONT CREATE CUSTOM ROLES IN BASEBALL BUT USE WHAT EXISTS 
        #IN ORDER TO CREATE A NEW ROLE YOU NEED TO HAVE DEEP UNDERSTANDING OF WHAT THE TOOL DOES. 
        
        
        #- A portfolio has a tool (Defined as an ENTITY owned by the portfolio)
        #- A tool has x Roles (Roles are hardcoded to the Tool)
        #- A tool+role combination is assigned to a team (Defined as a REL that links the Team with a Tool )
        #- A role has a list of actions
        
        

        #6. Create Team/Tool to Role Rel
        rel_data = {}
        rel_data['team_id'] = response_3['document']['_id'] #This is the default team_id
        rel_data['tool_id'] = response_5b['document']['_id'] #This is the default tool_id
        rel_data['role_id'] = 'DataEntry'
        response_6 = self.create_rel('team/tool:role',**rel_data)

        self.logger.debug('Step 6:Create Team/Tool to Role : DataEntry')
        self.logger.debug(response_6)

        if not response_6['success']:
            response_6['message'] = 'Could not create Team/Tool-action relationship'                
            return response_6
        else:
            transaction.append(response_6)


        #6b. Create Team/Tool to Role Rel (2)
        rel_data = {}
        rel_data['team_id'] = response_3['document']['_id'] #This is the default team_id
        rel_data['tool_id'] = response_5b['document']['_id'] #This is the default tool_id
        rel_data['role_id'] = 'Moderator'
        response_6b = self.create_rel('team/tool:role',**rel_data)

        self.logger.debug('Step 6b:Create Team/Tool to Role : Moderator')
        self.logger.debug(response_6b)

        if not response_6b['success']:
            response_6b['message'] = 'Could not create Team/Tool-role relationship'                
            return response_6b
        else:
            transaction.append(response_6b)
            
            
            
            
            
            
        # We can't create the tool to Org relationship as no orgs exist yet. We'll do that in a later step.

        #7. Create a default Org
        kwargs['name'] = 'First Org'
        kwargs['handle'] = '1f'
        kwargs['about'] = 'This is your first org'
        kwargs['portfolio_id'] = response_1['document']['_id'] #This is the portfolio_id
        response_7 = self.create_entity('org',**kwargs)

        self.logger.debug('Step 7: Creating new org')
        self.logger.debug(response_7)
        # Org documents explicitly relate to Portfolios. That's why you don't need a Portfolio:Orgs rel.

        if not response_7['success']:
            response_7['message'] = 'Could not install App'                
            return response_7 
        else:
            transaction.append(response_7)


        #8. Create Team to Org rel
        rel_data = {}
        rel_data['team_id'] = response_3['document']['_id'] #This is the team_id
        rel_data['org_id'] = response_7['document']['_id'] #This is the org_id
        rel_data['action_id'] = 'useApp'
        response_8 = self.create_rel('team:org',**rel_data)

        self.logger.debug('Step 8:Create team to org rel')
        self.logger.debug(response_8)

        if not response_8['success']:
            response_8['message'] = 'Could not create Team-Org relationship'                
            return response_8
        else:
            transaction.append(response_8)
            
            
        
        #9. Create Team/Tool to Org Rel
        rel_data = {}
        rel_data['team_id'] = response_3['document']['_id'] #This is the default team_id
        rel_data['tool_id'] = response_5b['document']['_id'] #This is the default tool_id
        rel_data['org_id'] = response_7['document']['_id'] #This is the default org_id
        response_9 = self.create_rel('team/tool:org',**rel_data)

        self.logger.debug('Step 9:Create Team/Tool to Role : DataEntry')
        self.logger.debug(response_9)

        if not response_9['success']:
            response_9['message'] = 'Could not create Team/Tool-action relationship'                
            return response_9
        else:
            transaction.append(response_9)



        '''
        
        #All went good, Summarize Transaction Success 
        self.logger.debug('End of Funnel ')

        result['success'] = True
        result['message'] = 'Create Portfolio Funnel completed, Ok'
        result['status'] = 200 
        result['document'] = transaction  

        self.logger.debug(result)            
        return result
    

        



    
    @coalesced_trees
    def create_org_funnel(self,**kwargs):
        result = {}
        transaction = []

        self.logger.debug('Initiating CREATE ORG FUNNEL')


        #1. Create a new Org

        #Check minimum requirements:
        required_keys = ['name','portfolio_id'] 
        if not all(key in kwargs for key in required_keys):
            response_0 = {}
            response_0['success'] = False
            response_0['message'] = 'Missing attributes' 
            response_0['status'] = 400             
            return response_0

        kwargs['handle'] = self.generate_handle(kwargs['name'])
        response_1 = self.create_entity('org',**kwargs)

        self.logger.debug('Step 1: Creating new org')
        self.logger.debug(response_1)
        # Org documents explicitly relate to Portfolios. That's why we don't need a Portfolio:Orgs rel.

        if not response_1['success']:
            response_1['message'] = 'Could not create Org'                
            return response_1 
        else:
            transaction.append(response_1)




        #All went good, Summarize Transaction Success 
        self.logger.debug('End of Funnel ')

        result['success'] = True
        result['message'] = 'Create Org  Funnel completed, Ok'
        result['status'] = 200 
        result['document'] = transaction  

        self.logger.debug(result)            
        return result




    @coalesced_trees
    def create_team_funnel(self,**kwargs):
        result = {}
        transaction = []

        self.logger.debug('Initiating CREATE TEAM FUNNEL')

        #1. Create a new Team
        required_keys = ['name','portfolio_id'] 
        #Check minimum requirements:
        if not all(key in kwargs for key in required_keys):
            response_0 = {}
            response_0['success'] = False
            response_0['message'] = 'Missing attributes' 
            response_0['status'] = 400             
            return response_0
        
        response_1 = self.create_entity('team',**kwargs)
        self.logger.debug('Step 1: Creating a new Team')
        self.logger.debug(response_1)
        # Team documents explicitly relate to Portfolios. That's why you don't need a Portfolio:Teams rel.

        if not response_1['success']:
            response_1['message'] = 'Could not create Team'                
            return response_1 
        else:
            transaction.append(response_1)



        #2. Create a Team to Portfolio Rel (You need this rel to assemble the Auth tree)
        rel_data = {}
        rel_data['portfolio_id'] = kwargs['portfolio_id'] #This is the portfolio_id
        rel_data['team_id'] = response_1['document']['_id'] #This is the team_id
        response_2 = self.create_rel('team:portfolio',**rel_data)

        self.logger.debug('Step 2:Creating Team-Portfolio relationship')
        self.logger.debug(response_2)

        if not response_2['success']:
            response_2['message'] = 'Could not create Team-Portfolio relationship'                
            return response_2
        else:
            transaction.append(response_2)




        #3. Create Team to User Rel
        rel_data = {}
        rel_data['user_id'] = kwargs['user_id']
        rel_data['team_id'] = response_1['document']['_id'] #This is the team_id
        response_3 = self.create_rel('team:user',**rel_data)

        self.logger.debug('Step 3:Creating Team-User relationship')
        self.logger.debug(response_3)

        if not response_3['success']:
            response_3['message'] = 'Could not create Team-User relationship'                
            return response_3
        else:
            transaction.append(response_3)

        

        #4. Create User to Team Rel (Adding the owner as the first member of the team)
        rel_data = {}
        rel_data['user_id'] = kwargs['user_id']
        rel_data['team_id'] = response_1['document']['_id'] #This is the team_id
        response_4 = self.create_rel('user:team',**rel_data)

        self.logger.debug('Step 4: Creating User to Team relationship ')
        self.logger.debug(response_4)

        if not response_4['success']:
            response_4['message'] = 'Could not create User-Team relationship'                
            return response_4
        else:
            transaction.append(response_4)





        #All went good, Summarize Transaction Success 
        self.logger.debug('End of Funnel ')

        result['success'] = True
        result['message'] = 'Create Team Funnel completed, Ok'
        result['status'] = 200 
        result['document'] = transaction  

        self.logger.debug(result)            
        return result
    


    @coalesced_trees
    def add_user_to_team_funnel(self,**kwargs):
        result = {}
        transaction = []

        self.logger.debug('Initiating ADD USER TO TEAM FUNNEL')

        #0. Check for requirements
        required_keys = ['user_id','team_id'] 
        #Check minimum requirements:
        if not all(key in kwargs for key in required_keys):
            response_0 = {}
            response_0['success'] = False
            response_0['message'] = 'Missing attributes' 
            response_0['status'] = 400             
            return response_0
         

        #1. Create Team to User Rel
        rel_data = {}
        rel_data['user_id'] = kwargs['user_id']
        rel_data['team_id'] = kwargs['team_id'] #This is the team_id
        response_1 = self.create_rel('team:user',**rel_data)

        self.logger.debug('Step 1:Creating Team-User relationship')
        self.logger.debug(response_1)

        if not response_1['success']:
            response_1['message'] = 'Could not create Team-User relationship'                
            return response_1
        else:
            transaction.append(response_1)

        

        #2. Create User to Team Rel (Adding the owner as the first member of the team)
        rel_data = {}
        rel_data['user_id'] = kwargs['user_id']
        rel_data['team_id'] = kwargs['team_id'] #This is the team_id
        response_2 = self.create_rel('user:team',**rel_data)

        self.logger.debug('Step 2: Creating User to Team relationship ')
        self.logger.debug(response_2)

        if not response_2['success']:
            response_2['message'] = 'Could not create User-Team relationship'                
            return response_2
        else:
            transaction.append(response_2)



        #All went good, Summarize Transaction Success 
        self.logger.debug('End of Funnel ')

        result['success'] = True
        result['message'] = 'Add user to Team Funnel completed, Ok'
        result['status'] = 200 
        result['document'] = transaction  

        self.logger.debug(result)            
        return result
    


    def invite_user_funnel(self,**kwargs):
        bridge = {}
        result = {}
        transaction = []

        self.logger.debug('Initiating INVITE USER FUNNEL')


        #1. Check minimum requirements

        required_keys = ['email','team_id'] 
        if not all(key in kwargs for key in required_keys):
            response_0 = {}
            response_0['success'] = False
            response_0['message'] = 'Missing attributes' 
            response_0['status'] = 400             
            return response_0
        


        #1a. Get Team document
        response_1a = self.get_entity(
            'team',
            portfolio_id=kwargs['portfolio_id'],
            team_id=kwargs['team_id']
        )
        if not response_1a['success']:
            return response_1a
        else:
            bridge['teamdoc'] = response_1a['document']


        #1b. Get Sender document

        response_1b = self.get_entity(
            'user',
            user_id=kwargs['sender_id']
        )
        if not response_1b['success']:
            return response_1b
        else:
            bridge['senderdoc'] = response_1b['document']
            profile = kwargs.get('sender_profile') or {}
            for field, key in (('name', 'name'), ('slot_a', 'slot_a'), ('email', 'email')):
                if not (bridge['senderdoc'].get(field) or '').strip():
                    fallback = (profile.get(key) or '').strip()
                    if fallback:
                        bridge['senderdoc'][field] = fallback
        

        #1c. Get Portfolio document
        response_1c = self.get_entity(
            'portfolio',
            portfolio_id=kwargs['portfolio_id']
        )
        if not response_1c['success']:
            return response_1c
        else:
            bridge['portfoliodoc'] = response_1c['document']
        


        #2. Create Email to Hash rel 
        rel_data = {}
        rel_data['ttl'] = self.generate_ttl(offset_min=1440) 
        rel_data['email'] = kwargs['email']
        bridge['hash'] = rel_data['hash'] = self.generate_invite_hash(kwargs['email'],rel_data['ttl'])
        response_2 = self.create_rel('email:hash:ttl',**rel_data)

        self.logger.debug('Step 2: Creating Email to Hash to TTL relationship ')
        self.logger.debug(response_2)

        if not response_2['success']:
            response_2['message'] = 'Could not create Email to Hash to TTL relationship'                
            return response_2
        else:
            transaction.append(response_2)
            


        #3. Create Hash to Team rel 

        rel_data = {}
        rel_data['hash'] = bridge['hash']
        rel_data['team_id'] = kwargs['team_id']
        response_3 = self.create_rel('hash:team',**rel_data)

        self.logger.debug('Step 3: Creating Hash to Team relationship ')
        self.logger.debug(response_3)

        if not response_3['success']:
            response_3['message'] = 'Could not create Hash to Team relationship'                
            return response_3
        else:
            transaction.append(response_3)




        #4. Send email to invite recipient
        # Invite links open the console (/invite), not the API BASE_URL.
        from_email = (self.config.get('FROM_EMAIL') or '').strip()
        fe_base_url = resolve_invite_fe_base_url(self.config)
        invite_hash = bridge['hash']
        invite_link = f"{fe_base_url}/invite?code={invite_hash}&email={kwargs['email']}"

        if not from_email:
            return {
                "success": False,
                "message": (
                    "FROM_EMAIL is not configured. Set it in env_config / SSM "
                    "(application SES identity from customer-config email_from)."
                ),
                "status": 500,
            }
        if not fe_base_url:
            return {
                "success": False,
                "message": (
                    "Invite console URL is not configured. Set FE_BASE_URL (cloud Amplify URL "
                    "from SSM) or INVITE_FE_BASE_URL (local dev console, e.g. "
                    "http://127.0.0.1:5174)."
                ),
                "status": 500,
            }

        team_label = (
            bridge['portfoliodoc']['name'] + '/' + bridge['teamdoc']['name']
        )
        inviter = self._inviter_display_name(bridge['senderdoc'])
        invite_email = render_invite_email(
            inviter=inviter,
            team=team_label,
            code=invite_hash,
            link=invite_link,
        )
        response_4 = self.AUM.send_email(
            sender=from_email,
            recipient=kwargs['email'],
            subject=invite_email.subject,
            body_text=invite_email.body_text,
            body_html=invite_email.body_html,
            inline_images=invite_inline_images(invite_email),
        )

        if not response_4['success']:
            ses_msg = response_4.get('message') or 'Could not send the invite'
            self.logger.debug('Invite User Funnel > SES send failed: %s', response_4)
            response_4['message'] = f'Could not send the invite: {ses_msg}'
            return response_4

        response_4['message'] = (
            'Sent invite to team ' + kwargs['team_id'] + ' via email to ' + kwargs['email']
        )
        transaction.append(response_4)


        

        #All went good, Summarize Transaction Success 
        self.logger.debug('End of Funnel ')

        result['success'] = True
        result['message'] = 'Invite User Funnel completed, Ok'
        result['status'] = 200 
        result['document'] = transaction  

        self.logger.debug(result)            
        return result




    def invite_create_user_funnel(self,**kwargs):
        bridge = {}
        result = {}
        transaction = []
        self.logger.debug('Initiating CREATE INVITE USER FUNNEL')



        #1. Check minimum requirements
        required_keys = ['code','email','first','last','pass'] 
        if not all(key in kwargs for key in required_keys):
            return{
            "success":False, 
            "message": "Missing attributes", 
            "status" :400
            }
        

        #1b. Check if this user already exists in the pool. Cancel funnel if True
        response_1b = self.AUM.check_user_by_email(kwargs['email'])
        if response_1b['success']:
            return {
                "success":False, 
                "message": "User already exists, sign in to access",
                "status" : 404
            }


        #2. Verify that the invitation code is valid
        #Input: email, code
        index = 'irn:rel:email:hash:ttl:*:*:*'
        prefix = kwargs['email']+':'+kwargs['code']
        #We need to use a prefix search on the sort key as we don't have the TTL
        response_2 = self.AUM.list_rel_prefix(index,prefix) 

        self.logger.debug('Step 2: Verifying that the invitation is valid ')
        self.logger.debug(response_2)

        # Invitation was found
        if response_2['success'] and len(response_2['document'])>0 :
            email,hash,ttl = response_2['document'][0]['rel'].split(":")
        else:
            return{
                "success":False, 
                "message": "Invitation is invalid ", 
                "status" :404 #404:Not found
                } 

        # TTL has not expired
        if  str(self.generate_ttl(0)) > str(ttl):
            return{
                "success":False, 
                "message": "Invitation is expired", 
                "status" :410 #410:Gone
                }  
        
        # Provided code is valid
        if not self._invite_code_accepted(email, ttl, kwargs['code']):
             return{
                "success":False, 
                "message": "Invitation is invalid", 
                "status" :400 #400:Bad Request
                }
        transaction.append(response_2)

 
            

        #3. Create Cognito user with the invitee's chosen password as permanent
        #   (CONFIRMED — no NEW_PASSWORD_REQUIRED challenge on first login)
        response_3 = self.AUM.cognito_user_create_with_permanent_password(
            kwargs['email'],
            kwargs['pass'],
            kwargs['first'],
            kwargs['last'],
        )
        self.logger.debug('Step 3: Creating Cognito user with permanent password')
        self.logger.debug(response_3)
        if not response_3['success']:
            response_3['message'] = (
                response_3.get('message')
                or 'Could not create user in Identity Service'
            )
            return response_3
        bridge['cognito_username'] = response_3['document']['User']['Username']
        transaction.append(response_3)


        #4. Create User Entity
        #Input: email, cognito_username, first, last
        data = {}
        bridge['user_id'] = data['user_id'] = create_md5_hash(bridge['cognito_username'],9)
        data['name'] = kwargs['first']
        data['slot_a'] = kwargs['last']
        data['email'] = kwargs['email']

        response_4 = self.create_entity('user',**data)
        self.logger.debug('Step 4: Create User Entity Document')
        if not response_4['success']:                      
            return response_4
        
        transaction.append(response_4)



        #5. Leave the invitation pending. The new account is not on the team
        # until this person accepts it after they sign in.
        index = 'irn:rel:email:hash:ttl:*:*:*'
        prefix = kwargs['email']+':'+kwargs['code']
        response_5 = self.AUM.list_rel_prefix(index,prefix)
        if not (response_5.get('success') and response_5.get('document')):
            return{
                "success":False,
                "message": "Could not find any invitation related to this email:code",
                "status" :400
                }
        transaction.append(response_5)



        #6. Send signal to FE to show "Account Created" message and redirect to loing page 
        # OPTIONALLY (SKIP): You could  programmatically login the user and send access tokens in the response 
        # That way they new user gets immediate access without having to go to the login page.
        # The problem is that you wouldn't show the user how to sing in. 

        #Returning status:200 is enough for FE to decide what to do next (redirect to sing in page)



        #All went good, Summarize Transaction Success 
        self.logger.debug('End of Funnel ')

        result['success'] = True
        result['message'] = 'Create Invite User Funnel completed, Ok'
        result['status'] = 200 
        result['document'] = transaction  

        self.logger.debug(result)            
        return result

    

    def _soft_delete_entity(self, entity_type, **lookup):
        """
        Mark a portfolio or org deleted and leave every relationship in place.

        Tree generation and get/list skip the row. The document stays so the
        data under that id can be recovered.
        """
        label = 'Portfolio' if entity_type == 'portfolio' else 'Org'
        if entity_type == 'portfolio' and not lookup.get('portfolio_id'):
            return {
                "success": False,
                "message": "Missing attributes",
                "status": 400,
            }
        if entity_type == 'org' and not (
            lookup.get('portfolio_id') and lookup.get('org_id')
        ):
            return {
                "success": False,
                "message": "Missing attributes",
                "status": 400,
            }

        current = self.get_entity(entity_type, include_deleted=True, **lookup)
        if not current.get('success'):
            current['message'] = label + ' not found'
            return current

        document = current['document']
        if entity_is_deleted(document):
            return {
                "success": True,
                "message": label + " already deleted",
                "status": 200,
                "document": [current],
            }

        document['status'] = ENTITY_STATUS_DELETED
        updated = self.AUM.update_entity(document)
        if not updated.get('success'):
            return updated

        self.logger.debug('%s soft-deleted; relationships kept', label)
        synced = self.trees.note_entity(entity_type, document, lookup, deleted=True)
        if not synced.get('success'):
            return synced
        return {
            "success": True,
            "message": label + " deleted",
            "status": 200,
            "document": [updated],
        }

    @coalesced_trees
    def remove_portfolio_funnel(self, **kwargs):
        self.logger.debug('Initiating DELETE PORTFOLIO FUNNEL')
        return self._soft_delete_entity(
            'portfolio',
            portfolio_id=kwargs.get('portfolio_id'),
        )

    @coalesced_trees
    def remove_org_funnel(self,**kwargs):
        self.logger.debug('Initiating DELETE ORG FUNNEL')
        return self._soft_delete_entity(
            'org',
            portfolio_id=kwargs.get('portfolio_id'),
            org_id=kwargs.get('org_id'),
        )
    


    @coalesced_trees
    def remove_team_funnel(self,**kwargs):
        bridge = {}
        result = {}
        transaction = []

        self.logger.debug('Initiating DELETE TEAM FUNNEL')


        #0. Check minimum requirements
        required_keys = ['portfolio_id','team_id'] 
        if not all(key in kwargs for key in required_keys):
            return{
            "success":False, 
            "message": "Missing attributes", 
            "status" :400
            }
        
        #1. Unlink document

        #1a. Retrieve document to be unlinked
        response_1a = self.get_entity(
                    'team',
                    portfolio_id=kwargs['portfolio_id'],
                    team_id=kwargs['team_id']
                    )
            
        if not response_1a['success']:
            return{
            "success":False, 
            "message": "Team not found", 
            "status" :400
            }
                
        #1b. Send document to unlink function.
        # A portfolio or org document must not be removed by this path.
        teamdoc = response_1a['document']
        if entity_forbids_hard_delete(teamdoc):
            return {
                "success": False,
                "message": HARD_DELETE_REFUSED,
                "status": 400,
            }
        response_1b = self.unlink_entity(**teamdoc)  
        if not response_1b['success']:
            return response_1b
         
        transaction.append(response_1b)



        #2. Remove team-portfolio rels
        # Remove rel team:portfolio 
        # You need to list the rels first and then eliminate one at a time
        response_2a = self.list_rel(
                'team:portfolio',
                team_id=kwargs['team_id']
                )
        
        for portfolio in response_2a['document']['items']:

            response_2aa = self.delete_rel(
                    'team:portfolio',
                    team_id=kwargs['team_id'],
                    portfolio_id=portfolio['rel']
                    )
            if not response_2aa['success']:
                response_2aa['message'] = 'Could not remove Team-Portfolio relationship'
                return response_2aa                 
            else:
                transaction.append(response_2aa)


        #3. Remove team-user rels
        # You need to list the rels first and then eliminate one at a time
        response_3 = self.list_rel(
                'team:user',
                team_id=kwargs['team_id']
                )
        
        for user in response_3['document']['items']:

            response_3a = self.delete_rel(
                    'team:user',
                    team_id=kwargs['team_id'],
                    user_id=user['rel']
                    )   
            if not response_3a['success']:
                response_3a['message'] = 'Could not remove Team-User relationship'
                return response_3a                 
            else:
                transaction.append(response_3a)

            
            response_3b = self.delete_rel(
                    'user:team',
                    team_id=kwargs['team_id'],
                    user_id=user['rel']
                    )   
            if not response_3b['success']:
                response_3b['message'] = 'Could not remove User-Team relationship'
                return response_3b                 
            else:
                transaction.append(response_3b)


        #4. Remove team-org rels
        response_4 = self.list_rel(
                'team:org',
                team_id=kwargs['team_id']
                )
        
        for org in response_4['document']['items']:

            response_4a = self.delete_rel(
                    'team:org',
                    team_id=kwargs['team_id'],
                    user_id=org['rel']
                    )   
            if not response_4a['success']:
                response_4a['message'] = 'Could not remove Team-Org relationship'
                return response_4a                 
            else:
                transaction.append(response_4a)


        #All went good, Summarize Transaction Success 
        self.logger.debug('End of Funnel ')

        result['success'] = True
        result['message'] = 'Delete Team Funnel completed, Ok'
        result['status'] = 200 
        result['document'] = transaction  

        self.logger.debug(result)            
        return result
    
    

    @coalesced_trees
    def remove_user_from_team_funnel(self,**kwargs):
        bridge = {}
        result = {}
        transaction = []

        self.logger.debug('Initiating REMOVE TEAM USER FUNNEL')


        #0. Check minimum requirements
        required_keys = ['team_id','user_id'] 
        if not all(key in kwargs for key in required_keys):
            return{
            "success":False, 
            "message": "Missing attributes", 
            "status" :400
            }


        #1. Remove rels
        #1a. Remove rel team:user 
        response_1a = self.delete_rel(
                'team:user',
                team_id=kwargs['team_id'],
                user_id=kwargs['user_id']
                )   
        if not response_1a['success']:
            response_1a['message'] = 'Could not remove Team-User relationship'
            return response_1a                 
        else:
            transaction.append(response_1a)

        
        response_1b = self.delete_rel(
                'user:team',
                team_id=kwargs['team_id'],
                user_id=kwargs['user_id']
                )   
        if not response_1b['success']:
            response_1b['message'] = 'Could not remove Team-User relationship'                 
            return response_1b
        else:
            transaction.append(response_1b)

  
        #All went good, Summarize Transaction Success 
        self.logger.debug('End of Funnel')

        result['success'] = True
        result['message'] = 'Remove Team User funnel completed, Ok'
        result['status'] = 200 
        result['document'] = transaction  

        self.logger.debug(result)            
        return result


    @coalesced_trees
    def create_extension_funnel(self,**kwargs):
        result = {}
        transaction = []

        self.logger.debug('Initiating CREATE EXTENSION FUNNEL')

        response_1 = self.create_entity('extension',**kwargs)

        self.logger.debug('Step 1: Installing extension in portfolio')
        self.logger.debug(response_1)

        if not response_1['success']:
            response_1['message'] = 'Could not install Extension'
            return response_1
        transaction.append(response_1)

        result['success'] = True
        result['message'] = 'Create Extension Funnel completed, Ok'
        result['status'] = 200
        result['document'] = transaction
        return result

    def create_tool_funnel(self,**kwargs):
        # New installs write the extension entity. Legacy create_tool callers
        # keep the same funnel name until that API is retired.
        return self.create_extension_funnel(**kwargs)
    
    
    @staticmethod
    def _response_items(response):
        document = (response or {}).get("document")
        if isinstance(document, list):
            return document
        if isinstance(document, dict):
            return document.get("items") or []
        return []

    def _delete_tool_entity_rows(self, portfolio_id, tool_id):
        """
        Hard-delete the installable from the entity table.

        Checks both extension and legacy tool partitions, live and unentity.
        Dynamo delete_item is idempotent, so we get first and only delete
        rows that exist. Does not touch data rings.
        """
        deleted_rows = []
        for kind in self.INSTALLABLE_TYPES:
            for archived in (False, True):
                index = self._installable_entity_index(
                    kind, portfolio_id, archived=archived
                )
                existing = self.AUM.get_entity(index, tool_id)
                if not existing.get("success"):
                    continue
                document = dict(existing.get("document") or {})
                document.setdefault("index", index)
                if entity_forbids_hard_delete(document):
                    return {
                        "success": False,
                        "message": HARD_DELETE_REFUSED,
                        "status": 400,
                    }
                deleted = self.AUM.delete_entity(index=index, _id=tool_id, type=document.get("type"))
                if not deleted.get("success"):
                    if deleted.get("status") == 400:
                        return deleted
                    continue
                deleted_rows.append(deleted)
                self.logger.info(
                    "Deleted %s %s row %s in %s",
                    "archived" if archived else "live",
                    kind,
                    tool_id,
                    portfolio_id,
                )
        if deleted_rows:
            return {
                "success": True,
                "message": "Entity deleted",
                "document": {"items": [row.get("document") for row in deleted_rows]},
                "status": 200,
            }
        return {
            "success": False,
            "message": "Nothing to delete",
            "status": 404,
        }

    @coalesced_trees
    def _apply_installable_rels(self, method, rel_key, team_id, entity_id, kinds=None, **extra):
        """
        Create or delete a grant on one or both installable rel families.

        DELETE always hits tool and extension. POST writes the detected kind,
        or both when the entity is not found (mixed leftover grants).
        """
        payload = {
            "team_id": team_id,
            "tool_id": entity_id,
            "extension_id": entity_id,
        }
        payload.update(extra)
        last = None
        for kind in kinds or self.INSTALLABLE_TYPES:
            reltype = self._installable_rel_types(kind)[rel_key]
            if method == "POST":
                last = self.create_rel(reltype, **payload)
            elif method == "DELETE":
                last = self.delete_rel(reltype, **payload)
            else:
                return {
                    "success": False,
                    "message": "Unsupported method",
                    "status": 400,
                }
            if not last.get("success"):
                return last
        return last or {
            "success": False,
            "message": "No installable rel applied",
            "status": 400,
        }

    def _installable_write_kinds(self, portfolio_id, entity_id):
        found = self.get_installable(portfolio_id, entity_id)
        if found:
            return found, [found["kind"]]
        return None, list(self.INSTALLABLE_TYPES)

    def _remove_tool_rels(self, portfolio_id, tool_id, transaction):
        """Drop assign/role/org rels for both tool and extension families."""
        teams = self._response_items(
            self.list_entity("team", portfolio_id=portfolio_id)
        )
        for team in teams:
            team_id = (team or {}).get("_id") or (team or {}).get("team_id")
            if not team_id:
                continue

            for kind in self.INSTALLABLE_TYPES:
                rels = self._installable_rel_types(kind)
                existing = self.get_rel(
                    rels["assign"],
                    team_id=team_id,
                    tool_id=tool_id,
                    extension_id=tool_id,
                )
                if existing.get("success"):
                    team_tool = self.delete_rel(
                        rels["assign"],
                        team_id=team_id,
                        tool_id=tool_id,
                        extension_id=tool_id,
                    )
                    if not team_tool.get("success"):
                        team_tool["message"] = (
                            "Could not remove Team-installable relationship"
                        )
                        return team_tool
                    transaction.append(team_tool)

                for role in self._response_items(
                    self.list_rel(
                        rels["role"],
                        team_id=team_id,
                        tool_id=tool_id,
                        extension_id=tool_id,
                    )
                ):
                    role_id = (role or {}).get("rel")
                    if not role_id:
                        continue
                    deleted = self.delete_rel(
                        rels["role"],
                        team_id=team_id,
                        tool_id=tool_id,
                        extension_id=tool_id,
                        role_id=role_id,
                    )
                    if not deleted.get("success"):
                        deleted["message"] = (
                            "Could not remove Team-installable-Role relationship"
                        )
                        return deleted
                    transaction.append(deleted)

                for org in self._response_items(
                    self.list_rel(
                        rels["org"],
                        team_id=team_id,
                        tool_id=tool_id,
                        extension_id=tool_id,
                    )
                ):
                    org_id = (org or {}).get("rel")
                    if not org_id:
                        continue
                    deleted = self.delete_rel(
                        rels["org"],
                        team_id=team_id,
                        tool_id=tool_id,
                        extension_id=tool_id,
                        org_id=org_id,
                    )
                    if not deleted.get("success"):
                        deleted["message"] = (
                            "Could not remove Team-installable-Org relationship"
                        )
                        return deleted
                    transaction.append(deleted)
        return None

    @coalesced_trees
    def remove_tool_funnel(self,**kwargs):
        result = {}
        transaction = []

        self.logger.debug('Initiating DELETE TOOL FUNNEL')


        #0. Check minimum requirements
        portfolio_id = kwargs.get('portfolio_id')
        tool_id = kwargs.get('tool_id') or kwargs.get('extension_id')
        if not portfolio_id or not tool_id:
            return{
            "success":False, 
            "message": "Missing attributes", 
            "status" :400
            }

        #1. Delete the tool entity row (live and any unentity archive).
        #   Data rings are not touched. Missing entity is ok: still drop rels.
        #   A portfolio or org row is refused and its relationships stay.
        response_1b = self._delete_tool_entity_rows(portfolio_id, tool_id)
        if (
            not response_1b.get('success')
            and response_1b.get('status') == 400
            and response_1b.get('message') == HARD_DELETE_REFUSED
        ):
            return response_1b
        if response_1b.get('success'):
            transaction.append(response_1b)
        else:
            self.logger.debug(
                'Tool entity %s already absent in %s; removing leftover rels',
                tool_id,
                portfolio_id,
            )

        noted = self.trees.note_portfolio_and_teams(portfolio_id)
        if not noted.get('success'):
            return noted

        #2. Remove team-tool / role / org rels for every team in the portfolio.
        #   The DELETE route has no team_id; the old commented cleanup could
        #   not run even if it were enabled.
        rels_error = self._remove_tool_rels(portfolio_id, tool_id, transaction)
        if rels_error:
            return rels_error

        #All went good, Summarize Transaction Success 
        self.logger.debug('End of Funnel ')

        result['success'] = True
        result['message'] = 'Delete Tool Funnel completed, Ok'
        result['status'] = 200 
        result['document'] = transaction  

        self.logger.debug(result)            
        return result
    
    
    
    def assign_team_tools(self,**kwargs):
        
        result = {}
        #Check minimum requirements:
        required_keys = ['team_id','tool_id'] 
        if not all(key in kwargs for key in required_keys):
            result['success'] = False
            result['message'] = 'Missing attributes' 
            result['status'] = 400             
            return result

        method = kwargs.get('method')
        entity_id = kwargs['tool_id']
        _found, write_kinds = self._installable_write_kinds(
            self._portfolio_id_for_team(kwargs['team_id']),
            entity_id,
        )
        kinds = list(self.INSTALLABLE_TYPES) if method == 'DELETE' else write_kinds
        return self._apply_installable_rels(
            method,
            'assign',
            kwargs['team_id'],
            entity_id,
            kinds=kinds,
        )
    
    
    def assign_team_tool_roles(self,**kwargs):
        
        result = {}
        #Check minimum requirements:
        required_keys = ['team_id','tool_id','role_id'] 
        if not all(key in kwargs for key in required_keys):
            result['success'] = False
            result['message'] = 'Missing attributes' 
            result['status'] = 400             
            return result

        role_id = str(kwargs['role_id']).strip()
        if not role_id:
            return {
                "success": False,
                "message": "Invalid role_id",
                "status": 400,
            }

        method = kwargs.get('method')
        entity_id = kwargs['tool_id']
        found, write_kinds = self._installable_write_kinds(
            self._portfolio_id_for_team(kwargs['team_id']),
            entity_id,
        )

        if method == 'POST' and found:
            catalog = self._normalize_roles_list(
                (found.get('document') or {}).get('roles')
            )
            if catalog and role_id not in catalog:
                return {
                    "success": False,
                    "message": f"Role '{role_id}' is not defined for this {found['kind']}",
                    "status": 400,
                }

        kinds = list(self.INSTALLABLE_TYPES) if method == 'DELETE' else write_kinds
        return self._apply_installable_rels(
            method,
            'role',
            kwargs['team_id'],
            entity_id,
            kinds=kinds,
            role_id=role_id,
        )
    
    
    def assign_team_tool_orgs(self,**kwargs):
        
        result = {}
        #Check minimum requirements:
        required_keys = ['team_id','tool_id','org_id'] 
        if not all(key in kwargs for key in required_keys):
            result['success'] = False
            result['message'] = 'Missing attributes' 
            result['status'] = 400             
            return result

        method = kwargs.get('method')
        entity_id = kwargs['tool_id']
        _found, write_kinds = self._installable_write_kinds(
            self._portfolio_id_for_team(kwargs['team_id']),
            entity_id,
        )
        kinds = list(self.INSTALLABLE_TYPES) if method == 'DELETE' else write_kinds
        return self._apply_installable_rels(
            method,
            'org',
            kwargs['team_id'],
            entity_id,
            kinds=kinds,
            org_id=kwargs['org_id'],
        )

    def create_installable_org_rel(self, portfolio_id, team_id, entity_id, org_id):
        """Create team/<kind>:org using whichever catalog the id lives in."""
        _found, write_kinds = self._installable_write_kinds(portfolio_id, entity_id)
        return self._apply_installable_rels(
            "POST",
            "org",
            team_id,
            entity_id,
            kinds=write_kinds,
            org_id=org_id,
        )



        
    