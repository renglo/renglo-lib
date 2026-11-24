"""Lifecycle status stored on entity documents.

The entity table is shared by every entity type. ``status`` may be
``active``, ``archived``, or ``deleted``. A missing or blank status means
the row is live, which is how existing documents stay accessible without a
backfill.

Portfolios, orgs, rings, and collections are soft-deleted by setting
``status`` to ``deleted``. They stay in the table, and their relationships
stay in the rel table. Users, teams, and extensions are still removed by
their existing delete paths. Those paths must refuse a portfolio, org, ring,
or collection document.
"""

ENTITY_STATUS_ACTIVE = "active"
ENTITY_STATUS_ARCHIVED = "archived"
ENTITY_STATUS_DELETED = "deleted"

SOFT_DELETE_ENTITY_TYPES = frozenset({"portfolio", "org", "ring", "collection"})

HARD_DELETE_REFUSED = "Portfolios, orgs, rings, and collections cannot be fully deleted"


def entity_status(document):
    """Return the lifecycle status, treating a missing field as active."""
    if not isinstance(document, dict):
        return ENTITY_STATUS_ACTIVE
    raw = document.get("status")
    if raw is None:
        return ENTITY_STATUS_ACTIVE
    text = str(raw).strip().lower()
    if not text:
        return ENTITY_STATUS_ACTIVE
    return text


def entity_is_deleted(document):
    return entity_status(document) == ENTITY_STATUS_DELETED


def entity_index_is_portfolio_or_org(index):
    """True for the portfolio, org, ring, and collection partitions, including unlinked copies."""
    normalized = str(index or "").replace(":unentity:", ":entity:", 1)
    if normalized == "irn:entity:portfolio:*":
        return True
    if normalized.startswith("irn:entity:portfolio/org/ring:"):
        return True
    if normalized.startswith("irn:entity:portfolio/org/collection:"):
        return True
    return normalized.startswith("irn:entity:portfolio/org:")


def entity_forbids_hard_delete(document):
    """True when this row must not be removed from the entity table."""
    if not isinstance(document, dict):
        return False
    kind = str(document.get("type") or "").strip().lower()
    if kind in SOFT_DELETE_ENTITY_TYPES:
        return True
    return entity_index_is_portfolio_or_org(document.get("index"))
