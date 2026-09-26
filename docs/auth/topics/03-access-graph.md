# Access graph

Entities say *what exists* ([tenancy](02-tenancy-model.md)). The **access graph** says *who is attached*. Membership and grants are edges, not fields nested inside a portfolio or team document.

Invite pending rows are edges too; the product flow is [invites and membership](06-invites-and-membership.md). Tool/role/org assignment as a product feature is [tools and grants](08-tools-and-grants.md). This page is the graph itself.

## Shape

```
user ──membership──► team ──placement──► portfolio
                       │
                       ├── may use ──► tool
                       ├── role on that tool ──► role name
                       └── that tool in ──► org  (this makes the org active)
```

Rules that follow:

- No user-to-portfolio edge. A person is in a tenant only through a team.
- No user-to-tool or user-to-role edge. Ada is an editor on Data in Downtown because she is on Editors, and Editors holds those grants.
- Org and tool **documents** live under a portfolio. **Access** to them is not that nesting; it is the edges.

Membership is **two** edges (user→team and team→user). Creating only one breaks either “Ada’s teams” or “Editors’ roster.”

## Edge kinds

| Edge | Meaning |
|---|---|
| User ↔ team | Membership (both directions) |
| Team → portfolio | Team sits in this tenant (needed to find “Ada’s portfolios”) |
| Team → tool | Team may use this installed tool |
| Team + tool → role | Named role on that tool (`viewer`, `admin`, …) |
| Team + tool → org | Team may use that tool **in this org** (or the portfolio-wide sentinel `_all`) |
| Team → org | Legacy link. **Not** what makes an org active |
| Email + code + expiry | Pending invite challenge |
| Code → team | Invite destination |

Same-tenant checks ask: do this user, this team, and this org share a portfolio? Grant writes for tool/role/org do not all run that check; the product still *intends* grants to stay inside one tenant.

## What “active” means

An org document can exist and still be closed for a user. It becomes **active** in their world when **some team they belong to** has a **tool-in-org** edge for that org. That is the switch [authorization](05-authorization.md) uses for org access.

## Walks you will hear about

| Walk | Why |
|---|---|
| user → team → portfolio | Portfolios this person belongs to |
| Those edges + tool/role/org grants | Compiled into the [auth tree](04-auth-tree.md) |

The tree is a **cached projection** of this graph plus names. The graph remains the source of truth for writes.

## Related

- [Auth tree](04-auth-tree.md)
- [Authorization](05-authorization.md)
- [Invites](06-invites-and-membership.md)
- [Tools and grants](08-tools-and-grants.md)
