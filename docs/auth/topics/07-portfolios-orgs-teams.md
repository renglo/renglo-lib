# Portfolios, orgs, and teams

These three entities are the **workspace**: tenant, sites, and groups. Definitions live in [tenancy](02-tenancy-model.md). This page is **lifecycle** — what create/update/delete mean in the product. Tools are [08](08-tools-and-grants.md). Membership is [06](06-invites-and-membership.md).

Mutating these should refresh the caller’s [auth tree](04-auth-tree.md).

## Portfolio (tenant)

A portfolio is the workspace. Files, data, and extensions all sit under a portfolio id.

**Create** is a funnel, not a lone row: the portfolio, a default **Admin** team, and the creator as that team’s first member. There is **no** default org and **no** default tool. Those are added later.

**List “mine”** is not “every portfolio in the system.” It is portfolios reached through the caller’s teams. Names are easier to take from the auth tree than from the raw list.

**Get / update** change the portfolio document (name, about, tags). There is no product funnel to delete a whole tenant.

## Org (site / unit)

An org is a data boundary inside a portfolio: most application records are *(portfolio, org, …)*.

**Create** needs a name (and the portfolio). A short **handle** is derived from the name (only capital letters and digits; an all-lowercase name can yield an empty handle). No team is granted access automatically — the org exists but is **inactive** until [tools and grants](08-tools-and-grants.md).

**Get / update** use either a composed id `portfolioId-orgId` or nested paths under the portfolio.

**Delete** has an HTTP route, but removal is **not implemented**. Calling it does not delete the org.

## Team (group)

A team is the unit of membership and grants. Users never receive tool roles directly.

**Create** makes the team, places it in the portfolio, and adds the creator as first member. The new team has **no** tools, roles, or orgs until someone assigns them.

**Update** is name and tags.

**Delete** unlinks the team document and clears placement and membership (and legacy team–org links). Tool grants on that team may be left behind.

Listing a team’s people and inviting onto it is [06](06-invites-and-membership.md).

## What a new workspace looks like

After “create portfolio” you have: one tenant, one Admin team, one member (you). You still need at least one org, at least one installed tool, and grants before anyone can work in a site.

## Related

- [Tenancy](02-tenancy-model.md)
- [Tools and grants](08-tools-and-grants.md)
- [Invites](06-invites-and-membership.md)
