# Auth tree

The **auth tree** is the compiled picture of everything **one signed-in user** can see and do: their portfolios, teams, orgs, installed tools, which orgs are active, and which roles they have.

It exists so every request does not re-walk the [access graph](03-access-graph.md). [Authorization](05-authorization.md) reads this snapshot, not the live graph, on each check. The console reads it to draw the workspace switcher.

How to load or refresh it in code is in [0.auth-controller.md](../0.auth-controller.md).

## What is in it

For that user only:

- **Portfolios** they reach via team membership (names, ids)
- **Teams** they are on, with per-tool **assigned** roles and org ids
- **Orgs** in those portfolios (names, handles, tags) and whether each is **`active`**
- **Tools** installed on the portfolio: display name, **handle**, and **role catalog**
- A synthetic org **`_all`** on every portfolio

An org can appear in the tree (it exists in the tenant) and still have `active: false` for this user. Existence ≠ access.

Catalog roles live on the **tool**. Assigned roles live on the **team**. Those are different lists.

## `_all`

`_all` is not a stored org. It is injected so portfolio-wide data (config, some schedulers) has an org key.

- Treating `_all` as “any org” is wrong.
- Org-level use of `_all` is allowed if the user already has **some** real active org.
- Tool-level use of `_all` still needs an explicit team/tool/`_all` grant.

## Cache and freshness

The tree is **cached per user**. Changing membership or grants does not update other people’s checks until **that user’s** tree is rebuilt.

Product writes that change the graph should **refresh** the current user’s tree. If you change another person’s grants, their next authorization can stay stale until their tree is rebuilt.

## Build order (conceptually)

1. User’s teams  
2. Each team’s portfolio  
3. Names of teams, tools, orgs in those portfolios  
4. That team’s tool / role / org grants  
5. Mark orgs active when a grant points at them  
6. Attach `_all`

## Related

- [Access graph](03-access-graph.md) — source of the snapshot
- [Authorization](05-authorization.md) — consumer of the snapshot
- [Tools and grants](08-tools-and-grants.md) — what `active` and roles mean in the product
