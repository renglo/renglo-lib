# Tenancy model

Renglo tenancy is five kinds of **entity**: named records that say *what exists*. Who is attached to what is the [access graph](03-access-graph.md). How you create and fetch records is in [0.auth-controller.md](../0.auth-controller.md).

This page is the model only. Day-to-day lifecycle of portfolios, orgs, and teams is [07](07-portfolios-orgs-teams.md). Tools are [08](08-tools-and-grants.md).

## The five types

```
User  (a person — global)
  └── member of teams (edges, not nesting)
        └── Team ── belongs to ── Portfolio (the tenant)
                                      ├── Org   (site / unit)
                                      └── Tool  (installed app)
```

| Type | Scope | Role in the product |
|---|---|---|
| **User** | Global | Person. Id comes from identity, not from a tenant. |
| **Portfolio** | Global | Tenant / workspace. “Which customer is this?” |
| **Org** | Inside a portfolio | Building, brand, or site. Most data is scoped *(portfolio, org)*. |
| **Team** | Inside a portfolio | Group of users. **All grants hang off teams.** |
| **Tool** | Inside a portfolio | Installed instance of a system app, plus the **catalog** of roles that instance allows. |

A person can work in many portfolios by sitting on teams in each. There is no “user belongs to portfolio” record. Path is always **user → team → portfolio**.

`_all` is **not** an entity. It is a sentinel org name meaning “portfolio-wide,” not “every org.” See [auth tree](04-auth-tree.md).

## Shared record

Every entity shares the same kind of document: an id, a type, a display name, optional description and handle, tags, owner, timestamps. Tools also carry a **role catalog** (names such as `viewer` or `admin`). Users use extra fields for email and last name.

Callers store the **id** and pass it back. Nested types are always addressed as parent portfolio id **plus** child id.

## What entities are not

- They do not contain nested member lists or grant arrays. Those are [relationships](03-access-graph.md).
- Creating a portfolio does not, by itself, mean anyone can open an org. Orgs start **inactive** until a team is granted a tool in that org ([tools and grants](08-tools-and-grants.md)).
- Installing a tool does not invent a new product. It **links** a known system tool to this portfolio.

## Create vs delete (conceptually)

Writes are either **one record** or a **funnel** (several records and edges that belong together — for example a new portfolio plus an Admin team and the creator as member).

Delete is usually **unlink**: the record disappears from live lists but is not immediately erased. Unlinking a team or tool does not always remove every leftover grant edge.

## Related

- [Portfolios, orgs, teams](07-portfolios-orgs-teams.md)
- [Tools and grants](08-tools-and-grants.md)
- [Access graph](03-access-graph.md)
