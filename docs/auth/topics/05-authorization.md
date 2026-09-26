# Authorization

Authorization answers: **may this caller use this portfolio and org (and optionally this tool)?** It does not create tenants or send invites. It reads the caller’s [auth tree](04-auth-tree.md).

How to call `authorize` is in [0.auth-controller.md](../0.auth-controller.md). This page is the decision.

## Two resource levels

| Check | Passes when |
|---|---|
| **Org** | That org is **`active`** for the user in that portfolio |
| **Tool** | Org check **and** some team of the user may use that tool **in that org** |

“Active” comes from a team/tool/org grant, not from the org merely existing ([access graph](03-access-graph.md)).

The tool may be named by **id or handle** (for example `data`). Roles returned are for the resolved tool.

`_all` as org: org-level access is allowed if the user has any real active org in the portfolio. Tool-level `_all` still needs an explicit `_all` grant. Details: [auth tree](04-auth-tree.md).

An **action** name can be passed (for example `delete`). It is **reserved**; it is not enforced yet. Extensions that need “editor vs viewer” compare the **roles list** themselves.

## Roles

Roles are the **union** of assignments from every team the user is on that has that tool in that org. Two teams can stack (`viewer` + `editor`).

They are **server-resolved**. Never trust a role list the client sent. On a successful decorated call, roles are stamped onto the payload under a reserved key.

Empty roles can mean “allowed into the org/tool but no named role” or “lookup failed” — prefer the allow/deny flag, then inspect roles.

## How product code should gate

Extension handlers should **authorize** before reading or writing tenant data.

A **decorator** on controller methods does the same check using `portfolio` / `org` / tool arguments, then stores the result on the instance and stamps roles on `payload` when present.

## What is not enforced yet

HTTP routes under Auth that create teams, invite users, and so on **name** actions (`createTeam`, `inviteUser`, …) but those named checks currently **always allow**. A valid signed-in session is still required. Real org/tool enforcement for extensions is the authorize path above.

## Outcomes (conceptually)

| Result | Meaning |
|---|---|
| Not signed in | Authentication required |
| No portfolio / inactive org | Access denied to organization |
| Tool not granted in that org | Access denied to tool |
| Unknown resource kind | Not implemented |
| Success | Caller id, resolved tool id, roles |

## Related

- [Auth tree](04-auth-tree.md)
- [Related modules](10-related-modules.md) — decorator and role forwarding
- [Tools and grants](08-tools-and-grants.md) — how roles get onto the tree
