# Tools and grants

A **tool** is an installed copy of a known system app on a **portfolio** (handle + **role catalog**). **Grants** attach that tool to a **team**, optionally with a role, and optionally in an **org**.

What a tool entity is: [tenancy](02-tenancy-model.md). Edge types: [access graph](03-access-graph.md). How `active` and roles affect checks: [authorization](05-authorization.md). This page is the product: install and assign.

## Install, do not invent

Installing does not create a new product. It **links** a catalog entry to this tenant.

The HTTP catalog today is:

| Key you send | Display name | Roles in the catalog |
|---|---|---|
| `data` | Data | `viewer`, `editor`, `admin` |
| `schd` | Schd | `operator`, `admin` |

Anything else is rejected. The stored document gets the display name, handle, about text, and that role list.

Onboarding code may create a tool row directly with its own handle and catalog; the HTTP install path is the restricted catalog above.

Rename and unlink (soft-delete) apply to the installation. Unlink does not always remove grant edges.

## Three grants, usually in order

| Grant | Meaning |
|---|---|
| Tool → team | This team may use the installation |
| Role on that tool | Named role; on assign, the name must appear in the tool’s **catalog** if the catalog is non-empty |
| Tool in an org | This team may use that tool **in that org** (including `_all`) |

The third grant is what marks the org **active** for members of that team. Without it, the org can exist and still fail org-level authorization.

Assigned roles are **not** stored on the tool document. Catalog = tool; assignment = team+tool+role.

Each assign/unassign should refresh the caller’s [auth tree](04-auth-tree.md).

## After “tool in this org”

A successful **assign** of tool-in-org also asks the extension to **initialize** for that org (config, jobs, whatever that app needs). Failure is logged; the grant is **not** undone. Unassign does not run a teardown hook.

Onboarding that only installs the tool on the portfolio is not enough. Per-org setup starts when this grant is written. See [related modules](10-related-modules.md).

## Example

Ada should edit Data in Downtown: put Ada on Editors ([membership](06-invites-and-membership.md)), install Data on the portfolio, grant Data to Editors, grant `editor`, grant Data in Downtown.

## Related

- [Access graph](03-access-graph.md)
- [Authorization](05-authorization.md)
- [Portfolios, orgs, teams](07-portfolios-orgs-teams.md)
