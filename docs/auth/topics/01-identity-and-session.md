# Identity and session

A **person** in Renglo is someone who can sign in. Auth does not invent accounts from thin air: the platform identity service holds credentials; Renglo holds a **profile** and a **stable user id**.

How you construct the controller and call identity methods is in [0.auth-controller.md](../0.auth-controller.md). This page is only the idea.

## Sign-in vs Renglo profile

| Layer | Question it answers |
|---|---|
| Identity service | Can they prove who they are? (email, password, signed credentials) |
| Session | Are those credentials present on this request (or forwarded into a worker)? |
| User id | What short id does Renglo use as a join key? |
| User document | What name, email copy, and tags does Renglo store? |

The identity service is the source of truth for **authentication**. The user document is the source of truth for **profile data Renglo owns**. The user id joins the two.

New people do not self-register on a typical deployment. They arrive by **team invite** or **operator-created account**. See [invites and membership](06-invites-and-membership.md).

## Session

Authenticated HTTP calls send **signed credentials** in the request. The API rejects the call if they are missing or invalid.

The one exception is **accepting an invite**: the caller proves they received the email (code + address), not that they already have an account.

Workers that are not an HTTP request (jobs, containers) do not see the original header. The API **forwards** the caller’s identity into the payload, and the worker treats that as the current user. Roles on that payload are stamped **by the server**; clients cannot supply their own.

## Stable user id

Renglo does not use the email as a primary key. It derives a short **user id** from the identity-service username and uses that everywhere: profile document, team membership, trees, “who did this.”

The same person must yield the same id whether the request used an identity token or an access token. Application code should ask Auth for the current user id, not parse credentials itself.

## Profile

On first successful sign-in, the product **upserts** a user document if it does not exist (IP and language may be recorded). Reading “me” returns that document. Updating display name writes both the Renglo profile and the identity-service name attributes.

The user document is **global**. It is not owned by a portfolio. Which tenants the person can enter is membership, not identity ([access graph](03-access-graph.md)).

## Operator vs team-invite accounts

| How they were created | First sign-in |
|---|---|
| Operator invite with a temporary password | Must complete a one-time password change before normal sign-in |
| Team invite they accepted themselves | Password is already permanent; they sign in normally |

Password recovery is blocked until an operator-created account finishes that one-time change.

## Related

- [Tenancy](02-tenancy-model.md) — the user **entity** among five types
- [Invites](06-invites-and-membership.md) — how a person joins a team
- [Related modules](10-related-modules.md) — console sign-in and identity forwarding
