# Users, invites, and membership

Identity is “who can sign in” ([01](01-identity-and-session.md)). This page is **how a person gets onto a team** — the only way they enter a tenant.

User as an entity type is [tenancy](02-tenancy-model.md). Bidirectional membership edges are [access graph](03-access-graph.md). Here is the product flow.

## Membership

A team’s roster is membership edges both ways (user→team and team→user). Adding or removing a person always writes **both**.

| Operation | Rule |
|---|---|
| List members | Caller must already be on that team. Result is names and emails, not the full user record. |
| Remove | Allowed for others on the team. **You cannot remove yourself.** |
| Add someone already in the identity service | They are attached immediately. **No invite email.** |
| Add an email the service has never seen | Invite email; they are not members until they accept. |

The inviter never receives the secret invite code in the API response. That would skip the “they have the inbox” proof.

## Invite (new email)

1. A 24-hour challenge is stored (address + short code + expiry).
2. A second record points the code at the **destination team**.
3. Mail goes to the invitee with a console link (`/invite`) and the code.
4. They submit code, email, first name, last name, and a password **without** being signed in.

On accept, Auth validates the challenge, creates a **confirmed** account with that password (no extra “set password on first login”), creates the Renglo user profile, and attaches them to the team(s) still valid for that email/code.

Expired challenges are rejected. If the email already has an account, accept tells them to **sign in** instead of creating a duplicate.

Stale invite records may remain after accept; expiry makes them useless.

## Invite (existing account)

If the address is already in the identity service, invite **does not** send mail. The person is added to the team (and thus to that team’s portfolio). Same-tenant or not, current behavior still adds them to the team.

## After membership changes

The [auth tree](04-auth-tree.md) is per user. Adding Ada to Editors does not update Bob’s tree, but it must refresh **Ada’s** (and the actor’s, if the HTTP layer always refreshes the caller). Until Ada’s tree is rebuilt, authorization may ignore the new team.

## Related

- [Identity](01-identity-and-session.md)
- [Access graph](03-access-graph.md)
- [Portfolios, orgs, teams](07-portfolios-orgs-teams.md) — creating the team you invite into
