# Renglo Auth, in essence

Auth turns an anonymous request into a sentence the rest of Renglo can act on:

> **Ada**, working for **Acme**, at the **Downtown** site, may use **Data**, as an **editor**.

Every part of the service exists to produce that sentence cheaply, on every request. This document is the whole idea in about ten minutes. It is not a tour of the code: method signatures live in [0.auth-controller.md](0.auth-controller.md), and storage layout, id formats, and response shapes are deliberately absent here because none of them change how the system thinks.

---

## 1. Two questions, one seam

**Authentication** — *are you who you say you are?* Renglo does not answer this. An external identity provider holds the credentials and issues signed tokens. Auth reads the result and trusts it.

**Authorization** — *may you do this, here?* This is the entire job of the service.

The seam between the two is a **stable user id**: a short id derived deterministically from the identity provider's username. Email is never a key. That id is the only thing Auth carries forward from the authentication world, and it is the join key for everything else — profiles, membership, "who did this."

Two practical consequences:

- **Identity in, identity out.** Application code asks Auth "who is calling?" and gets an id. It never parses a token itself. When work leaves the HTTP process — a job runner, a container — the caller's identity is forwarded in the payload and applied on the other side, so the worker sees the same id the API saw.
- **The profile is separate from the credential.** Auth keeps a small user record (name, email copy, tags) that it owns, and the provider keeps the credential. They are joined by the id, and either can be missing for a moment without breaking the other.

---

## 2. Four containers and a person

Renglo's world is four kinds of container plus the person who moves between them. Each one maps onto something a customer would recognize, which is why the model survives:

| Concept | Renglo name | Plain meaning |
|---|---|---|
| Tenant | **Portfolio** | The customer. The top of every data path. |
| Site | **Org** | A building, brand, or business unit inside the tenant. Most application data is scoped to *(portfolio, org)*. |
| Group | **Team** | A set of people. The only thing that ever holds access. |
| Installed app | **Tool** | One system extension installed into this tenant, carrying the list of role names it supports. |
| Person | **User** | Global. Not owned by any tenant. |

The person is global on purpose. A consultant working for three customers is one user on teams in three portfolios — not three accounts. There is no "user belongs to tenant" record anywhere in the system; the path is always **user → team → portfolio**.

```
Ada ──member of──► Editors ──sits in──► Acme
                      │                  ├── Downtown  (org)
                      │                  └── Data      (tool)
                      └── holds all the grants
```

---

## 3. The rule that shapes everything: access attaches to groups

**A person is never granted anything directly.** Not a tool, not a role, not a site. Ada can edit Data in Downtown because she is on Editors, and *Editors* holds those grants.

This single constraint is what makes the rest of the system small, and it is the first thing to preserve in any reimplementation. It buys you:

- **Revocation is one edge.** Take Ada off Editors and every derived permission disappears at once. There is no per-user grant table to sweep.
- **Access is explainable.** Any "why can Ada do this?" resolves to a short path through named things a customer understands, not a pile of individual exceptions.
- **Onboarding is copy-paste.** The tenth employee gets the same team as the first and is immediately correct.

The cost is that you must always ask "which team?" before you can grant anything. That is the intended friction.

---

## 4. A grant is a sentence with three parts

To let a team work, you say three things about it. They are separate on purpose, and they answer different questions:

1. **Editors may use Data** — *which apps does this group have at all?*
2. **Editors are `editor` on Data** — *at what level?*
3. **Editors may use Data in Downtown** — *at which sites?*

Splitting them means a group can have wide app access and narrow site access, or the reverse, without a combinatorial grant table. It also means adding a site to an existing team is one statement, not a re-grant of everything.

Two distinctions that trip people up, both worth building in explicitly:

- **Catalog roles vs assigned roles.** The tool knows which role names exist (`viewer`, `editor`, `admin`). The team holds which of those it was actually given. Those are different lists that happen to share a vocabulary.
- **Roles stack.** If Ada is on two teams that both reach Data in Downtown, her roles are the union. There is no precedence or override; more membership only ever means more.

There is also a portfolio-wide scope, written as a sentinel org name, for data that belongs to the tenant rather than to any one site — configuration, tenant-level schedules. Read it as "the tenant itself," never as "all sites."

---

## 5. Existence is not access

An org can exist in a tenant and still be completely closed to you. A site becomes **active** in your world only when some team you are on has a *tool-in-this-org* grant for it.

This is the least obvious idea in the model and the most useful. It means the third part of the grant sentence does double duty: it authorizes the tool, and it is also the switch that opens the site. A freshly created org is inert — visible to administration, invisible to work — until someone deliberately puts a team to work in it. You never have to separately "publish" or "enable" a site, and you can never accidentally leak one by creating it.

Directory listings and access checks therefore answer different questions, and both are legitimate. An admin screen may list orgs that no one can enter yet.

---

## 6. The snapshot

So far this is a graph: people joined to groups, groups joined to tenants, groups granted apps, roles, and sites. Walking that graph on every request would be several round trips for a yes/no answer.

So Auth compiles it. For each user, it produces one **snapshot** of their entire world: every tenant they reach, the teams they are on, the sites in those tenants and whether each is active for them, the installed tools with their role catalogs, and the roles they hold. Every authorization check reads this snapshot and nothing else.

The graph stays the source of truth for writes; the snapshot is a derived cache. That gives you the usual cache contract, and it is the one operational thing an implementer must get right:

- **Writes invalidate.** Changing membership or grants must rebuild the affected snapshot, or checks keep answering from the old world.
- **Invalidation is per person.** Granting Ada something rebuilds *Ada's* snapshot. If you change Bob's access, Bob's snapshot is stale until it is rebuilt for him — refreshing your own does nothing for him. This is the sharpest edge in the design and the one to handle deliberately rather than discover.

The snapshot is also what the UI reads to draw the workspace switcher, which is a good test of whether it contains the right things: if the console needs a name, the snapshot should already carry it.

---

## 7. The decision

Every check is the same call, with the same shape: *for this caller, in this tenant, at this site — and optionally for this app — yes or no, and with which roles?*

- **Site-level:** passes when the site is active for the caller.
- **App-level:** passes when the site check passes *and* some team of theirs may use that app in that site.

A successful answer carries the **roles**. That is where Auth stops, and the boundary is intentional: Auth decides *whether you are in the room*; the application decides *what you may touch once you are there*. An extension that needs "editors can delete, viewers cannot" reads the returned roles and makes that call itself. Auth accepts an action name in the check for future use, but it does not interpret it today.

The one rule that must never bend: **roles are resolved by the server and never accepted from the client.** When the request passes through a decorator or is forwarded to a worker, the roles are stamped onto the payload by the server, overwriting whatever was there. A caller supplying its own role list is the whole attack.

For anyone consuming Auth, the contract is three lines long: resolve the caller, check before you read or write tenant data, trust the returned roles.

---

## 8. How a workspace comes to life

The model is easier to hold once you have walked the golden path. Creating a tenant gives you a tenant, one **Admin** team, and you as its only member — nothing else. There is no default site and no default app, because guessing either one is worse than the empty state.

From there, four steps make work possible:

1. **Create a site.** It exists and is inert.
2. **Install an app** into the tenant. Installing links a known system app to this tenant; it does not invent a product.
3. **Create a team** (or reuse Admin) and put people on it.
4. **Grant** the app to the team, give it a role, and point it at the site. The last grant activates the site, and it is the moment the app is asked to set itself up for that site — configuration, scheduled jobs, whatever it needs.

Notice that nothing useful happens until step 4. That is the model being honest: a tenant is a container, and access is the only thing that turns containers into a workspace.

---

## 9. How people arrive

People do not self-register. They arrive one of two ways, and the difference is only whether the identity provider has already heard of the email address.

**Known address:** they are added to the team immediately, which is also how they gain the tenant. No email is sent, because nothing needs to be proved — they already have an account.

**Unknown address:** Auth stores a short-lived challenge (address, code, expiry) and a pointer from that code to the destination team, then mails the invitee a link. They accept while signed out by submitting the code, their address, their name, and a password. Accepting creates the account, creates the profile, and attaches the membership in one move.

Two properties are load-bearing. First, **the code never comes back to the inviter** in any response; if it did, the invite would prove nothing beyond "somebody knew an email address." Second, **membership is the destination**, not the tenant — you invite people onto a team, and the tenant follows from where that team sits. Accept is the only unauthenticated write in the service, and it is unauthenticated by design: possession of the inbox is the credential.

Removal is symmetric and immediate. The one restriction is that you cannot remove yourself, which keeps a team from becoming ownerless by accident.

---

## 10. The invariants

If you reimplement this service, these are the decisions that carry the design. Everything else — storage, id formats, transport, method names — can change freely.

1. **Authentication is somebody else's job.** Own the id, not the password.
2. **A person is global.** Tenancy is reached through membership, never stored on the user.
3. **Only groups hold access.** No direct user-to-app, user-to-role, or user-to-site grant, ever.
4. **A grant is three separable statements:** the app, the level, the site.
5. **Existence is not access.** A site is open only because a grant points at it.
6. **Compile per user, invalidate per user.** The graph is truth; the snapshot is speed.
7. **Auth grants entry; the app grants actions.** Roles are returned, not enforced.
8. **Roles come from the server.** Never from the caller.

---

## 11. Deliberate gaps

Worth knowing so you do not go looking for machinery that is not there:

- **Actions are named but not enforced.** The check takes an action argument and the administrative routes name their operations, but nothing maps roles to actions yet. Applications compare roles themselves. This is the natural place to grow if the model ever needs real permissions.
- **Deletion is mostly unlinking.** Records disappear from live listings; some edges outlive them. Expiry, not cleanup, is what makes stale invites harmless.
- **Multi-step writes are not transactional.** Creating a tenant is several records, and a failure partway through leaves a partial one.
- **Cross-tenant checks are uneven.** The model intends grants to stay inside a single tenant, and some paths verify it while others trust the caller.

None of these are load-bearing today, and the service is stable in production with all of them. They are listed because an implementer should choose them consciously rather than inherit them.

---

## Where to go next

- [0.auth-controller.md](0.auth-controller.md) — the method-by-method interface for calling the service
- [topics/](topics/) — one page per area, at implementation depth
