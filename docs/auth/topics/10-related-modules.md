# Related modules

Renglo Auth is not only the controller. These pieces sit beside it. They are not a second tenancy model; they **adapt** identity, authorization, and grants into HTTP, UI, workers, and platform setup.

Controller methods: [0.auth-controller.md](../0.auth-controller.md). Decision rules: [authorization](05-authorization.md). Tree: [04](04-auth-tree.md).

## Authorization decorator

Controllers that hold an Auth instance can mark a method: require org access, or tool access, before the method runs. On deny, the method is skipped and the deny payload is returned. On allow, the result is stored on the instance and **roles** are stamped onto `payload` so handlers do not trust the client.

## Identity forwarding (workers)

When work leaves the HTTP process (a job runner, a container), there is no request header. A small runtime copies the caller’s claims into the payload and, on the worker, applies them as **invocation identity**. It also overwrites the roles key so a client cannot forge roles. Warm workers must **reset** identity between invocations.

## Post-grant extension initialize

After a team is granted a tool **in an org**, Auth invokes that extension’s initialize handler (config, scheduled jobs). The grant is already stored; initialize failure does **not** roll it back. Unassign does not call a mirror teardown. See [tools and grants](08-tools-and-grants.md).

## Console sign-in

The web app talks to the **identity service** for sign-in, one-time password change (operator accounts), and password recovery. It stores credentials in the tab and upserts the Renglo user on first success ([identity](01-identity-and-session.md)). Public self-registration helpers may exist in the UI while the pool still **disallows** self-sign-up.

## Platform identity setup

Infrastructure defines the user pool: email sign-in, no public registration, token lifetime, and the operator-invite email that points at the console setup screen. That is provisioning, not the Auth controller.

## Legacy cookie login

An old helper redirected to a login page when a cookie session was missing. The current API uses **request credentials**, not that cookie. Treat the helper as unused.

## Status (for operators)

| Area | State |
|---|---|
| Identity, tree, invites, tenant/tool CRUD, grants, org/tool authorize | Live |
| Named checks on Auth HTTP routes | Stub (always allow) |
| Org delete funnel | Stub (does not delete) |
| Role→action mapping | Reserved, not enforced |
| Cookie login helper | Unused |

## Related

- [Identity](01-identity-and-session.md)
- [Authorization](05-authorization.md)
- [Tools and grants](08-tools-and-grants.md)
