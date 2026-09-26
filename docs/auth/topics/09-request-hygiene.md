# Request hygiene

The Auth **HTTP** layer does not only call the service. It **cleans and constrains** what a client may send. Library callers of the controller do not get this automatically; they must send already-sane data (or sanitize tags themselves).

This is not [authorization](05-authorization.md). Hygiene asks “is this payload well-formed and non-hostile?” Authorization asks “may this user do it?”

## What the API checks

**Required keys.** Invite, org create, team create, and tool install reject bodies that omit mandatory fields (for example org/team `name`, invite email + team + portfolio).

**Allowlists.** Some updates (org, team, tool name/tags) accept only named fields. Extra attributes are rejected. Values that look like injection (quotes, brackets, `$`, pipes, and similar) are dropped rather than stored.

**Tags.** Tags are a map of lowercase keys to lists of strings. Unsafe characters in keys or values are stripped. Empty or non-map tags become an empty map.

**JSON.** Missing or invalid JSON on mutating routes is rejected.

These rules apply at the HTTP boundary. Direct controller `update` with a `payload` dict will write whatever keys you pass (tags still get sanitized inside Auth).

## What it does not do

- It does not replace org/tool **authorization** for extensions.
- Named Auth-route actions (`createTeam`, `inviteUser`, …) are still a stub that always allows; session credentials are still required.
- Invite **accept** is unauthenticated by design; hygiene there is “required keys,” not a signed-in user.

## Related

- [Authorization](05-authorization.md)
- [0.auth-controller.md](../0.auth-controller.md) — `payload` / `tags` on updates
