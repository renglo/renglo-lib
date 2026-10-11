"""Decide whether Cognito should reject an external-provider sign-up.

Native username/password and admin-created users are unchanged. An external
provider is rejected when that email already belongs to a user in the pool,
before Cognito creates a second profile.
"""

from __future__ import annotations

EXTERNAL_SIGNUP = "PreSignUp_ExternalProvider"
EMAIL_TAKEN_MESSAGE = (
    "An account with this email already exists. Sign in with your password."
)


def should_deny_external_signup(trigger_source: str, email_taken: bool) -> bool:
    return trigger_source == EXTERNAL_SIGNUP and bool(email_taken)


def external_signup_error(trigger_source: str, email_taken: bool) -> str | None:
    """Return the message Cognito should show, or None when sign-up may continue."""
    if should_deny_external_signup(trigger_source, email_taken):
        return EMAIL_TAKEN_MESSAGE
    return None
