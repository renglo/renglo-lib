"""Decide whether an external provider may create a Cognito user.

Native username/password and admin-created users are unchanged. When the
email already belongs to a user and the provider has verified it, the
Pre Sign-up function links that provider to the existing user and stops
this attempt. The next sign-in with the same provider opens that user.
An unverified email is still rejected, so a second profile is not created.
"""

from __future__ import annotations

EXTERNAL_SIGNUP = "PreSignUp_ExternalProvider"
ACCOUNT_LINKED = "ACCOUNT_LINKED"
EMAIL_TAKEN_MESSAGE = (
    "An account with this email already exists. Sign in with your password."
)


def email_is_verified(value: object) -> bool:
    return str(value or "").strip().lower() == "true"


def external_signup_action(
    trigger_source: str,
    *,
    email_taken: bool,
    email_verified: bool,
    can_link: bool,
) -> str:
    """Return allow, link, or deny.

    allow creates the federated user. link attaches the provider to the
    existing user and must not create another one. deny stops sign-up.
    """
    if trigger_source != EXTERNAL_SIGNUP or not email_taken:
        return "allow"
    if email_verified and can_link:
        return "link"
    return "deny"
