"""AuthModel interface.

Callers construct this class. CLOUD in config selects
renglo.auth.submodels.auth_model_<implementation>: aws, azure, or another submodel.
"""

from __future__ import annotations

from renglo.submodel import load_submodel

from renglo.auth.submodels.auth_model_aws import (
    logger,
    _cognito_temp_password,
)

def _submodel(config):
    return load_submodel("renglo.auth.submodels", "auth_model", "AuthModel", config)


class AuthModel:
    def __init__(self, config=None, tid=False, ip=False):
        cls = _submodel(config)
        object.__setattr__(self, "_impl", cls(config, tid, ip))

    def check_user_by_email(self, email):
        return self._impl.check_user_by_email(email)

    def cognito_user_delete(self, username):
        """Remove a Cognito user. Used to roll back a failed invite create."""
        return self._impl.cognito_user_delete(username)

    def cognito_user_create_with_permanent_password(self, email, password, first='FIRST', last='LAST'):
        """Create an invitee Cognito user already CONFIRMED (no NEW_PASSWORD_REQUIRED).

    AdminCreateUser always starts users in FORCE_CHANGE_PASSWORD; we immediately
    assign the user-chosen password as permanent. On failure after create, delete
    the Cognito user so they are not stuck unable to reset password.
    """
        return self._impl.cognito_user_create_with_permanent_password(email, password, first, last)

    def cognito_user_permanent_password_assign(self, email, password):
        return self._impl.cognito_user_permanent_password_assign(email, password)

    def cognito_user_create(self, email, first='FIRST', last='LAST'):
        """Create a Cognito user with a temporary password (FORCE_CHANGE_PASSWORD)."""
        return self._impl.cognito_user_create(email, first, last)

    def cognito_update_user_attributes(self, username, *, given_name=None, family_name=None):
        """Update standard Cognito profile attributes for an existing pool user."""
        return self._impl.cognito_update_user_attributes(username, given_name=given_name, family_name=family_name)

    def cognito_user_login_challenge(self, email, temporary_password, new_password):
        return self._impl.cognito_user_login_challenge(email, temporary_password, new_password)

    def send_email(self, sender, recipient, subject, body_text, body_html, inline_images=None):
        return self._impl.send_email(sender, recipient, subject, body_text, body_html, inline_images)

    def list_entity(self, index, limit=50, lastkey=None):
        return self._impl.list_entity(index, limit, lastkey)

    def get_entity(self, index, id):
        return self._impl.get_entity(index, id)

    def put_entity_exclusive(self, data):
        """Insert an entity only when that index/_id pair does not exist yet."""
        return self._impl.put_entity_exclusive(data)

    def create_entity(self, data):
        return self._impl.create_entity(data)

    def update_entity(self, data):
        return self._impl.update_entity(data)

    def delete_entity(self, **entity_document):
        return self._impl.delete_entity(**entity_document)

    def get_rel(self, index, rel):
        return self._impl.get_rel(index, rel)

    def list_rel(self, index, limit=50, lastkey=None):
        return self._impl.list_rel(index, limit, lastkey)

    def list_rel_prefix(self, partition_key_value, prefix):
        return self._impl.list_rel_prefix(partition_key_value, prefix)

    def create_rel(self, **rel_document):
        return self._impl.create_rel(**rel_document)

    def delete_rel(self, **rel_document):
        return self._impl.delete_rel(**rel_document)

    def __getattr__(self, name):
        return getattr(object.__getattribute__(self, "_impl"), name)

    def __setattr__(self, name, value):
        if name == "_impl":
            object.__setattr__(self, name, value)
            return
        setattr(self._impl, name, value)
