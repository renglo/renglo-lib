import functools


def login_required(f):
    @functools.wraps(f)
    def decorated_function(*args, **kwargs):
        try:
            from flask import request, redirect, session, url_for  # type: ignore
        except Exception as exc:
            raise RuntimeError("login_required requires Flask runtime context") from exc
        if 'id_token' not in session:
            return redirect(url_for('app_auth.login', next=request.url))
        return f(*args, **kwargs)
    return decorated_function