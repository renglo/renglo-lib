from flask import Flask, session, redirect, url_for, request, jsonify
import functools


def login_required(f):
    @functools.wraps(f)
    def decorated_function(*args, **kwargs):
        if 'id_token' not in session:
            return redirect(url_for('app_auth.login', next=request.url))
        return f(*args, **kwargs)
    return decorated_function