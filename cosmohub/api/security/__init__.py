from flask import g, jsonify
from flask_httpauth import MultiAuth
from functools import wraps
from werkzeug import exceptions as http_exc

from .authentication import (
    basic_auth,
    token_auth,
)
from .authorization import (
    PRIV_USER,
    PRIV_ADMIN,
    PRIV_FRESH_LOGIN,
    PRIV_DOWNLOAD,
    PRIV_PASSWORD_RESET,
    PRIV_EMAIL_CONFIRM,
)

auth = MultiAuth(basic_auth, token_auth)

def auth_required(priv):
    def wrapper(f):
        @wraps(f) 
        @auth.login_required
        def wrapped(*args, **kwargs):
            if not priv.can():
                raise http_exc.Forbidden
            
            return f(*args, **kwargs)
        
        return wrapped
    
    return wrapper

@basic_auth.error_handler
def _auth_error_handler():
    return jsonify(
        message = 'Authentication not present or invalid'
    )