from flask import g
from flask_httpauth import MultiAuth
from functools import wraps
from werkzeug import exceptions as http_exc

from .authentication import (
    basic_auth,
    token_auth,
)
from .authorization import (
    PRIV_ADMIN,
    PRIV_FRESH_LOGIN,
    PRIV_QUERY_DOWNLOAD,
    PRIV_RESET_PASSWORD,
    PRIV_USER,
    PRIV_VALIDATE_EMAIL,
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
