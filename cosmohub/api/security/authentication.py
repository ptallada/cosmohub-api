from flask import g, current_app, request
from flask_httpauth import HTTPBasicAuth, HTTPTokenAuth
from flask_restful import marshal
from itsdangerous import BadData
from operator import methodcaller
from sqlalchemy.orm import joinedload, undefer_group
from sqlalchemy.orm.exc import NoResultFound

from cosmohub.api import db

from .authorization import (
    Privilege,
    PRIV_ADMIN,
    PRIV_USER,
    PRIV_FRESH_LOGIN,
)
from .. import fields
from ..db import model
from ..db.session import transactional_session

basic_auth = HTTPBasicAuth(realm='CosmoHub')
token_auth = HTTPTokenAuth(realm='CosmoHub', scheme='Token')

@basic_auth.verify_password
def _verify_password(username, password):
    g.current_user = None
    g.current_privs = None
    
    # First, try embedded token in query string
    token = request.args.get('auth_token')
    if token:
        granted = _verify_token(token)
        
        if granted:
            return True
    
    # If token authentication does not succeed, fallback to  username and password
    
    if not username or not password:
        return False
    
    with transactional_session(db.session, read_only=False) as session:
        try:
            user = session.query(model.User).options(
                joinedload(model.User.groups),
                undefer_group('password'),
            ).filter_by(
                email=username,
                is_enabled=True,
            ).one()

        except NoResultFound:
            return False

        else:
            # CAUTION: This comparison may refresh the password and requires
            # a writable transaction
            if not user.password == password:
                return False

            g.current_user = marshal(user, fields.TOKEN)

            g.current_privs = set([PRIV_USER, PRIV_FRESH_LOGIN])
            if user.is_admin:
                g.current_privs.add(PRIV_ADMIN)

            return True

@token_auth.verify_token
def _verify_token(token):
    g.current_user = None
    g.current_privs = None
    
    if not token:
        return False
    
    try:
        g.current_user = current_app.jwt.loads(token)
    except BadData:
        return False
    
    g.current_privs = set([Privilege(*priv) for priv in g.current_user.pop('privs', [])])

    return True

def refresh_token():
    token = {}
    current_user = getattr(g, 'current_user', None)
    if current_user:
        token.update(current_user)
    
    current_privs = getattr(g, 'current_privs', None)
    if current_privs:
        current_privs.discard(PRIV_FRESH_LOGIN)
        token.update({ 'privs' : map(methodcaller('to_list'), current_privs)})
    
    return token
