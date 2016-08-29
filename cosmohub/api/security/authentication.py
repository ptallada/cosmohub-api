from flask import g, current_app, request
from flask_httpauth import HTTPBasicAuth, HTTPTokenAuth
from flask_restful import marshal
from itsdangerous import BadData
from operator import methodcaller
from sqlalchemy.orm import undefer_group
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql import func

from cosmohub.api import db

from .authorization import (
    Privilege,
    PRIV_ADMIN,
    PRIV_USER,
    PRIV_FRESH_LOGIN,
)
from ..marshal import schema
from ..db import model
from ..db.session import transactional_session

basic_auth = HTTPBasicAuth(realm='CosmoHub')
token_auth = HTTPTokenAuth(realm='CosmoHub', scheme='Token')

@basic_auth.verify_password
def verify_password(username, password):
    g.current_user = None
    g.current_privs = None
    
    # Try embedded token in query string.
    # If it does not succeed, continue with username and password
    token = request.args.get('auth_token')
    if token:
        granted = verify_token(token)
        
        if granted:
            return True
    
    if not username or not password:
        return False
    
    with transactional_session(db.session, read_only=False) as session:
        try:
            user = session.query(model.User).options(
                undefer_group('password'),
            ).filter(
                model.User.email==username,
            ).one()

        except NoResultFound:
            return False

        else:
            # CAUTION: This comparison may refresh the password and requires
            # a writable transaction
            if not user.password == password:
                return False

            # Update last login timestamp
            user.ts_last_login = func.now()

            g.current_user = marshal(user, schema.Token)
            g.current_privs = set([PRIV_FRESH_LOGIN])
            
            if user.ts_email_confirmed != None:
                g.current_privs.add(PRIV_USER)
                
            if user.is_admin:
                g.current_privs.add(PRIV_ADMIN)

            return True

@token_auth.verify_token
def verify_token(token):
    g.current_user = None
    g.current_privs = None
    
    if not token:
        return False
    
    with transactional_session(db.session, read_only=False) as session:
        try:
            token = current_app.jwt.loads(token)
            
            user = session.query(model.User).options(
                undefer_group('password'),
            ).filter(
                model.User.id==token['id'],
            ).one()
        
        except BadData:
            return False
        
        except NoResultFound:
            return False
    
        g.current_user = marshal(user, schema.Token)
        privs = set([tuple(priv) for priv in token.get('privs', [])])
        
        if user.ts_email_confirmed == None:
            privs.add(tuple(PRIV_USER.to_list()))

        if user.is_admin:
            privs.add(tuple(PRIV_ADMIN.to_list()))

        g.current_privs = set([Privilege(*priv) for priv in privs])

        return True

def refresh_token(current_privs=None):
    token = {}
    current_user = getattr(g, 'current_user', None)
    if current_user:
        token.update(current_user)
    
    if current_privs:
        token.update({ 'privs' : map(methodcaller('to_list'), current_privs)})
    
    return token
