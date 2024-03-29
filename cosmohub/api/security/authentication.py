from flask import g, current_app, request
from flask_httpauth import HTTPBasicAuth, HTTPTokenAuth
from itsdangerous import BadData, SignatureExpired
from sqlalchemy.orm import undefer_group, joinedload
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql import func
from werkzeug.datastructures import MultiDict

from cosmohub.api import db

from .privilege import Privilege
from ..database import model
from ..database.session import transactional_session

basic_auth = HTTPBasicAuth(realm='CosmoHub')
token_auth = HTTPTokenAuth(realm='CosmoHub', scheme='Token')

@basic_auth.verify_password
def verify_password(username, password):
    # Try embedded token in query string.
    # If it does not succeed, continue with username and password
    token = request.args.get('auth_token')
    if token:
        # FIXME: THIS IS WRONG. request.args is immutable :(
        # But we have to capture the token or it thinks is a form parameter too
        # For the next version, auth MUST be a header, no excuses.
        request.args = MultiDict(request.args)
        del request.args['auth_token']
        
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
            ).options(
                joinedload('groups_administered'),
            ).one()

        except NoResultFound:
            g.session['track']({
                't' : 'event',
                'ec' : 'login',
                'ea' : 'error',
            })
            return False

        else:
            # CAUTION: This comparison may refresh the password and requires
            # a writable transaction
            if not user.password == password:
                g.session['track']({
                    't' : 'event',
                    'ec' : 'login',
                    'ea' : 'failed',
                    'el' : user.id,
                })
                return False
            
            # Update last login timestamp
            user.ts_last_login = func.now()
            
            session.flush()
            session.expunge_all()
            
            g.session['user'] = user
            
            if user.ts_email_confirmed != None:
                if user.groups_administered:
                    g.session['privilege'] = Privilege('/user/admin')
                else:
                    g.session['privilege'] = Privilege('/user')
            
            g.session['track']({
                't' : 'event',
                'ec' : 'login',
                'ea' : 'successful',
                'el' : g.session['user'].id,
            })
            
            return True

@token_auth.verify_token
def verify_token(token):
    if not token:
        return False
    
    with transactional_session(db.session, read_only=False) as session:
        try:
            token = current_app.jwt.loads(token)
            
            user = session.query(model.User).filter(
                model.User.id==token['user']['id'],
            ).one()
        
        except SignatureExpired:
            g.session['track']({
                't' : 'event',
                'ec' : 'token',
                'ea' : 'expired',
            })
            return False
        
        except BadData:
            g.session['track']({
                't' : 'event',
                'ec' : 'token',
                'ea' : 'invalid',
            })
            return False
        
        except NoResultFound:
            return False
        
        session.flush()
        session.expunge(user)
        
        g.session['token'] = token
        g.session['user'] = user
        g.session['privilege'] = Privilege(token['privilege'])
        
        g.session['track']({
            't' : 'event',
            'ec' : 'token',
            'ea' : 'successful',
            'el' : g.session['user'].id,
        })
        
        return True
