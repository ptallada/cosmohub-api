import ldap3

from flask import g, current_app, request
from flask_httpauth import HTTPBasicAuth, HTTPTokenAuth
from flask_restful import marshal
from itsdangerous import BadData, SignatureExpired
from sqlalchemy.orm import undefer_group, joinedload
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql import func
from ldap3.core.exceptions import LDAPInvalidCredentialsResult, LDAPNoSuchAttributeResult
from werkzeug.datastructures import MultiDict

from cosmohub.api import db

from .privilege import Privilege
from .. import fields
from .. import ldap
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
    
    with ldap.connection() as conn:
        if '@' in username:
            user_reader = ldap.user_reader(conn, gecos=username)
        else:
            user_reader = ldap.user_reader(conn, uid=username)
        user_reader.search()
        
        # BEGIN
        # FIXME: Remove when LDAP migration is finished
        if not user_reader.entries:
            
            with transactional_session(db.session, read_only=False) as session:
                try:
                    user = session.query(model.User).options(
                        undefer_group('password'),
                    ).filter(
                        model.User.email==username,
                    ).options(
                        joinedload('groups_granted'),
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
                    
                    with ldap.connection(
                        current_app.config['LDAP_ADMIN_USER'],
                        current_app.config['LDAP_ADMIN_PASSWORD']
                    ) as sconn:
                        uidnext_reader = ldap3.Reader(
                            sconn,
                            ldap3.ObjectDef(['uidNext'], sconn),
                            base=current_app.config['LDAP_UIDNEXT_DN'],
                            get_operational_attributes=True
                        )
                        
                        uidNext = None
                        while not uidNext:
                            uidnext_reader.search()
                            
                            uidnext_entry = uidnext_reader.entries[0].entry_writable()
                            
                            next_uidNumber = uidnext_entry.uidNumber.value
                            
                            uidnext_entry.uidNumber -= next_uidNumber
                            uidnext_entry.uidNumber += next_uidNumber + 1
                            
                            try:
                                uidnext_entry.entry_commit_changes()
                            except LDAPNoSuchAttributeResult:
                                # Conflict while getting uidNext
                                pass
                            else:
                                uidNext = next_uidNumber
                        
                        uid = 'u{0}'.format(uidNext)
                        new_user = ldap.new_user(sconn, uid)
                        new_user.uidNumber = uidNext
                        new_user.gidNumber = 50009 # FIXME
                        new_user.cn = user.name
                        new_user.gecos = user.email
                        new_user.userPassword = password # FIXME
                        new_user.homeDirectory = '/dev/null'
                        
                        new_user.entry_commit_changes()
                        
                        user._id = uidNext
                        
                        groups_granted = [
                            group.name
                            for group in user.groups_granted
                        ]
                        groups_administered = [
                            group.name
                            for group in user.groups_administered
                        ]
                        groups = set(groups_granted) + set(groups_administered)
                        
                        group_reader = ldap.group_reader(sconn, cn = [groups])
                        group_writer = ldap3.Writer.from_cursor(group_reader)
                        
                        for group in group_writer:
                            if 
                        
                        
            
            user_reader.search()
        # FIXME: END
        
        if not user_reader.entries:
            # User NOT FOUND in LDAP
            g.session['track']({
                't' : 'event',
                'ec' : 'login',
                'ea' : 'error',
            })
            return False
        else:
            # User FOUND in LDAP
            try:
                conn.rebind(user_reader.entries[0].entry_dn, password)
                
                with transactional_session(db.session, read_only=True) as session:
                    groups = session.query(
                        model.GroupCatalog._group_id
                    ).distinct().all()
                    
                    gidNumber = [str(group[0]) for group in groups]
                    
                    group_reader = ldap.group_reader(
                        conn, 
                        gidNumber= gidNumber,
                        memberUid=user_reader.entries[0].uid.value,
                    )
                    group_reader.search()
                    
                    user = marshal(user_reader.entries[0], fields.UserToken)
                    user['groups'] = [group.gidNumber.value for group in group_reader.entries]
                    
                    g.session['user'] = user
                    
                    # FIXME: Check admin privileges
                    # FIXME: Check email validation
                    g.session['privilege'] = Privilege('/user/admin')
                    
                    # FIXME
                    # 
                    #if user.ts_email_confirmed != None:
                    #    if user.groups_administered:
                    #        g.session['privilege'] = Privilege('/user/admin')
                    #    else:
                    #        g.session['privilege'] = Privilege('/user')
                     
                    g.session['track']({
                        't' : 'event',
                        'ec' : 'login',
                        'ea' : 'successful',
                        'el' : g.session['user']['id'],
                    })
                    
                    return True
            
            except LDAPInvalidCredentialsResult:
                # Invalid password
                g.session['track']({
                    't' : 'event',
                    'ec' : 'login',
                    'ea' : 'failed',
                })
                
                return False

@token_auth.verify_token
def verify_token(token):
    if not token:
        return False
    
    try:
        token = current_app.jwt.loads(token)
    
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
    
    # FIXME: Verify if user still exists
    
    g.session['token'] = token
    g.session['user'] = token['user']
    g.session['privilege'] = Privilege(token['privilege'])
    
    g.session['track']({
        't' : 'event',
        'ec' : 'token',
        'ea' : 'successful',
        'el' : g.session['user']['id'],
    })
    
    return True
