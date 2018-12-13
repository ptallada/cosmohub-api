import ldap3 

from flask import (
    current_app,
)

def connection(user=None, password=None):
    read_only = False if user else True
    
    user = user if user else current_app.config['LDAP_BIND_USER']
    password = password if password else current_app.config['LDAP_BIND_PASSWORD']
    
    return ldap3.Connection(
        server = current_app.config['LDAP_HOST'],
        user = user,
        password = password,
        auto_bind=ldap3.AUTO_BIND_TLS_BEFORE_BIND,
        read_only = read_only,
        raise_exceptions = True,
    )

def user_reader(conn, uid=None, gecos=None):
    filters = []
    if uid:
        filters.append('uid: {0}'.format(uid))
    if gecos:
        filters.append('gecos: {0}'.format(gecos))
    
    query = ''
    if filters:
        query = ', '.join(filters)
    
    return ldap3.Reader(
        conn,
        ldap3.ObjectDef(['posixAccount'], conn),
        base=current_app.config['LDAP_BASE_USER'],
        query=query,
        get_operational_attributes=True
    )

def new_user(conn, uid):
    user_writer = ldap3.Writer(
        conn,
        ldap3.ObjectDef(
            [
                'account',
                'posixAccount',
            ],
            conn
        ),
        get_operational_attributes=True
    )
    
    dn = 'uid={0},{1}'.format(uid, current_app.config['LDAP_BASE_USER'])
    
    return user_writer.new(dn)

def group_reader(conn, cn=None, gidNumber=None, memberUid=None):
    filters = []
    if cn:
        filters.append('cn : {0}'.format('; '.join(cn)))
    if gidNumber:
        filters.append('gidNumber : {0}'.format('; '.join(gidNumber)))
    if memberUid:
        filters.append('memberUid : {0}'.format(memberUid))
    
    query = ''
    if filters:
        query = ', '.join(filters)
    
    return ldap3.Reader(
        conn,
        ldap3.ObjectDef(['posixGroup'], conn),
        base=current_app.config['LDAP_BASE_GROUP'],
        query=query,
        get_operational_attributes=True
    )