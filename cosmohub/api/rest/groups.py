from flask import g
from flask_restful import Resource, marshal

from cosmohub.api import db, api_rest, fields

from .. import ldap
from ..database import model
from ..database.session import transactional_session

class GroupCollection(Resource):
    def get(self):
        with transactional_session(db.session, read_only=True) as session:
            groups = session.query(
                model.GroupCatalog._group_id
            ).distinct().all()
            
            conn = ldap.connection()
            group_reader = ldap.group_reader(
                conn, 
                gidNumber=[str(group[0]) for group in groups],
            )
            group_reader.search()
            
            g.session['track']({
                't' : 'event',
                'ec' : 'groups',
                'ea' : 'list',
                'ev' : len(group_reader.entries),
            })
            
            return marshal(group_reader.entries, fields.Group)

api_rest.add_resource(GroupCollection, '/groups')
