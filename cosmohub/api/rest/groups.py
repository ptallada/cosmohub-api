from flask import g
from flask_restful import Resource, marshal

from cosmohub.api import db, api_rest, fields

from ..db import model
from ..db.session import transactional_session

class GroupCollection(Resource):
    def get(self):
        with transactional_session(db.session, read_only=True) as session:
            groups = session.query(model.Group).all()
            
            g.session['track']({
                't' : 'event',
                'ec' : 'groups',
                'ea' : 'list',
                'ev' : len(groups),
            })
            
            return marshal(groups, fields.Group)

api_rest.add_resource(GroupCollection, '/groups')
