from flask import g
from flask_restful import Resource

from cosmohub.api import db, api_rest

from ..db import model
from ..db.session import transactional_session

class GroupCollection(Resource):
    def get(self):
        with transactional_session(db.session, read_only=True) as session:
            groups = session.query(model.Group.name).all()
            
            data = [group.name for group in groups]
            
            g.session['track']({
                't' : 'event',
                'ec' : 'groups',
                'ea' : 'list',
                'ev' : len(data),
            })
            
            return data

api_rest.add_resource(GroupCollection, '/groups')
