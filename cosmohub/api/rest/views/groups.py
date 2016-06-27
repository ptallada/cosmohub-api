from flask_restful import Resource

from ...app import db, api_rest
from ...db import model
from ...db.session import transactional_session

class GroupCollection(Resource):
    def get(self):
        with transactional_session(db.session, read_only=True) as session:
            groups = session.query(model.Group.name).all()

            return [group.name for group in groups]

api_rest.add_resource(GroupCollection, '/groups')
