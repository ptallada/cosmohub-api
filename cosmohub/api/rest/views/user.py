import logging
import werkzeug.exceptions as http_exc

from flask import g
from flask_restful import Resource, reqparse, marshal
from sqlalchemy.orm import joinedload

from .. import api_rest
from ... import db, model, fields
from ...security import auth
from ...session import transactional_session, retry_on_serializable_error

log = logging.getLogger(__name__)

class UserItem(Resource):
    @auth.login_required
    def get(self):
        return getattr(g, 'current_user')
    
    @auth.login_required
    def patch(self):
        @retry_on_serializable_error
        def patch_user(user_id, attrs):
            with transactional_session(db.session) as session:
                user = session.query(model.User).\
                    option(
                        joinedload(model.User.groups)
                    ).\
                    filter_by(id=user_id).one()
                
                for key, value in attrs.iteritems():
                    setattr(user, key, value)
                
                return marshal(user, fields.USER)
        
        parser = reqparse.RequestParser()
        parser.add_argument('name',     store_missing=False)
        parser.add_argument('password', store_missing=False)
        attrs = parser.parse_args(strict=True)
        
        g.current_user = patch_user(getattr(g, 'current_user')['id'], attrs)
        
        return g.current_user
    
    def post(self):
        @retry_on_serializable_error
        def post_user(attrs):
            with transactional_session(db.session) as session:
                groups = session.query(model.Group).filter(
                    model.Group.name.in_(attrs['groups']),
                ).all()
                
                if len(groups) != len(attrs['groups']):
                    raise http_exc.BadRequest("One or more of the requested groups does not exist.")
                
                attrs['groups'] = set(groups)
                
                user = model.User(**attrs)
                session.add(user)
                session.flush()
                return marshal(user, fields.USER)
        
        parser = reqparse.RequestParser()
        parser.add_argument('name',     store_missing=False)
        parser.add_argument('email',    store_missing=False)
        parser.add_argument('password', store_missing=False)
        parser.add_argument('groups',   store_missing=False, action='append')
        
        attrs = parser.parse_args(strict=True)
        
        g.current_user = post_user(attrs)
        
        return g.current_user, 201

api_rest.add_resource(UserItem, '/user')
