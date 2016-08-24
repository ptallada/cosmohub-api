import logging
import werkzeug.exceptions as http_exc

from flask import g
from flask_restful import Resource, reqparse, marshal
from sqlalchemy.orm import joinedload

from cosmohub.api import db, api_rest

from .. import fields
from ..db import model
from ..db.session import transactional_session, retry_on_serializable_error
from ..security import auth_required, PRIV_USER, PRIV_FRESH_LOGIN, PRIV_RESET_PASSWORD

log = logging.getLogger(__name__)

class UserItem(Resource):
    @auth_required(PRIV_USER)
    def get(self):
        with transactional_session(db.session, read_only=True) as session:
            user = session.query(model.User).options(
                    joinedload('groups')
                ).filter_by(
                    id=getattr(g, 'current_user')['id']
                ).one()

            return marshal(user, fields.USER)

    @auth_required( (PRIV_USER & PRIV_FRESH_LOGIN) | PRIV_RESET_PASSWORD )
    def patch(self):
        @retry_on_serializable_error
        def patch_user(user_id, attrs):
            with transactional_session(db.session) as session:
                user = session.query(model.User).options(
                    joinedload('groups')
                ).filter_by(
                    id=user_id
                ).one()

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

                # Use the all_groups relationship
                attrs['all_groups'] = attrs['groups']
                attrs['all_groups'] = set(groups)
                del attrs['groups']

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
