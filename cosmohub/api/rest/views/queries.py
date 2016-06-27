from flask import g
from flask_restful import Resource, marshal, reqparse


from ... import fields
from ...app import db, api_rest
from ...db import model
from ...db.session import transactional_session, retry_on_serializable_error
from ...security import auth

class QueryCollection(Resource):
    decorators = [auth.login_required]

    def get(self):
        with transactional_session(db.session, read_only=True) as session:
            queries = session.query(model.Query).join(
                'user'
            ).filter(
                model.User.id == getattr(g, 'current_user')['id'],
            ).all()

            return marshal(queries, fields.QUERY)

    def post(self):
        @retry_on_serializable_error
        def post_query(sql):
            with transactional_session(db.session) as session:
                user = session.query(model.User).filter_by(
                    id = getattr(g, 'current_user')['id']
                ).one()

                query = model.Query(
                    user = user,
                    sql = sql
                )
                session.add(query)
                session.flush()

                return marshal(query, fields.QUERY)

        parser = reqparse.RequestParser()
        parser.add_argument('sql', required=True)
        attrs = parser.parse_args(strict=True)

        query = post_query(attrs['sql'])

        return query, 201

api_rest.add_resource(QueryCollection, '/queries')
