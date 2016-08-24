import werkzeug.exceptions as http_exc

from flask import g, current_app
from flask_restful import Resource, marshal
from sqlalchemy.orm import joinedload

from cosmohub.api import db, api_rest

from .. import fields
from ..db import model
from ..db.session import transactional_session
from ..security import auth_required, PRIV_USER

class CatalogCollection(Resource):
    decorators = [auth_required(PRIV_USER)]

    def get(self):
        with transactional_session(db.session, read_only=True) as session:
            public = session.query(model.Catalog).filter_by(
                is_public = True
            )

            restricted = session.query(model.Catalog).join(
                'groups', 'users'
            ).filter(
                model.User.id == getattr(g, 'current_user')['id']
            )

            catalogs = public.union(restricted).all()

            return marshal(catalogs, fields.CATALOGS)

api_rest.add_resource(CatalogCollection, '/catalogs')

class CatalogItem(Resource):
    decorators = [auth_required(PRIV_USER)]

    def get(self, id_):
        with transactional_session(db.session, read_only=True) as session:
            catalog = session.query(model.Catalog).filter_by(
                id = id_
            ).options(
                joinedload('datasets'),
                joinedload('files')
            ).one()

            if not catalog.is_public:
                user = session.query(model.User).join(
                    'groups', 'catalogs'
                ).filter(
                    model.Catalog.id == id_,
                    model.User.id == getattr(g, 'current_user')['id'],
                ).first()

                if not user:
                    raise http_exc.Forbidden

            columns = current_app.columns.loc[catalog.relation].to_dict('records')
            data = marshal(catalog, fields.CATALOG)
            data.update({'columns' : columns})

            return data

api_rest.add_resource(CatalogItem, '/catalogs/<int:id_>')
