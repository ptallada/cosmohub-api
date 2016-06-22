import werkzeug.exceptions as http_exc

from flask import g
from flask_restful import Resource, marshal

from .. import api_rest
from ... import app, db, model, fields
from ...security import auth
from ...session import transactional_session

class CatalogCollection(Resource):
    decorators = [auth.login_required]
    
    def get(self):
        with transactional_session(db.session, read_only=True) as session:
            public = session.query(model.Catalog).\
                filter_by(public = True)
            
            restricted = session.query(model.Catalog).\
                join(model.Catalog.groups).\
                join(model.Group.users).\
                filter(model.User.id == getattr(g, 'current_user')['id'])
            
            catalogs = public.union(restricted).all()
            
            return marshal(catalogs, fields.CATALOGS)

api_rest.add_resource(CatalogCollection, '/catalogs')

class CatalogItem(Resource):
    decorators = [auth.login_required]
    
    def get(self, id_):
        with transactional_session(db.session, read_only=True) as session:
            catalog = session.query(model.Catalog).filter_by(id=id_).one()
            
            if not catalog.public:
                catalog = session.query(model.Catalog).\
                    join(model.Catalog.groups).\
                    join(model.Group.users).\
                    filter(
                        model.Catalog.id == id_,
                        model.User.id == getattr(g, 'current_user')['id'],
                    ).first()
                
            if not catalog:
                raise http_exc.Forbidden
            
            columns = app.columns.loc[catalog.view].to_dict('records')
            
            data = marshal(catalog, fields.CATALOG)
            data.update({'columns' : columns})
            
            return data

api_rest.add_resource(CatalogItem, '/catalogs/<int:id_>')
