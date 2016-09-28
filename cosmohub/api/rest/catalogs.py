import werkzeug.exceptions as http_exc

from flask import g, current_app
from flask_restful import Resource, marshal
from sqlalchemy.orm import joinedload

from cosmohub.api import db, api_rest

from .downloads import DatasetReadmeDownload, FileReadmeDownload, FileContentsDownload
from .. import fields
from ..database import model
from ..database.session import transactional_session
from ..security import auth_required, Privilege, Token

class CatalogCollection(Resource):
    decorators = [auth_required(Privilege(['user']))]

    def get(self):
        with transactional_session(db.session, read_only=True) as session:
            public = session.query(model.Catalog).filter_by(
                is_public = True
            )

            restricted = session.query(model.Catalog).join(
                'groups', 'allowed_users'
            ).filter(
                model.User.id == g.session['user'].id
            )

            catalogs = public.union(restricted).all()

            g.session['track']({
                't' : 'event',
                'ec' : 'catalogs',
                'ea' : 'list',
                'ev' : len(catalogs),
            })

            return marshal(catalogs, fields.CatalogCollection)

api_rest.add_resource(CatalogCollection, '/catalogs')

class CatalogItem(Resource):
    decorators = [auth_required(Privilege(['user']))]

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
                    'groups_granted', 'catalogs'
                ).filter(
                    model.Catalog.id == id_,
                    model.User.id == g.session['user'].id,
                ).first()

                if not user:
                    raise http_exc.Forbidden

            columns = current_app.columns.loc[catalog.relation].to_dict('records')
            data = marshal(catalog, fields.Catalog)
            data.update({'columns' : columns})
            
            for dataset in data['datasets']:
                token = Token(
                    g.session['user'],
                    Privilege(['download'], ['dataset'], [dataset['id']]),
                    expires_in=current_app.config['TOKEN_EXPIRES_IN']['download'],
                )
                    
                url = api_rest.url_for(DatasetReadmeDownload, id_=dataset['id'], auth_token=token.dump(), _external=True)
                dataset['download_readme'] = url
            
            for file_ in data['files']:
                token = Token(
                    g.session['user'],
                    Privilege(['download'], ['file'], [file_['id']]),
                    expires_in=current_app.config['TOKEN_EXPIRES_IN']['download'],
                )
                
                url = api_rest.url_for(FileReadmeDownload, id_=file_['id'], auth_token=token.dump(), _external=True)
                file_['download_readme'] = url
                url = api_rest.url_for(FileContentsDownload, id_=file_['id'], auth_token=token.dump(), _external=True)
                file_['download_contents'] = url
            
            g.session['track']({
                't' : 'event',
                'ec' : 'catalogs',
                'ea' : 'list',
                'el' : catalog.id,
            })
            
            return data

api_rest.add_resource(CatalogItem, '/catalogs/<int:id_>')
