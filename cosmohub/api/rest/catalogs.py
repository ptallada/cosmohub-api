import logging
import time
import werkzeug.exceptions as http_exc

from flask import g, current_app, request
from flask_restful import Resource, marshal
from pyhive import hive
from sqlalchemy.sql import func
from sqlalchemy.orm import (
    joinedload,
    undefer_group,
)

from cosmohub.api import db, api_rest

from .downloads import DatasetReadmeDownload, FileReadmeDownload, FileContentsDownload
from .. import fields
from ..database import model
from ..database.session import transactional_session
from ..security import auth_required, Privilege, Token

log = logging.getLogger(__name__)


class CatalogCollection(Resource):
    decorators = [auth_required(Privilege('/user'))]

    def get(self):
        with transactional_session(db.session, read_only=True) as session:
            public = session.query(model.Catalog).filter_by(
                is_public = True
            )

            restricted = session.query(model.Catalog).join(
                'groups', 'users_allowed'
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
    decorators = [auth_required(Privilege('/user'))]

    def get(self, id_):
        with transactional_session(db.session, read_only=True) as session:
            catalog = session.query(model.Catalog).filter_by(
                id = id_
            ).options(
                joinedload('datasets'),
                joinedload('files'),
                undefer_group('text'),
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
                    Privilege('/download/dataset/{0}'.format(dataset['id'])),
                    expires_in=current_app.config['TOKEN_EXPIRES_IN']['download'],
                )
                    
                url = api_rest.url_for(DatasetReadmeDownload, id_=dataset['id'], auth_token=token.dump(), _external=True)
                dataset['download_readme'] = url
            
            for file_ in data['files']:
                token = Token(
                    g.session['user'],
                    Privilege('/download/file/{0}'.format(file_['id'])),
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


class PermissionDenied(Exception):
    pass


class CatalogSyntaxItem(Resource):
    decorators = [auth_required(Privilege('/user'))]
    
    def get(self):
        cursor = hive.connect(
            host=current_app.config['HIVE_HOST'],
            port=current_app.config['HIVE_PORT'],
            database=current_app.config['HIVE_DATABASE'],
            auth='KERBEROS',
            kerberos_service_name='hive',
        ).cursor()
        
        sql = "SELECT * FROM ( {0} ) AS t LIMIT 0".format(request.args['sql'])
        
        try:
            start = time.time()
            
            explain = "EXPLAIN EXTENDED {0}".format(sql)
            cursor.execute(explain, async=False)
            ast = [row[0].strip() for row in cursor.fetchall()]
            prefix = current_app.config['HIVE_DATABASE'] + '.'
            tables = [
                ast[i+1][len(prefix):] if ast[i+1].startswith(prefix) else ast[i+1]
                for i,x in enumerate(ast) if x == "TOK_TABNAME"
            ]
            
            with transactional_session(db.session, read_only=True) as session:
                # From the selected tables, fetch the public status and the user access privileges
                access = {
                    r.relation:r
                    for r in session.query(
                        model.Catalog.relation,
                        model.Catalog.is_public,
                        func.bool_or(model.ACL.is_granted).label('is_granted')
                    ).outerjoin(
                        model.GroupCatalog
                    ).outerjoin(
                        model.Group
                    ).outerjoin(
                        model.ACL,
                        (model.Group.id == model.ACL.group_id) & (model.ACL.user_id == g.session['user'].id)
                    ).filter(
                        model.Catalog.relation.in_(tables)
                    ).group_by(
                        model.Catalog.relation,
                        model.Catalog.is_public
                    ).all()
                }
            
            for table in tables:
                if table in access:
                    if not access[table].is_public:
                        if not access[table].is_granted:
                            log.warning(
                                'User {0} tried to access table {1} without permissions.'.format(
                                    g.session['user'].id, table
                                )
                            )
                            raise PermissionDenied
            
            # If it gets here, access is granted to the requested tables
            cursor.execute(sql, async=False)
            
            finish = time.time()
            
            # col[0][2:] : Remove 't.' prefix from column names
            cols = [
                {
                    'name' : col[0][2:],
                    'type' : col[1][:-5],
                }
                for col in cursor.description
            ]

            
            g.session['track']({
                't' : 'event',
                'ec' : 'catalogs',
                'ea' : 'syntax_ok',
                'el' : g.session['user'].id,
                'ev' : int(finish-start),
            })
            
            return {
                'type' : 'syntax',
                'data' : {
                    'columns' : cols,
                }
            }
        
        except PermissionDenied:
            finish = time.time()
            
            g.session['track']({
                't' : 'event',
                'ec' : 'catalogs',
                'ea' : 'syntax_denied',
                'el' : g.session['user'].id,
                'ev' : int(finish-start),
            })
            
            return {
                'type' : 'syntax',
                'error' : {
                    'message' : "You don't have enough privileges to access one of the tables.",
                }
            }
        
        except hive.OperationalError as e:
            finish = time.time()
            status = e.args[0].status
            prefix = "Error while compiling statement: FAILED: "
            
            if status.sqlState in ['21000', '42000', '42S02']:
                g.session['track']({
                    't' : 'event',
                    'ec' : 'catalogs',
                    'ea' : 'syntax_error',
                    'el' : g.session['user'].id,
                    'ev' : int(finish-start),
                })
                
                return {
                    'type' : 'syntax',
                    'error' : {
                        'message' : status.errorMessage[len(prefix):],
                    }
                }
            
            else:
                raise

api_rest.add_resource(CatalogSyntaxItem, '/catalogs/syntax')
