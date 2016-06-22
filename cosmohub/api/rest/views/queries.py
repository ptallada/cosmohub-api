import mimetypes
import os

from flask import g, request, Response
from flask_restful import Resource, marshal, reqparse
from hdfs import InsecureClient
from sqlalchemy.orm import exc as sa_exc
from sqlalchemy.orm import undefer_group
from sqlalchemy.sql import and_
from werkzeug import exceptions as http_exc
from werkzeug.datastructures import Headers

from .. import api_rest
from ... import app, db, model, fields, streaming
from ...security import auth
from ...session import transactional_session, retry_on_serializable_error

class QueryCollection(Resource):
    decorators = [auth.login_required]
    
    def get(self):
        with transactional_session(db.session, read_only=True) as session:
            queries = session.query(model.Query).\
                join(model.Query.user).\
                options(undefer_group('yaml')).\
                filter(
                    model.User.id == getattr(g, 'current_user')['id'],
                ).all()
            
            return marshal(queries, fields.QUERY)
    
    def post(self):
        @retry_on_serializable_error
        def post_query(user_id, email, sql):
            with transactional_session(db.session) as session:
                user = session.query(model.User).\
                    filter_by(id = user_id).one()
                
                query = model.Query(name = 'cosmohub.hive_query')
                query.user = user
                
                query.set_input({
                    'email' : email,
                    'sql' : sql,
                })
                
                query.set_config({
                    'format' : 'csv',
                    #'db_url' : request.registry.settings['sqlalchemy.url'],
                    'cosmohub_activity_url' : '',
                    #'batch_size' : request.registry.settings['cosmohub.custom.config.batch_size'],
                    #'path' : request.registry.settings['cosmohub.custom.config.path'],
                    #'smtp_host' : request.registry.settings['cosmohub.custom.config.smtp_host'],
                })
                query.submit()
                session.flush()
                
                return marshal(query, fields.QUERY)
        
        parser = reqparse.RequestParser()
        parser.add_argument('sql', required=True)
        attrs = parser.parse_args(strict=True)
        
        query = post_query(
            getattr(g, 'current_user')['id'],
            getattr(g, 'current_user')['email'],
            attrs['sql']
        )
        
        return query, 201

api_rest.add_resource(QueryCollection, '/queries')

class QueryItem(Resource):
    decorators = [auth.login_required]
    
    def _headers(self, path):
        headers = Headers()
        basename = os.path.basename(path)
        headers.add('Content-Disposition', 'attachment', filename=basename)
    
    def get(self, id_):
        with transactional_session(db.session, read_only=True) as session:
            query = session.query(model.Query).\
                filter_by(id=id_).one()
            
            user = session.query(model.User).\
                select_from(model.Query).\
                join(model.UserQuery).\
                join(model.User).\
                filter(
                    model.Query.id == id_,
                    model.User.id == getattr(g, 'current_user')['id'],
                ).first()
            
            if not user:
                raise http_exc.Forbidden
            
            content_type = mimetypes.guess_type(query.path)[0] or 'application/octet-stream'
            headers = self._headers(query.path)
            http_code = 200
            
            client = InsecureClient(app.config['HADOOP_NAMENODE_URI'], user='cosmohub')
            data = streaming.HDFSPathStreamer(client, query.path, chunk_size=app.config['HADOOP_HDFS_CHUNK_SIZE'])
            
            range_header = request.headers.get('Range', None)
            if range_header:
                try:
                    content_range = streaming.ContentRange(range_header, len(data))
                    headers.add('Content-Range', content_range.to_header())
                    data = data[content_range.start:content_range.stop]
                    http_code = 206
                except IndexError:
                    raise http_exc.NotAcceptable("Cannot satisfy requested range")
            
            response = Response(data, http_code, mimetype=content_type)
            response.content_length = len(data)
            response.headers.extend(headers)
            
            return response

api_rest.add_resource(QueryItem, '/queries/<int:id_>/download')
