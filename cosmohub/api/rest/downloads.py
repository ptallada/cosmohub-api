import io
import mimetypes
import os
import werkzeug.exceptions as http_exc

from flask import g, current_app, request, Response
from flask_restful import Resource
from pyhdfs import HdfsClient
from sqlalchemy.orm import joinedload
from werkzeug.datastructures import Headers
from werkzeug.http import parse_range_header

from cosmohub.api import db, api_rest

from ..db import model
from ..db.session import transactional_session
from ..security import auth
from ..io.hdfs import HDFSPathReader

def create_content_range(range_header, length):
    if not range_header:
        return

    range_ = parse_range_header(range_header)

    if not range_:
        raise http_exc.NotAcceptable("Cannot parse 'Range' header.")

    if not range_.range_for_length(length):
        raise http_exc.RequestedRangeNotSatisfiable("Cannot satisfy requested range.")

    return range_.make_content_range(length)

def range_iter(fd, start, stop, chunk_size):
    pos = fd.seek(start)
    while pos < stop:
        chunk = fd.read(chunk_size)
        
        if pos + len(chunk) > stop:
            chunk = chunk[:stop-pos]
        
        pos += len(chunk)
        
        yield chunk

class BaseDownload(object):
    @staticmethod
    def _headers(path=None):
        return Headers()

    @staticmethod
    def _get_path(item):
        raise NotImplementedError

    def _create_client(self):
        client = HdfsClient(
            hosts = current_app.config['HADOOP_NAMENODES'],
            user_name='jcarrete'
        )
        return client

    def _build_response(self, reader, path, range_header=None):
        content_length = reader.seek(0, io.SEEK_END)
        mimetype = mimetypes.guess_type(path)
        content_type = 'application/octet-stream'
        if mimetype[0] and not mimetype[1]:
            content_type = mimetype[0]
        headers = self._headers(path)

        if range_header:
            try:
                content_range = create_content_range(range_header, content_length)
                headers.add('Content-Range', content_range.to_header())
                data = range_iter(reader, content_range.start, content_range.stop, current_app.config['HADOOP_HDFS_CHUNK_SIZE'])
                content_length = content_range.stop - content_range.start
                http_code = 206
            except IndexError:
                raise http_exc.RequestedRangeNotSatisfiable("Cannot satisfy requested range")
        else:
            data = range_iter(reader, 0, content_length, current_app.config['HADOOP_HDFS_CHUNK_SIZE'])
            http_code = 200

        response = Response(data, http_code, mimetype=content_type)
        response.content_length = content_length
        response.headers.extend(headers)

        return response

class DatasetReadmeDownload(BaseDownload, Resource):
    decorators = [auth.login_required]

    @staticmethod
    def _get_path(item):
        return os.path.join(current_app.config['DOWNLOADS_BASE_DIR'], item.path_readme)

    def get(self, id_):
        with transactional_session(db.session, read_only=True) as session:
            dataset = session.query(model.Dataset).options(
                joinedload('catalog')
            ).filter_by(
                id=id_
            ).one()

            if not dataset.catalog.is_public:
                user = session.query(model.Dataset).join(
                    'catalog', 'groups', 'users'
                ).filter(
                    model.Dataset.id == id_,
                    model.User.id == getattr(g, 'current_user')['id'],
                ).first()

            if not user:
                raise http_exc.Forbidden

            range_header = request.headers.get('Range', None)
            path = self._get_path(dataset)
            reader = HDFSPathReader(self._create_client(), path)
            
            return self._build_response(reader, path, range_header)

api_rest.add_resource(DatasetReadmeDownload, '/downloads/datasets/<int:id_>/readme')

class FileResource(Resource):
    decorators = [auth.login_required]

    def get(self, id_):
        with transactional_session(db.session, read_only=True) as session:
            file_ = session.query(model.File).filter_by(
                id=id_
            ).one()

            is_public = session.query(model.File).join(
                'catalogs'
            ).filter(
                model.File.id==id_,
                model.Catalog.is_public == True
            ).first()

            if not is_public:
                user = session.query(model.User).join(
                    'groups', 'catalogs', 'files'
                ).filter(
                    model.File.id == id_,
                    model.User.id == getattr(g, 'current_user')['id'],
                ).first()

            if not user:
                raise http_exc.Forbidden
            
            range_header = request.headers.get('Range', None)
            path = self._get_path(file_)
            reader = HDFSPathReader(self._create_client(), path)
            
            return self._build_response(reader, path, range_header)

class FileReadmeDownload(BaseDownload, FileResource):
    @staticmethod
    def _get_path(item):
        return os.path.join(current_app.config['DOWNLOADS_BASE_DIR'], item.path_readme)

api_rest.add_resource(FileReadmeDownload, '/downloads/files/<int:id_>/readme')

class FileContentsDownload(BaseDownload, FileResource):
    def _headers(self, path):
        headers = super(FileContentsDownload, self)._headers(path)
        headers.add('Content-Disposition', 'attachment', filename=os.path.basename(path))
        return headers

    @staticmethod
    def _get_path(item):
        return os.path.join(current_app.config['DOWNLOADS_BASE_DIR'], item.path_contents)

api_rest.add_resource(FileContentsDownload, '/downloads/files/<int:id_>/contents')

class QueryDownload(BaseDownload, Resource):
    decorators = [auth.login_required]

    def _headers(self, path):
        headers = super(QueryDownload, self)._headers(path)
        headers.add('Content-Disposition', 'attachment', filename=os.path.basename(path))
        return headers

    @staticmethod
    def _get_path(item):
        return os.path.join(current_app.config['RESULTS_BASE_DIR'], str(item.id))

    def get(self, id_):
        with transactional_session(db.session, read_only=True) as session:
            query = session.query(model.Query).filter_by(
                id=id_
            ).one()

            if model.Query.Status(query.status) != model.Query.Status.SUCCEEDED:
                raise http_exc.UnprocessableEntity('The requested query is query is not succeeded.')

            user = session.query(model.User).join(
                'queries'
            ).filter(
                model.Query.id == id_,
                model.User.id == getattr(g, 'current_user')['id'],
            ).first()

            if not user:
                raise http_exc.Forbidden

            range_header = request.headers.get('Range', None)
            client = self._create_client()
            path = self._get_path(query)
            if not path.startswith('/'):
                path = os.path.join(client.get_home_directory(), path)
                
            reader = HDFSPathReader(client, path)
            data = current_app.formats[query.format](reader, query.schema)
            path = '{path}.{ext}'.format(path=path, ext=query.format)
            
            return self._build_response(data, path, range_header)

api_rest.add_resource(QueryDownload, '/downloads/queries/<int:id_>/results')