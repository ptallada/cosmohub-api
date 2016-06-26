import mimetypes
import os
import werkzeug.exceptions as http_exc

from flask import g, current_app, request, Response
from flask_restful import Resource
from hdfs import InsecureClient
from sqlalchemy.orm import joinedload
from werkzeug.datastructures import Headers
from werkzeug.http import parse_range_header

from ...app import app, db, api_rest
from ...db import model
from ...db.session import transactional_session
from ...security import auth

def ContentRange(range_header, length):
    if not range_header:
        return
    
    range_ = parse_range_header(range_header)
    
    if not range_:
        raise http_exc.NotAcceptable("Cannot parse 'Range' header.")
    
    if not range_.range_for_length(length):
        raise http_exc.NotAcceptable("Cannot satisfy requested range.")
    
    return range_.make_content_range(length)

class HDFSPathStreamer(object):
    def __init__(self, client, path, chunk_size=512*1024, prefix=''):
        self._client = client
        self._path = path
        self._chunk_size = chunk_size
        self._prefix = prefix
        
        self._iter = None
        self._size = len(self._prefix)
        
        self._files = []
        
        status = client.status(path)
        if status['type'] == 'FILE':
            self._path = os.path.dirname(path)
            self._files.append({
                'name'   : os.path.basename(path),
                'size'   : status['length'],
                'offset' : 0,
            })
            self._size += status['length']
        else:
            for name, status in client.list(path, status=True):
                if status['type'] != 'FILE':
                    continue
                
                self._files.append({
                    'name'   : name,
                    'size'   : status['length'],
                    'offset' : 0,
                })
                self._size += status['length']
    
    def __getitem__(self, key):
        if not isinstance(key, slice):
            raise TypeError("slice object was expected.")
        
        if min(key.start, key.stop) < 0:
            raise IndexError("indexes must be positive")
        
        if key.start > key.stop:
            raise IndexError("start index must be less or equal than stop index.")
        
        if key.step:
            raise NotImplementedError("step slicing is not supported.")
        
        new_files = []
        new_size = 0
        new_prefix = self._prefix
        
        skip = key.start
        remaining = key.stop - key.start
        
        if len(self._prefix) <= skip:
            new_prefix = ''
            skip -= len(self._prefix)
        else:
            new_prefix = self._prefix[skip:skip+remaining]
            skip = 0
            remaining -= len(new_prefix)
        
        new_size += len(new_prefix)
        
        for entry in self._files:
            if entry['size'] <= skip:
                skip -= entry['size']
                continue
            
            if not remaining:
                break
            
            if skip:
                entry['offset'] += skip
                entry['size'] -= skip
                skip = 0
            
            if remaining < entry['size']:
                entry['size'] = remaining
            
            new_files.append(entry)
            
            remaining -= entry['size']
            new_size += entry['size']
        
        if remaining:
            raise IndexError("Requested range cannot be satisfied")
        
        self._prefix = new_prefix
        self._files = new_files
        self._size = new_size
        
        return self
    
    def __iter__(self):
        if self._prefix:
            yield self._prefix
        
        for entry in self._files:
            file_path = os.path.join(self._path, entry['name'])
            remaining = entry['size']
            
            with self._client.read(file_path, chunk_size=self._chunk_size, offset=entry['offset']) as reader:
                for chunk in reader:
                    if len(chunk) > remaining:
                        chunk = chunk[:remaining]
                    
                    if chunk:
                        yield chunk
                        remaining -= len(chunk)
                    
                    if not remaining:
                        break
            
            assert remaining == 0
    
    def __len__(self):
        return self._size

class BaseDownload(object):
    def _headers(self, path):
        return Headers()
    
    def _get_path(self, item):
        raise NotImplementedError
    
    def _build_response(self, path, range_header=None, prefix=''):
        content_type = mimetypes.guess_type(path)[0] or 'application/octet-stream'
        headers = self._headers(path)
        http_code = 200
        
        client = InsecureClient(app.config['HADOOP_NAMENODE_URI'], user='cosmohub')
        path = os.path.join(current_app.config['DOWNLOADS_BASE_DIR'], path)
        data = HDFSPathStreamer(client, path, chunk_size=app.config['HADOOP_HDFS_CHUNK_SIZE'], prefix=prefix)
        if range_header:
            try:
                content_range = ContentRange(range_header, len(data))
                headers.add('Content-Range', content_range.to_header())
                data = data[content_range.start:content_range.stop]
                http_code = 206
            except IndexError:
                raise http_exc.NotAcceptable("Cannot satisfy requested range")
        
        response = Response(data, http_code, mimetype=content_type)
        response.content_length = len(data)
        response.headers.extend(headers)
        
        return response

class FileBaseDownload(BaseDownload, Resource):
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
            
            path = self._get_path(file_)
            range_header = request.headers.get('Range', None)
            
            return self._build_response(path, range_header, prefix='')

class DatasetReadmeDownload(BaseDownload, Resource):
    decorators = [auth.login_required]
    
    def _get_path(self, item):
        return item.path_readme
    
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
            
            path = self._get_path(dataset)
            range_header = request.headers.get('Range', None)
            
            return self._build_response(path, range_header, prefix='')

api_rest.add_resource(DatasetReadmeDownload, '/downloads/datasets/<int:id_>/readme')

class FileReadmeDownload(FileBaseDownload):
    def _get_path(self, item):
        return item.path_readme

api_rest.add_resource(FileReadmeDownload, '/downloads/files/<int:id_>/readme')

class FileContentsDownload(FileBaseDownload):
    def _headers(self, path):
        headers = super(FileContentsDownload, self)._headers(path)
        basename = os.path.basename(path)
        headers.add('Content-Disposition', 'attachment', filename=basename)
        
        return headers
    
    def _get_path(self, item):
        return item.path_contents

api_rest.add_resource(FileContentsDownload, '/downloads/files/<int:id_>/contents')

class QueryDownload(BaseDownload, Resource):
    decorators = [auth.login_required]
    
    def _headers(self, path):
        headers = super(QueryDownload, self)._headers(path)
        basename = os.path.basename(path) + '.csv.bz2'
        headers.add('Content-Disposition', 'attachment', filename=basename)
        
        return headers
    
    def _get_path(self, item):
        return item.path_contents
    
    def get(self, id_):
        with transactional_session(db.session, read_only=True) as session:
            query = session.query(model.Query).filter_by(
                id=id_
            ).one()
            
            if model.Query.Status(query.status) != model.Query.Status.SUCCEEDED:
                raise http_exc.UnprocessableEntity('This query is not yet completed.')
            
            user = session.query(model.User).join(
                'queries'
            ).filter(
                model.Query.id == id_,
                model.User.id == getattr(g, 'current_user')['id'],
            ).first()
            
            if not user:
                raise http_exc.Forbidden
            
            path = self._get_path(query)
            range_header = request.headers.get('Range', None)
            
            return self._build_response(path, range_header, prefix='')

api_rest.add_resource(QueryDownload, '/downloads/queries/<int:id_>/results')
