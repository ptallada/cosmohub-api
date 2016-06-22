import mimetypes
import os

from flask import g, request, Response
from flask_restful import Resource
from hdfs import InsecureClient
from sqlalchemy.orm import joinedload
from werkzeug import exceptions as http_exc
from werkzeug.datastructures import Headers

from .. import api_rest
from ... import app, db, model, streaming
from ...security import auth
from ...session import transactional_session

class BasePrebuilt(object):
    _path_attr = None
    
    def _headers(self, path):
        return Headers()
    
    def get(self, id_):
        with transactional_session(db.session, read_only=True) as session:
            prebuilt = session.query(model.Prebuilt).\
                options(joinedload(model.Prebuilt.catalog)).\
                filter_by(id=id_).one()
            
            if not prebuilt.catalog.public:
                prebuilt = session.query(model.Prebuilt).\
                join(model.Prebuilt.catalog).\
                join(model.Catalog.groups).\
                join(model.Group.users).\
                filter(
                    model.Prebuilt.id == id_,
                    model.User.id == getattr(g, 'current_user')['id'],
                ).first()
            
            if not prebuilt:
                raise http_exc.Forbidden
            
            path = getattr(prebuilt, self._path_attr)
            content_type = mimetypes.guess_type(path)[0] or 'application/octet-stream'
            headers = self._headers(path)
            http_code = 200
            
            client = InsecureClient(app.config['HADOOP_NAMENODE_URI'], user='cosmohub')
            data = streaming.HDFSPathStreamer(client, path, chunk_size=app.config['HADOOP_HDFS_CHUNK_SIZE'])
            
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

class PrebuiltDownload(BasePrebuilt, Resource):
    decorators = [auth.login_required]
    
    _path_attr = 'path_catalog'
    
    def _headers(self, path):
        headers = super(PrebuiltDownload, self)._headers(path)
        basename = os.path.basename(path)
        headers.add('Content-Disposition', 'attachment', filename=basename)
        
        return headers
    
api_rest.add_resource(PrebuiltDownload, '/prebuilts/<int:id_>/download')

class PrebuiltReadme(BasePrebuilt, Resource):
    decorators = [auth.login_required]
    
    _path_attr = 'path_readme'

api_rest.add_resource(PrebuiltReadme, '/prebuilts/<int:id_>/readme')
