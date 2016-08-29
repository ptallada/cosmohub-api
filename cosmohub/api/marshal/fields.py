from flask import current_app
from flask_restful import fields

from cosmohub.api import api_rest

class DownloadURL(fields.Raw):
    def _priv(self):
        return ['download']
    
    def format(self, value):
        from ..security.authentication import refresh_token
        from ..security.authorization import PRIV_DOWNLOAD
        
        priv = self._priv() + [value]
        
        token = refresh_token()
        token.update({ 'privs' : PRIV_DOWNLOAD(*priv).to_list()})
        
        return current_app.jwt.dumps(token)

class DownloadQueryURL(DownloadURL):
    def _priv(self):
        priv = super(DownloadQueryURL, self)._priv()
        return priv + ['query']
    
    def format(self, value):
        from ..rest.downloads import QueryDownload
        
        token = super(DownloadQueryURL, self).format(value)
        
        return api_rest.url_for(QueryDownload, id_=value, auth_token=token, _external=True)

class DownloadDatasetReadmeURL(DownloadURL):
    def _priv(self):
        priv = super(DownloadDatasetReadmeURL, self)._priv()
        return priv + ['dataset']
    
    def format(self, value):
        from ..rest.downloads import DatasetReadmeDownload
        
        token = super(DownloadDatasetReadmeURL, self).format(value)
        
        return api_rest.url_for(DatasetReadmeDownload, id_=value, auth_token=token, _external=True)

class DownloadFileReadmeURL(DownloadURL):
    def _priv(self):
        priv = super(DownloadFileReadmeURL, self)._priv()
        return priv + ['file']
    
    def format(self, value):
        from ..rest.downloads import FileReadmeDownload
        
        token = super(DownloadFileReadmeURL, self).format(value)
        
        return api_rest.url_for(FileReadmeDownload, id_=value, auth_token=token, _external=True)


class DownloadFileContentsURL(DownloadURL):
    def _priv(self):
        priv = super(DownloadFileContentsURL, self)._priv()
        return priv + ['file']
    
    def format(self, value):
        from ..rest.downloads import FileContentsDownload
        
        token = super(DownloadFileContentsURL, self).format(value)
        
        return api_rest.url_for(FileContentsDownload, id_=value, auth_token=token, _external=True)
