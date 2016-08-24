from flask import current_app
from flask_restful import fields

from cosmohub.api import api_rest

class DownloadQuery(fields.Raw):
    def format(self, value):
        from ..security.authentication import refresh_token
        from ..security.authorization import PRIV_QUERY_DOWNLOAD
        from ..rest.downloads import QueryDownload
        
        token = refresh_token()
        token.update({ 'privs' : PRIV_QUERY_DOWNLOAD(value).to_list()})
        token = current_app.jwt.dumps(token)
        return api_rest.url_for(QueryDownload, id_=value, auth_token=token, _external=True)