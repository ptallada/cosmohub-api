from flask import current_app
from flask_restful import marshal
from itsdangerous import TimedJSONWebSignatureSerializer

from .. import fields

class Token(object):
    _serializer_class = TimedJSONWebSignatureSerializer
    
    @staticmethod
    def _secret():
        return current_app.config['SECRET_KEY']
    
    def __init__(self, user, privilege, expires_in=None):
        self._user = user
        self._privilege = privilege
        
        if not expires_in:
            expires_in = current_app.config['TOKEN_EXPIRES_IN_DEFAULT']
        
        self._serializer = self._serializer_class(
            secret_key = self._secret(),
            expires_in = expires_in,
        )
    
    def dump(self):
        token = {
            'user' : marshal(self._user, fields.UserToken),
            'privilege' : self._privilege.attr,
        }
        
        return self._serializer.dumps(token)