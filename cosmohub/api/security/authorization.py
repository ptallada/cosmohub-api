import new

from flask import g
from itertools import izip_longest

class Privilege(object):
    def __init__(self, *args):
        self._priv = args
        self._repr = str(args)
    
    def can(self, privs=None):
        if privs == None:
            privs = getattr(g, 'current_privs', set())
        
        for priv in privs:
            for pair in izip_longest(self._priv, priv.to_list()):
                if pair[0] == pair[1] and pair[0] != None:
                    return True
        
        return False
    
    def __and__(self, other):
        def can(s, privs):
            return self.can(privs) and other.can(privs)
        
        p = Privilege()
        p.can = new.instancemethod(can, p, None)
        p._repr = '({0}) & ({1})'.format(self, other)
        
        return p
    
    def __or__(self, other):
        def can(s, privs):
            return self.can(privs) or other.can(privs)
        
        p = Privilege()
        p.can = new.instancemethod(can, p, None)
        p._repr = '({0}) | ({1})'.format(self, other)
        
        return p
    
    def __repr__(self):
        return self._repr
    
    def to_list(self):
        return self._priv

PRIV_ADMIN          = Privilege('admin')
PRIV_FRESH_LOGIN    = Privilege('fresh')
PRIV_QUERY_DOWNLOAD = Privilege('query_download')
PRIV_RESET_PASSWORD = Privilege('reset_password')
PRIV_USER           = Privilege('user')
PRIV_VALIDATE_EMAIL = Privilege('validate_email')
