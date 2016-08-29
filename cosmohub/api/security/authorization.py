import new

from flask import g
from itertools import izip_longest

class Privilege(object):
    def __init__(self, *priv):
        self._priv = list(priv)
        self._repr = str(priv)
    
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
    
    def __call__(self, *priv):
        privs = self._priv + list(priv)
        return Privilege(*privs)
    
    def __repr__(self):
        return self._repr
    
    def to_list(self):
        return self._priv

PRIV_ADMIN          = Privilege('admin')
PRIV_USER           = Privilege('user')
PRIV_FRESH_LOGIN    = Privilege('fresh')
PRIV_DOWNLOAD       = Privilege('download')
PRIV_PASSWORD_RESET = Privilege('password_reset')
PRIV_EMAIL_CONFIRM  = Privilege('email_confirm')
