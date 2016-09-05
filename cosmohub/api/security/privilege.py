import new

from itertools import izip_longest

class PrivilegeComparable(object):
    def can(self):
        raise NotImplementedError
    
    def __and__(self, other):
        def can(_, privilege):
            return self.can(privilege) and other.can(privilege)
        
        p = PrivilegeComparable()
        p.can = new.instancemethod(can, p, None)
        
        return p
    
    def __or__(self, other):
        def can(_, privilege):
            return self.can(privilege) or other.can(privilege)
        
        p = PrivilegeComparable()
        p.can = new.instancemethod(can, p, None)
        
        return p

class Privilege(PrivilegeComparable):
    def __init__(self, *attrs):
        self._attrs = attrs
    
    @property
    def attrs(self):
        return self._attrs
    
    def can(self, privilege):
        for pair in izip_longest(self._attrs, privilege.attrs, fillvalue=[]):
            if set(pair[0]) <= set(pair[1]):
                continue
            else:
                return False
        
        return True
