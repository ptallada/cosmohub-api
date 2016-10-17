import new

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
    def __init__(self, attr):
        self._attr = attr
    
    @property
    def attr(self):
        return self._attr
    
    def can(self, privilege):
        return privilege._attr.startswith(self._attr)
