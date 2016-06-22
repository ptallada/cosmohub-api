import hashlib

import brownthrower as bt

from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship, backref
from sqlalchemy.schema import Column
from sqlalchemy.schema import ForeignKeyConstraint, PrimaryKeyConstraint, UniqueConstraint
from sqlalchemy.types import BigInteger, Boolean, Date, Integer, String

from . import db

class User(db.Model):
    __tablename__ = 'user'
    __table_args__ = (
        # Primary key
        PrimaryKeyConstraint('id'),
        # Unique key
        UniqueConstraint('email'),
    )
    
    _password_salt = '82299c7d63bfbe34b89fe51d5daa9738'
    
    # Columns
    id        = Column(Integer,     nullable=False)
    email     = Column(String(40),  nullable=False)
    name      = Column(String(40),  nullable=False)
    _password = Column(String(128), nullable=False, name='password')
    enabled   = Column(Boolean,     nullable=False, default=False)
    is_admin  = Column(Boolean,     nullable=False, default=False)
    token     = Column(String(40),  nullable=True)
    
    
    # Relationships
    groups  = relationship('Group', back_populates='users', collection_class=set, secondary=lambda: ACL.__table__)
    #queries = relationship('Query', back_populates='user',  collection_class=set, secondary=lambda: UserQuery.__table__)
    
    @hybrid_property
    def password(self):
        return self._password
    
    @password.setter
    def password(self, password):
        self._password = hashlib.sha512( # @UndefinedVariable
            password + self._password_salt
        ).hexdigest() # @UndefinedVariable
    
    def check_password(self, password ):
        return self._password == hashlib.sha512( # @UndefinedVariable
            password + self._password_salt
        ).hexdigest() # @UndefinedVariable
    
    def __repr__(self):
        return u"%s(id=%s, email=%s, name=%s, enabled=%s, is_admin=%s)" % (
            self.__class__.__name__,
            repr(self.id),
            repr(self.email),
            repr(self.name),
            repr(self.enabled),
            repr(self.is_admin),
        )

class Group(db.Model):
    __tablename__ = 'group'
    __table_args__ = (
        # Primary key
        PrimaryKeyConstraint('id'),
        # Unique key
        UniqueConstraint('name'),
    )
    
    # Columns
    id   = Column(Integer,    nullable=False)
    name = Column(String(20), nullable=False)
    password = Column(String(40), nullable=False)
    
    # Relationships
    users    = relationship('User',    back_populates='groups', collection_class=set, secondary=lambda: ACL.__table__)
    catalogs = relationship('Catalog', back_populates='groups', collection_class=set, secondary=lambda: GroupCatalog.__table__)
    
    def __repr__(self):
        return u"%s(id=%s, name=%s)" % (
            self.__class__.__name__,
            repr(self.id),
            repr(self.name),
        )

class ACL(db.Model):
    __tablename__ = 'acl'
    __table_args__ = (
        # Primary key
        PrimaryKeyConstraint('user_id', 'group_id'),
        # Foreign keys
        ForeignKeyConstraint(['user_id'],  [User.id],  onupdate='CASCADE', ondelete='CASCADE'),
        ForeignKeyConstraint(['group_id'], [Group.id], onupdate='CASCADE', ondelete='CASCADE'),
    )
    
    # Columns
    user_id  = Column(Integer, nullable=False)
    group_id = Column(Integer, nullable=False)

    # Relationships
    user  = relationship('User')
    group = relationship('Group')
    
    def __repr__(self):
        return u"%s(user_id=%s, group_id=%s)" % (
            self.__class__.__name__,
            repr(self.user_id),
            repr(self.group_id),
        )

class Query(bt.Job):
    # Relationships
    user = relationship(User, backref=backref('queries', collection_class=set), secondary=lambda: UserQuery.__table__, uselist=False)
    
    @property
    def path(self):
        return self.get_output()
    
    @property
    def email(self):
        try:
            return self.get_input()['email']
        except (KeyError, AttributeError):
            return None
    
    @property
    def sql(self):
        try:
            return self.get_input()['sql']
        except (KeyError, AttributeError):
            return None
    """
    def __json__(self, request):
        return {
            'id'         : self.id,
            'super_id'   : self.super_id,
            'name'       : self.name,
            'status'     : self.status,
            'path'       : self.path,
            'email'      : self.email,
            'sql'        : self.sql,
            'created' : self.ts_created.isoformat(' ') if self.ts_created else None,
            'queued'  : self.ts_queued.isoformat(' ')  if self.ts_queued else None,
            'started' : self.ts_started.isoformat(' ') if self.ts_started else None,
            'ended'   : self.ts_ended.isoformat(' ')   if self.ts_ended else None,
        }"""
    
    def __repr__(self):
        return u"%s(id=%s, status=%s, email=%s, sql=%s)" % (
            self.__class__.__name__,
            repr(self.id),
            repr(self.status),
            repr(self.email),
            repr(self.sql),
        )

class Catalog(db.Model):
    __tablename__ = 'catalog'
    __table_args__ = (
        # Primary key
        PrimaryKeyConstraint('id'),
        # Unique keys
        UniqueConstraint('name'),
    )
    
    # Columns
    id          = Column(Integer,      nullable=False)
    name        = Column(String(20),   nullable=False)
    description = Column(String(1000), nullable=True)
    summary     = Column(String(1000), nullable=True)
    view        = Column(String(40),   nullable=True)
    version     = Column(String(10),   nullable=False)
    date        = Column(Date,         nullable=False)
    public      = Column(Boolean,      nullable=False, default=False)
    simulated   = Column(Boolean,      nullable=False, default=False)
    # Relationships
    groups    = relationship('Group',    back_populates='catalogs', collection_class=set, secondary=lambda: GroupCatalog.__table__)
    prebuilts = relationship('Prebuilt', back_populates='catalog',  collection_class=set)
    
    def __repr__(self):
        return u"%s(id=%s, name=%s,  view=%s, version=%s, date=%s)" % (
            self.__class__.__name__,
            repr(self.id),
            repr(self.name),
            repr(self.view),
            repr(self.version),
            repr(self.date),
        )

class UserQuery(db.Model):
    __tablename__ = 'query'
    __table_args__ = (
        # Primary key
        PrimaryKeyConstraint('job_id'),
        # Unique keys
        UniqueConstraint('user_id', 'job_id'),
        # Foreign keys
        ForeignKeyConstraint(['job_id'],  [Query.id], onupdate='CASCADE', ondelete='CASCADE'),
        ForeignKeyConstraint(['user_id'], [User.id],  onupdate='CASCADE', ondelete='CASCADE'),
    )
    
    # Columns
    job_id  = Column(Integer, nullable=False)
    user_id = Column(Integer, nullable=False)
    
    # Relationships
    query = relationship(Query)
    user  = relationship('User')
    
    def __repr__(self):
        return u"%s(job_id=%s, user_id=%s)" % (
            self.__class__.__name__,
            repr(self.job_id),
            repr(self.user_id),
        )

class GroupCatalog(db.Model):
    __tablename__ = 'group_catalog'
    __table_args__ = (
        # Primary key
        PrimaryKeyConstraint('group_id', 'catalog_id'),
        # Foreign keys
        ForeignKeyConstraint(['group_id'],   [Group.id],   onupdate='CASCADE', ondelete='CASCADE'),
        ForeignKeyConstraint(['catalog_id'], [Catalog.id], onupdate='CASCADE', ondelete='CASCADE'),
    )
    
    # Columns
    group_id   = Column(Integer, nullable=False)
    catalog_id = Column(Integer, nullable=False)
    
    # Relationships
    group   = relationship('Group')
    catalog = relationship('Catalog')
    
    def __repr__(self):
        return u"%s(group_id=%s, catalog_id=%s)" % (
            self.__class__.__name__,
            repr(self.group_id),
            repr(self.catalog_id),
        )

class Prebuilt(db.Model):
    __tablename__ = 'prebuilt'
    __table_args__ = (
        # Primary key
        PrimaryKeyConstraint('id'),
        # Foreign keys
        ForeignKeyConstraint(['catalog_id'], [Catalog.id], onupdate='CASCADE', ondelete='CASCADE'),
    )
    
    # Columns
    id           = Column(Integer,     nullable=False)
    catalog_id   = Column(Integer,     nullable=False)
    name         = Column(String(20),  nullable=False)
    description  = Column(String(200), nullable=True)
    size         = Column(BigInteger,  nullable=False)
    path_catalog = Column(String(200), nullable=False)
    path_readme  = Column(String(200), nullable=False)
    
    # Relationships
    catalog = relationship('Catalog', back_populates='prebuilts')
    
    def __repr__(self):
        return u"%s(id=%s, catalog_id=%s, name=%s, size=%s)" % (
            self.__class__.__name__,
            repr(self.id),
            repr(self.catalog_id),
            repr(self.name),
            repr(self.size),
        )

