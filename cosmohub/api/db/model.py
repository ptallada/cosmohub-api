import enum

from flask import current_app
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship, backref
from sqlalchemy.schema import ForeignKeyConstraint, PrimaryKeyConstraint, UniqueConstraint, Index
from sqlalchemy.sql import func
from sqlalchemy.types import BigInteger, Boolean, Date, DateTime, Enum, Integer, String, Text
from sqlalchemy_utils import PasswordType, force_auto_coercion

from .schema import Column

from cosmohub.api import db

# Required for PasswordType
force_auto_coercion()

class User(db.Model):
    """\
    User identities and profiles
    """
    __tablename__ = 'user'
    __table_args__ = (
        # Primary key
        PrimaryKeyConstraint('id'),
        # Unique key
        UniqueConstraint('email'),
    )

    _Password = PasswordType(onload=lambda: current_app.config['PASSLIB_CONTEXT'])

    # Columns
    _id        = Column('id',         Integer,    nullable=False,                            comment='User unique identifier')
    name       = Column('name',       String(64), nullable=False,                            comment='Full name (for metadata purposes)')
    email      = Column('email',      String(64), nullable=False,                            comment='E-Mail address')
    is_enabled = Column('is_enabled', Boolean,    nullable=False, default=False,             comment='Whether this User has login privileges')
    is_admin   = Column('is_admin',   Boolean,    nullable=False, default=False,             comment='Whether this User has admin privileges')
    password   = Column('password',   _Password,  nullable=False,                            comment='Password')
    ts_created = Column('ts_created', DateTime,   nullable=False, server_default=func.now(), comment='When this User was created')

    # Relationships
    groups = relationship('Group',
        secondary=lambda: ACL.__table__, collection_class=set,
        primaryjoin='and_(User.id==ACL._user_id, ACL.is_granted==True)',
        backref=backref('users', collection_class=set, passive_deletes=True)
    )

    all_groups = relationship('Group',
        secondary=lambda: ACL.__table__, collection_class=set,
    )

    @hybrid_property
    def id(self):
        return self._id

    def __repr__(self):
        return u"%s(id=%s, name=%s, email=%s)" % (
            self.__class__.__name__,
            repr(self.id),
            repr(self.name),
            repr(self.email),
        )

class ACL(db.Model):
    """\
    Groups to wich a User might have access to
    """
    __tablename__ = 'acl'
    __table_args__ = (
        # Primary key
        PrimaryKeyConstraint('user_id', 'group_id'),
        # Unique keys
        UniqueConstraint('group_id', 'user_id'),
        # Foreign keys
        ForeignKeyConstraint(['user_id'],  ['user.id'],  onupdate='CASCADE', ondelete='CASCADE'),
        ForeignKeyConstraint(['group_id'], ['group.id'], onupdate='CASCADE', ondelete='CASCADE'),
    )

    # Columns
    _user_id   = Column('user_id',    Integer,  nullable=False,                            comment='User unique identifier')
    _group_id  = Column('group_id',   Integer,  nullable=False,                            comment='Group unique identifier')
    ts_created = Column('ts_created', DateTime, nullable=False, server_default=func.now(), comment='When this entry was created')
    is_granted = Column('is_granted', Boolean,  nullable=False, default=False,             comment='Whether this User has access to this Group')

    def __repr__(self):
        return u"%s(user_id=%s, group_id=%s, is_granted=%s)" % (
            self.__class__.__name__,
            repr(self._user_id),
            repr(self._group_id),
            repr(self.is_granted),
        )

class Group(db.Model):
    """\
    Privilege groups
    """
    __tablename__ = 'group'
    __table_args__ = (
        # Primary key
        PrimaryKeyConstraint('id'),
        # Unique key
        UniqueConstraint('name'),
    )

    # Columns
    _id        = Column('id',         Integer,    nullable=False,                            comment='Group unique identifier')
    name       = Column('name',       String(32), nullable=False,                            comment='Name')
    ts_created = Column('ts_created', DateTime,   nullable=False, server_default=func.now(), comment='When this Group was created')

    @hybrid_property
    def id(self):
        return self._id

    def __repr__(self):
        return u"%s(id=%s, name=%s)" % (
            self.__class__.__name__,
            repr(self.id),
            repr(self.name),
        )

class GroupCatalog(db.Model):
    """\
    Many-to-many between Catalog and Group
    """
    __tablename__ = 'group_catalog'
    __table_args__ = (
        # Primary key
        PrimaryKeyConstraint('group_id', 'catalog_id'),
        # Unique keys
        UniqueConstraint('catalog_id', 'group_id'),
        # Foreign keys
        ForeignKeyConstraint(['group_id'],   ['group.id'],   onupdate='CASCADE', ondelete='CASCADE'),
        ForeignKeyConstraint(['catalog_id'], ['catalog.id'], onupdate='CASCADE', ondelete='CASCADE'),
    )

    # Columns
    _group_id   = Column('group_id',   Integer,  nullable=False,                            comment='Group unique identifier')
    _catalog_id = Column('catalog_id', Integer,  nullable=False,                            comment='Catalog unique identifier')
    ts_created  = Column('ts_created', DateTime, nullable=False, server_default=func.now(), comment='When this entry was created')

    def __repr__(self):
        return u"%s(group_id=%s, catalog_id=%s)" % (
            self.__class__.__name__,
            repr(self._group_id),
            repr(self._catalog_id),
        )

class Catalog(db.Model):
    """\
    Catalog metadata
    """
    __tablename__ = 'catalog'
    __table_args__ = (
        # Primary key
        PrimaryKeyConstraint('id'),
        # Unique keys
        UniqueConstraint('name', 'version'),
    )

    # Columns
    _id          = Column('id',           Integer,     nullable=False,                            comment='Catalog unique identifier')
    name         = Column('name',         String(32),  nullable=False,                            comment='Name')
    version      = Column('version',      String(32),  nullable=False,                            comment='Version')
    description  = Column('description',  String(256), nullable=False,                            comment='Short description')
    summary      = Column('summary',      Text,        nullable=False,                            comment='Long description')
    relation     = Column('relation',     String(32),  nullable=False,                            comment='Relation in Hive that contains the data')
    rows         = Column('rows',         BigInteger,  nullable=False,                            comment='Total number of rows')
    is_public    = Column('is_public',    Boolean,     nullable=False, default=False,             comment='Whether this Catalog is accessible to anyone')
    is_simulated = Column('is_simulated', Boolean,     nullable=False,                            comment='Whether this Catalog data is simulated')
    ts_released  = Column('ts_released',  Date,        nullable=True,                             comment='When this Catalog was released')
    ts_uploaded  = Column('ts_uploaded',  DateTime,    nullable=False, server_default=func.now(), comment='When this Catalog was uploaded')

    # Relationships
    groups = relationship('Group',
        secondary=lambda: GroupCatalog.__table__, collection_class=set,
        backref=backref('catalogs', collection_class=set, passive_deletes=True)
    )
    files = relationship('File',
        secondary=lambda: VAD.__table__, collection_class=set,
        backref=backref('catalogs', collection_class=set, passive_deletes=True)
    )

    @hybrid_property
    def id(self):
        return self._id

    def __repr__(self):
        return u"%s(id=%s, name=%s, version=%s)" % (
            self.__class__.__name__,
            repr(self.id),
            repr(self.name),
            repr(self.version),
        )

class Dataset(db.Model):
    """\
    Predefined subsets of a Catalog
    """
    __tablename__ = 'dataset'
    __table_args__ = (
        # Primary key
        PrimaryKeyConstraint('id'),
        # Foreign keys
        ForeignKeyConstraint(['catalog_id'], ['catalog.id'], onupdate='CASCADE', ondelete='CASCADE'),
    )

    # Columns
    _id         = Column('id',          Integer,     nullable=False,                            comment='Unique identifier')
    _catalog_id = Column('catalog_id',  Integer,     nullable=False,                            comment='Catalog unique identifier')
    name        = Column('name',        String(32),  nullable=False,                            comment='Name')
    version     = Column('version',     String(32),  nullable=False,                            comment='Version')
    description = Column('description', String(256), nullable=False,                            comment='Short description')
    rows        = Column('rows',        BigInteger,  nullable=False,                            comment='Total number of rows')
    recipe      = Column('recipe',      JSON,        nullable=False,                            comment='Machine-readable instructions to reconstruct this Dataset.')
    path_readme = Column('path_readme', Text,        nullable=False,                            comment='Relative path to the README file')
    ts_defined  = Column('ts_uploaded', DateTime,    nullable=False, server_default=func.now(), comment='When this Dataset was defined')

    # Relationships
    catalog = relationship('Catalog',
        backref=backref('datasets', collection_class=set, passive_deletes=True)
    )

    @hybrid_property
    def id(self):
        return self._id

    @hybrid_property
    def catalog_id(self):
        return self._catalog_id

    def __repr__(self):
        return u"%s(id=%s, catalog_id=%s, name=%s, version=%s)" % (
            self.__class__.__name__,
            repr(self.id),
            repr(self.catalog_id),
            repr(self.name),
            repr(self.version),
        )

class Query(db.Model):
    """\
    Custom subset of a Catalog
    """
    __tablename__ = 'query'
    __table_args__ = (
        # Primary key
        PrimaryKeyConstraint('id'),
        # Unique keys
        UniqueConstraint('job_id'),
        # Indexes
        Index('ix__query__user_id__status__ts_submitted', 'user_id', 'status', 'ts_submitted'),
        # Foreign keys
        ForeignKeyConstraint(['user_id'], ['user.id'], onupdate='CASCADE', ondelete='CASCADE'),
    )

    class Status(enum.Enum):
        PREP      = 'PROCESSING'
        RUNNING   = 'PROCESSING'
        SUCCEEDED = 'SUCCEEDED'
        FAILED    = 'FAILED'
        KILLED    = 'KILLED'
        UNKNOWN   = 'UNKNOWN'
        
        def is_final(self):
            return self in (
                self.SUCCEEDED,
                self.FAILED,
                self.KILLED,
                self.UNKNOWN,
            )

    _StatusType = Enum(*[s.value for s in Status], name='ty__query__status')

    # Columns
    _id               = Column('id',                Integer,     nullable=False,                            comment='Unique identifier')
    _user_id          = Column('user_id',           Integer,     nullable=False,                            comment='User unique identifier')
    sql               = Column('sql',               Text,        nullable=False,                            comment='SQL statement')
    format            = Column('format',            String(8),   nullable=False,                            comment='Record serialization format')
    status            = Column('status',            _StatusType, nullable=False, default='PROCESSING',      comment='Status')
    job_id            = Column('job_id',            String(32),  nullable=True,                             comment='Job unique identifier (external)')
    schema            = Column('schema',            JSON,        nullable=True,                             comment='Mapping of columns and types present in this Query')
    size              = Column('size',              BigInteger,  nullable=True,                             comment='Size in bytes')
    ts_submitted      = Column('ts_submitted',      DateTime,    nullable=False, server_default=func.now(), comment='When this Query was submitted for execution')
    ts_started        = Column('ts_started',        DateTime,    nullable=True,                             comment='When this Query execution started')
    ts_finished       = Column('ts_finished',       DateTime,    nullable=True,                             comment='When this Query execution finished')

    # Relationships
    user = relationship('User',
        backref=backref('queries', collection_class=set, passive_deletes=True)
    )

    @hybrid_property
    def id(self):
        return self._id

    @hybrid_property
    def user_id(self):
        return self._user_id

    def __repr__(self):
        return u"%s(id=%s, user_id=%s, job_id=%s, status=%s, sql=%s)" % (
            self.__class__.__name__,
            repr(self.id),
            repr(self.user_id),
            repr(self.job_id),
            repr(self.status),
            repr(self.sql),
        )

class VAD(db.Model):
    """\
    Many-to-many between Catalog and File
    """
    __tablename__ = 'vad'
    __table_args__ = (
        # Primary key
        PrimaryKeyConstraint('catalog_id', 'file_id'),
        # Unique keys
        UniqueConstraint('file_id', 'catalog_id'),
        # Foreign keys
        ForeignKeyConstraint(['catalog_id'], ['catalog.id'], onupdate='CASCADE', ondelete='CASCADE'),
        ForeignKeyConstraint(['file_id'],    ['file.id'],    onupdate='CASCADE', ondelete='CASCADE'),
    )

    # Columns
    _catalog_id = Column('catalog_id', Integer,  nullable=False,                            comment='Catalog unique identifier')
    _file_id    = Column('file_id',    Integer,  nullable=False,                            comment='File unique identifier')
    ts_created  = Column('ts_created', DateTime, nullable=False, server_default=func.now(), comment='When this File was associated with this Catalog')

    def __repr__(self):
        return u"%s(catalog_id=%s, file_id=%s)" % (
            self.__class__.__name__,
            repr(self._catalog_id),
            repr(self._file_id),
        )

class File(db.Model):
    """\
    Additional data related to one or more Catalogs
    """
    __tablename__ = 'file'
    __table_args__ = (
        # Primary key
        PrimaryKeyConstraint('id'),
        # Unique key
        UniqueConstraint('name', 'version'),
    )

    # Columns
    _id           = Column('id',            Integer,     nullable=False,                            comment='File unique identifier')
    name          = Column('name',          String(32),  nullable=False,                            comment='Name')
    version       = Column('version',       String(32),  nullable=False,                            comment='Version')
    description   = Column('description',   String(256), nullable=False,                            comment='Short description')
    size          = Column('size',          BigInteger,  nullable=False,                            comment='Size in bytes')
    path_readme   = Column('path_readme',   Text,        nullable=False,                            comment='Relative path to its README file')
    path_contents = Column('path_contents', Text,        nullable=False,                            comment='Relative path to the actual contents')
    ts_uploaded   = Column('ts_uploaded',   DateTime,    nullable=False, server_default=func.now(), comment='When this File was uploaded')

    @hybrid_property
    def id(self):
        return self._id

    def __repr__(self):
        return u"%s(id=%s, name=%s, version=%s)" % (
            self.__class__.__name__,
            repr(self.id),
            repr(self.name),
            repr(self.version),
        )
