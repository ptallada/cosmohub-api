#!/usr/bin/env python
# -*- coding: utf-8 -*-

import enum
import textwrap

from flask import current_app
from sqlalchemy import (
    DDL,
    event,
)
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import (
    backref,
    deferred,
    relationship,
)
from sqlalchemy.orm.collections import attribute_mapped_collection
from sqlalchemy.schema import (
    ForeignKeyConstraint,
    PrimaryKeyConstraint,
    UniqueConstraint,
    Index,
)
from sqlalchemy.sql import func
from sqlalchemy.types import (
    BigInteger,
    Boolean,
    DateTime,
    Enum,
    Integer,
    String,
    Text,
)
from sqlalchemy_utils import (
    PasswordType,
    force_auto_coercion,
)

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
    
    _Password = PasswordType(
        onload=lambda: current_app.config['PASSLIB_CONTEXT']
    )
    
    # Columns
    _id = Column(
        'id',
        Integer,
        nullable=False,
        comment='User unique identifier'
    )
    name = Column(
        'name',
        String(64),
        nullable=False,
        comment='Full name (for communications)'
    )
    email = Column(
        'email',
        String(64),
        nullable=False,
        comment='E-Mail address'
    )
    is_superuser = Column(
        'is_superuser',
        Boolean,
        nullable=False,
        default='FALSE',
        comment='Has superuser privileges?'
    )
    password = deferred(
        Column(
            'password',
            _Password,
            nullable=False,
            comment='User credentials'
        )
    )
    ts_created = Column(
        'ts_created',
        DateTime,
        nullable=False,
        server_default=func.now(),
        comment='User creation timestamp'
    )
    ts_email_confirmed = Column(
        'ts_email_confirmed',
        DateTime,
        nullable=True,
        comment='Email confirmation timestamp'
    )
    ts_last_login = Column(
        'ts_last_login',
        DateTime,
        nullable=True,
        comment='Last login timestamp'
    )
    
    # Relationships
    groups_granted = relationship(
        'Group',
        secondary=lambda: ACL.__table__,
        collection_class=set,
        primaryjoin='and_(User.id==ACL._user_id, ACL.is_granted==True)',
        backref=backref(
            'users_allowed',
            collection_class=set,
            passive_deletes=True,
            viewonly=True
        )
    )
    
    groups_administered = relationship(
        'Group',
        secondary=lambda: ACL.__table__,
        collection_class=set,
        primaryjoin='and_(User.id==ACL._user_id, ACL.is_granted==True, ACL.is_admin==True)',
        backref=backref(
            'users_admins',
            collection_class=set,
            passive_deletes=True,
            viewonly=True
        )
    )
    
    @hybrid_property
    def id(self):
        return self._id
    
    def __repr__(self):
        return u"%s(id=%s, name=%s, email=%s)" % (
            self.__class__.__name__,
            repr(self._id),
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
        PrimaryKeyConstraint(
            'user_id',
            'group_id',
        ),
        # Unique keys
        UniqueConstraint(
            'group_id',
            'user_id',
        ),
        # Foreign keys
        ForeignKeyConstraint(
            ['user_id'],
            ['user.id'],
            onupdate='CASCADE',
            ondelete='CASCADE',
        ),
        ForeignKeyConstraint(
            ['group_id'],
            ['group.id'],
            onupdate='CASCADE',
            ondelete='CASCADE',
        ),
    )

    # Columns
    _user_id  = Column(
        'user_id',
        Integer,
        nullable=False,
        comment='User unique identifier'
    )
    _group_id = Column(
        'group_id',
        Integer,
        nullable=False,
        comment='Group unique identifier'
    )
    ts_requested = Column(
        'ts_requested',
        DateTime,
        nullable=False,
        server_default=func.now(),
        comment='When access to this group was requested'
    )
    is_admin = Column(
        'is_admin',
        Boolean,
        nullable=False,
        server_default='FALSE',
        comment='Can grant or deny access to other users?'
    )
    is_granted = Column(
        'is_granted',
        Boolean,
        nullable=True,
        comment='Has access to the project data?'
    )
    ts_resolved = Column(
        'ts_resolved',
        DateTime,
        nullable=True,
        comment='When access request was granted or denied'
    )
    
    # Relationships
    group = relationship(
        'Group',
        backref=backref(
            'acls',
            collection_class=attribute_mapped_collection('user'),
            passive_deletes=True
        ),
    )
    user = relationship(
        'User',
        backref=backref(
            'acls',
            collection_class=attribute_mapped_collection('group'),
            passive_deletes=True
        )
    )
    @hybrid_property
    def group_id(self):
        return self._group_id
    
    @hybrid_property
    def user_id(self):
        return self._user_id

    def __repr__(self):
        return u"%s(user_id=%s, group_id=%s, is_granted=%s)" % (
            self.__class__.__name__,
            repr(self._user_id),
            repr(self._group_id),
            repr(self.is_granted),
        )

event.listen(
    ACL.__table__,
    "after_create",
    DDL(
        textwrap.dedent("""\
            CREATE OR REPLACE FUNCTION acl__ts_resolved__before_insert_or_update()
            RETURNS TRIGGER AS $$
            BEGIN
               NEW.ts_resolved = now();
               RETURN NEW;
            END;
            $$ language 'plpgsql' VOLATILE;
            
            CREATE TRIGGER acl__ts_resolved__before_update
            BEFORE UPDATE
            ON acl
            FOR EACH ROW
            WHEN (OLD.is_granted != NEW.is_granted)
            EXECUTE PROCEDURE acl__ts_resolved__before_insert_or_update();
            
            CREATE TRIGGER acl__ts_resolved__before_insert
            BEFORE INSERT
            ON acl
            FOR EACH ROW
            WHEN (NEW.is_granted IS NOT NULL)
            EXECUTE PROCEDURE acl__ts_resolved__before_insert_or_update();
        """)
    )
)

event.listen(
    ACL.__table__,
    "before_drop",
    DDL(
        textwrap.dedent("""\
            DROP TRIGGER IF EXISTS acl__ts_resolved__before_update ON acl;
            DROP TRIGGER IF EXISTS acl__ts_resolved__before_insert ON acl;
            DROP FUNCTION IF EXISTS acl__ts_resolved__before_insert_or_update();
        """)
    )
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
    _id = Column(
        'id',
        Integer,
        nullable=False,
        comment='Group unique identifier',
    )
    name = Column(
        'name',
        String(32),
        nullable=False,
        comment='Name',
    )
    description = deferred(
        Column(
            'description',
            Text,
            nullable=False,
            comment='Short description',
        )
    )
    ts_created = Column(
        'ts_created',
        DateTime,
        nullable=False,
        server_default=func.now(),
        comment='When this Group was created',
    )

    @hybrid_property
    def id(self):
        return self._id

    def __repr__(self):
        return u"%s(id=%s, name=%s)" % (
            self.__class__.__name__,
            repr(self._id),
            repr(self.name),
        )

class GroupCatalog(db.Model):
    """\
    Many-to-many between Catalog and Group
    """
    __tablename__ = 'group_catalog'
    __table_args__ = (
        # Primary key
        PrimaryKeyConstraint(
            'group_id',
            'catalog_id'
        ),
        # Unique keys
        UniqueConstraint(
            'catalog_id',
            'group_id'
        ),
        # Foreign keys
        ForeignKeyConstraint(
            ['group_id'],
            ['group.id'],
            onupdate='CASCADE',
            ondelete='CASCADE'
        ),
        ForeignKeyConstraint(
            ['catalog_id'],
            ['catalog.id'],
            onupdate='CASCADE',
            ondelete='CASCADE'
        ),
    )

    # Columns
    _group_id = Column(
        'group_id',
        Integer,
        nullable=False,
        comment='Group unique identifier'
    )
    _catalog_id = Column(
        'catalog_id',
        Integer,
        nullable=False,
        comment='Catalog unique identifier'
    )
    ts_created = Column(
        'ts_created',
        DateTime,
        nullable=False,
        server_default=func.now(),
        comment='When this entry was created'
    )

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
        UniqueConstraint(
            'name',
            'version'
        ),
    )

    # Columns
    _id = Column(
        'id',
        Integer,
        nullable=False,
        comment='Catalog unique identifier'
    )
    name = Column(
        'name',
        String(32),
        nullable=False,
        comment='Name'
    )
    version = Column(
        'version',
        String(32),
        nullable=False,
        comment='Version'
    )
    description = Column(
        'description',
        String(256),
        nullable=False,
        comment='Short description'
    )
    summary = deferred(
        Column(
            'summary',
            Text,
            nullable=False,
            comment='Long description'
        )
    )
    relation = Column(
        'relation',
        String(32),
        nullable=False,
        comment='Relation in Hive that contains the data'
    )
    rows = Column(
        'rows',
        BigInteger,
        nullable=False,
        comment='Total number of rows'
    )
    is_public = Column(
        'is_public',
        Boolean,
        nullable=False,
        default=False,
        comment='Whether this Catalog is accessible to anyone'
    )
    is_simulated = Column(
        'is_simulated',
        Boolean,
        nullable=False,
        comment='Whether this Catalog data is simulated'
    )
    ts_released = Column(
        'ts_released',
        DateTime,
        nullable=True,
        comment='When this Catalog was released'
    )
    ts_uploaded = Column(
        'ts_uploaded',
        DateTime,
        nullable=False,
        server_default=func.now(),
        comment='When this Catalog was uploaded'
    )

    # Relationships
    groups = relationship(
        'Group',
        secondary=lambda: GroupCatalog.__table__,
        collection_class=set,
        backref=backref(
            'catalogs',
            collection_class=set,
            passive_deletes=True
        )
    )
    files = relationship(
        'File',
        secondary=lambda: VAD.__table__,
        collection_class=set,
        backref=backref(
            'catalogs',
            collection_class=set,
            passive_deletes=True
        )
    )

    @hybrid_property
    def id(self):
        return self._id

    def __repr__(self):
        return u"%s(id=%s, name=%s, version=%s)" % (
            self.__class__.__name__,
            repr(self._id),
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
        ForeignKeyConstraint(
            ['catalog_id'],
            ['catalog.id'],
            onupdate='CASCADE',
            ondelete='CASCADE'
        ),
    )

    # Columns
    _id = Column(
        'id',
        Integer,
        nullable=False,
        comment='Unique identifier'
    )
    _catalog_id = Column(
        'catalog_id',
        Integer,
        nullable=False,
        comment='Catalog unique identifier'
    )
    name = Column(
        'name',
        String(32),
        nullable=False,
        comment='Name'
    )
    version = Column(
        'version',
        String(32),
        nullable=False,
        comment='Version'
    )
    description = Column(
        'description',
        String(256),
        nullable=False,
        comment='Short description'
    )
    rows = Column(
        'rows',
        BigInteger,
        nullable=False,
        comment='Total number of rows'
    )
    recipe = Column(
        'recipe',
        JSON,
        nullable=False,
        comment='Machine-readable instructions to reconstruct this Dataset.'
    )
    path_readme = Column(
        'path_readme',
        Text,
        nullable=False,
        comment='Relative path to the README file'
    )
    ts_defined = Column(
        'ts_uploaded',
        DateTime,
        nullable=False,
        server_default=func.now(),
        comment='When this Dataset was defined'
    )

    # Relationships
    catalog = relationship(
        'Catalog',
        backref=backref(
            'datasets',
            collection_class=set,
            passive_deletes=True
        )
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
            repr(self._id),
            repr(self._catalog_id),
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
        Index(
            'ix__query__user_id__status__ts_submitted',
            'user_id',
            'status',
            'ts_submitted'
        ),
        # Foreign keys
        ForeignKeyConstraint(
            ['user_id'],
            ['user.id'],
            onupdate='CASCADE',
            ondelete='CASCADE'
        ),
    )

    class Status(enum.Enum):
        PREP      = 'PROCESSING'
        RUNNING   = 'PROCESSING'
        SUCCEEDED = 'SUCCEEDED'
        FAILED    = 'FAILED'
        KILLED    = 'CANCELLED'
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
    _id = Column(
        'id',
        Integer,
        nullable=False,
        comment='Unique identifier'
    )
    _user_id = Column(
        'user_id',
        Integer,
        nullable=False,
        comment='User unique identifier'
    )
    sql = Column(
        'sql',
        Text,
        nullable=False,
        comment='SQL statement'
    )
    format = Column(
        'format',
        String(8),
        nullable=False,
        comment='Record serialization format'
    )
    status = Column(
        'status',
        _StatusType,
        nullable=False,
        default='PROCESSING',
        comment='Status'
    )
    job_id = Column(
        'job_id',
        String(32),
        nullable=True,
        comment='Job unique identifier (external)'
    )
    schema = deferred(
        Column(
            'schema',
            JSON,
            nullable=True,
            comment='Mapping of columns and types present in this Query',
        ),
    )
    size = Column(
        'size',
        BigInteger,
        nullable=True,
        comment='Size in bytes'
    )
    ts_submitted = Column(
        'ts_submitted',
        DateTime,
        nullable=False,
        server_default=func.now(),
        comment='When this Query was submitted for execution'
    )
    ts_started = Column(
        'ts_started',
        DateTime,
        nullable=True,
        comment='When this Query execution started'
    )
    ts_finished = Column(
        'ts_finished',
        DateTime,
        nullable=True,
        comment='When this Query execution finished'
    )

    # Relationships
    user = relationship('User',
        backref=backref(
            'queries',
            collection_class=set,
            passive_deletes=True
        )
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
            repr(self._id),
            repr(self._user_id),
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
        PrimaryKeyConstraint(
            'catalog_id',
            'file_id',
        ),
        # Unique keys
        UniqueConstraint(
            'file_id',
            'catalog_id',
        ),
        # Foreign keys
        ForeignKeyConstraint(
            ['catalog_id'],
            ['catalog.id'],
            onupdate='CASCADE',
            ondelete='CASCADE',
        ),
        ForeignKeyConstraint(
            ['file_id'],
            ['file.id'],
            onupdate='CASCADE',
            ondelete='CASCADE',
        ),
    )

    # Columns
    _catalog_id = Column(
        'catalog_id',
        Integer,
        nullable=False,
        comment='Catalog unique identifier'
    )
    _file_id = Column(
        'file_id',
        Integer,
        nullable=False,
        comment='File unique identifier'
    )
    ts_created = Column(
        'ts_created',
        DateTime,
        nullable=False,
        server_default=func.now(),
        comment='When this File was associated with this Catalog'
    )

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
        UniqueConstraint(
            'name',
            'version',
        ),
    )

    # Columns
    _id = Column(
        'id',
        Integer,
        nullable=False,
        comment='File unique identifier'
    )
    name = Column(
        'name',
        String(32),
        nullable=False,
        comment='Name'
    )
    version = Column(
        'version',
        String(32),
        nullable=False,
        comment='Version'
    )
    description = Column(
        'description',
        String(256),
        nullable=False,
        comment='Short description'
    )
    size = Column(
        'size',
        BigInteger,
        nullable=False,
        comment='Size in bytes'
    )
    path_readme = Column(
        'path_readme',
        Text,
        nullable=False,
        comment='Relative path to its README file'
    )
    path_contents = Column(
        'path_contents',
        Text,
        nullable=False,
        comment='Relative path to the actual contents'
    )
    ts_uploaded = Column(
        'ts_uploaded',
        DateTime,
        nullable=False,
        server_default=func.now(),
        comment='When this File was uploaded'
    )

    @hybrid_property
    def id(self):
        return self._id

    def __repr__(self):
        return u"%s(id=%s, name=%s, version=%s)" % (
            self.__class__.__name__,
            repr(self._id),
            repr(self.name),
            repr(self.version),
        )
