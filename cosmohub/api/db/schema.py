import textwrap

from flask_sqlalchemy import _BoundDeclarativeMeta
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import schema

class Table(schema.Table):
    def __init__(self, *args, **kwargs):
        doc = kwargs.pop('comment', None)
        super(Table,self).__init__(*args, **kwargs)
        self.__doc__ = doc

class Column(schema.Column):
    def __init__(self, *args, **kwargs):
        doc = kwargs.pop('comment', None)
        super(Column,self).__init__(*args, **kwargs)
        self.__doc__ = doc

class MetaData(schema.MetaData):
    def create_all(self, bind=None, *args, **kwargs):
        super(MetaData, self).create_all(bind, *args, **kwargs)

        if bind is None:
            bind = schema._bind_or_error(self)

        session = sessionmaker(bind)()
        for t in self.sorted_tables:
            if t.__doc__:
                session.execute("COMMENT ON TABLE \"%s\" IS '%s'"
                    % (t.name, t.__doc__.replace("'", "''").strip()))
            for c in t.columns:
                if c.__doc__:
                    session.execute("COMMENT ON COLUMN \"%s\".\"%s\" IS '%s'"
                        % (t.name, c.name, c.__doc__.replace("'", "''").strip()))
        session.commit()

class DeclarativeMeta(_BoundDeclarativeMeta):
    def __init__(cls, classname, bases, dict_): # @NoSelf
        if hasattr(cls, '__table__') and cls.__table__.__doc__:
            cls.__doc__ = textwrap.dedent(cls.__table__.__doc__)
        ret = _BoundDeclarativeMeta.__init__(cls, classname, bases, dict_)
        if hasattr(cls, '__table__') and cls.__doc__:
            cls.__table__.__doc__ = textwrap.dedent(cls.__doc__)
        return ret
