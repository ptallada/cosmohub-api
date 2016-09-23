# -*- coding: utf-8 -*-

import contextlib
import logging

from functools import wraps
from sqlalchemy.exc import DBAPIError

log = logging.getLogger(__name__)

def is_serializable_error(exc):
    """\
    Return True if the provided exception is a PostgreSQL serialization error.
    """
    if isinstance(exc, DBAPIError):
        if hasattr(exc.orig, 'pgcode'):
            return exc.orig.pgcode == '40001'

    return False

def retry_on_serializable_error(fn):
    """\
    Decorator that retries a call if it fails with a serialization error.
    """
    @wraps(fn)
    def wrapper(*args, **kwargs):
        while True:
            try:
                value = fn(*args, **kwargs)
                return value
            except DBAPIError as e:
                if not is_serializable_error(e):
                    raise
                log.debug("Retrying call to «%s» due to serialization error." % fn)
    return wrapper

# https://gist.github.com/obeattie/210032
@contextlib.contextmanager
def transactional_session(session, read_only=False):
    """\
    Context manager which provides transaction management for the nested block.
    A transaction is started when the block is entered, and then either
    committed if the block exits without incident, or rolled back if an error is
    raised.
    """
    try:
        if read_only:
            session.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE")
        yield session
    
    except:
        # Roll back if the nested block raised an error
        session.rollback()
        raise
    
    else:
        session.commit()
    
    finally:
        session.expunge_all()
