from flask import g
from flask_httpauth import HTTPBasicAuth
from flask_restful import marshal
from sqlalchemy.orm import joinedload, undefer_group
from sqlalchemy.orm.exc import NoResultFound

from . import fields
from .app import db
from .db import model
from .db.session import transactional_session

auth = HTTPBasicAuth()

@auth.verify_password
def _verify_password(username, password):
    g.current_user = None

    with transactional_session(db.session, read_only=False) as session:
        try:
            user = session.query(model.User).options(
                joinedload(model.User.groups),
                undefer_group('password'),
            ).filter_by(
                email=username
            ).one()

        except NoResultFound:
            return None

        else:
            g.current_user = marshal(user, fields.USER)
            # CAUTION: Refreshing an old hash requires a writable transaction
            return user.password == password
