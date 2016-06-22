from flask import g
from flask_httpauth import HTTPBasicAuth
from flask_restful import marshal
from sqlalchemy.orm import joinedload
from sqlalchemy.orm.exc import NoResultFound

from . import db, model, fields
from .session import transactional_session

auth = HTTPBasicAuth()

@auth.verify_password
def _verify_password(username, password):
    g.current_user = None
    
    with transactional_session(db.session, read_only=True) as session:
        try:
            user = session.query(model.User).\
                options(
                    joinedload(model.User.groups),
                ).filter_by(email=username).one()
        
        except NoResultFound:
            return None
        
        else:
            g.current_user = marshal(user, fields.USER)
            return user.check_password(password)
