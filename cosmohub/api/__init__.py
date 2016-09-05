from gevent.monkey import patch_all
patch_all()
from psycogreen.gevent import patch_psycopg
patch_psycopg()

import logging

from flask import g, Flask, Blueprint, jsonify
from flask_logconfig import LogConfig
from flask_mail import Mail
from flask_recaptcha import ReCaptcha
from flask_restful import Api
from flask_sqlalchemy import SQLAlchemy, Model as FlaskModel
from flask_sockets import Sockets
from flask_cors import CORS
from itsdangerous import TimedJSONWebSignatureSerializer
from pkg_resources import iter_entry_points # @UnresolvedImport
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext import declarative
from sqlalchemy.orm.exc import NoResultFound

from .db import naming, schema, reflection

log = logging.getLogger(__name__)

# Create and configure application
app = Flask(__name__)
app.config.from_object('config')

# Configure logging
logconfig = LogConfig(app)

# Enable CORS
CORS(app, expose_headers=['X-Token'])

# Configure mail service
mail = Mail(app)

# Configure ReCaptcha service
recaptcha = ReCaptcha(app)

# Configure SQLAlchemy extension
metadata = schema.MetaData()
metadata.naming_convention = naming.naming_convention

db = SQLAlchemy(app, metadata=metadata)
db.engine.pool._use_threadlocal = True

db.Model = declarative.declarative_base(
    cls=FlaskModel, name='Model', metadata=metadata,
    metaclass=schema.DeclarativeMeta
)

# Load set of columns and comments for each catalog
app.columns = reflection.reflect_catalog_columns(
    metastore_uri=app.config['HIVE_METASTORE_URI'],
    database=app.config['HIVE_DATABASE'],
)

# Load available formats
app.formats = {
    entry.name : entry.load()
    for entry in iter_entry_points(group='cosmohub_format')
}

# Set up token signer
app.jwt = TimedJSONWebSignatureSerializer(app.config['SECRET_KEY'])

@app.before_request
def _clear_session():
    g.session = {
        'user' : None,
        'privilege' : None,
        'token' : None,
    }

# Add/refresh token to every authenticated request 
from .security import Token, Privilege
# FIXME: refactor authentication code to remove cicle import on db
@app.after_request
def _refresh_token(response):
    if g.session['privilege'] and Privilege(['user']).can(g.session['privilege']):
        response.headers['X-Token'] = Token(g.session['user'], g.session['privilege']).dump()
    else:
        response.headers['X-Token'] = g.session['token']
    
    return response

# Configure REST API Blueprint
mod_rest = Blueprint('rest', __name__, url_prefix='/rest')
api_rest = Api(mod_rest)

# Configure WebSocket extension
ws = Sockets(app)

# Setup global error handlers
@app.errorhandler(NoResultFound)
def _handle_noresultfound(e):
    log.info('A NoResultFound Exception has been captured', exc_info=True)

    return jsonify(
        message = 'One of the requested entities was not found.'
    ), 404

@app.errorhandler(IntegrityError)
def _handle_integrityerror(e):
    log.info('An IntegrityError Exception has been captured', exc_info=True)

    return jsonify(
        message = 'Some conflict occured while processing the request.'
    ), 409

# Import submodules to register their routes
from . import rest # @UnusedImport
app.register_blueprint(mod_rest)

# Import sockets module to register its routes
from . import sockets # @UnusedImport
