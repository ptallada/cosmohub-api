from gevent.monkey import patch_all
patch_all()
from psycogreen.gevent import patch_psycopg
patch_psycopg()

import brownthrower as bt

from flask import Flask, g, jsonify
from flask_logconfig import LogConfig
from flask_sqlalchemy import SQLAlchemy
from flask_sockets import Sockets
from flask_cors import CORS
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import NoResultFound

# Create and configure application
app = Flask(__name__)
app.config.from_object('config')

# Enable CORS
CORS(app)

# Load extensions
logconfig = LogConfig(app)

metadata = bt.model.Base.metadata # @UndefinedVariable
db = SQLAlchemy(app, metadata=metadata)
db.engine.pool._use_threadlocal = True

ws = Sockets(app)

# Load set of columns and comments for each catalog
from .reflection import reflect_catalog_columns
app.columns = reflect_catalog_columns()

# Register blueprints
from .rest import mod_rest
app.register_blueprint(mod_rest)

# Import sockets module to register its routes
from . import sockets

@app.errorhandler(NoResultFound)
def _handle_noresultfound(e):
    return jsonify(
        message = 'One of the requested entities was not found.'
    ), 404

@app.errorhandler(IntegrityError)
def _handle_integrityerror(e):
        return jsonify(
        message = 'Some conflict occured while processing the request.'
    ), 409
