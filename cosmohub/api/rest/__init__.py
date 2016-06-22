from flask import Blueprint
from flask_restful import Api

mod_rest = Blueprint('rest', __name__, url_prefix='/rest')
api_rest = Api(mod_rest)

# Import submodules to register their routes
from . import views