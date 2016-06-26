from gevent.monkey import patch_all
patch_all()
from psycogreen.gevent import patch_psycopg
patch_psycopg()

from .app import app, mod_rest
from .app import db # @UnusedImport

# Import submodules to register their routes
from .rest import views # @UnusedImport

app.register_blueprint(mod_rest)

# Import sockets module to register its routes
from . import sockets # @UnusedImport

if __name__ == '__main__':
    app.run()
