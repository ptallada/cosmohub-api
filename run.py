import os

from cosmohub.api import app
from werkzeug.debug import DebuggedApplication

app.wsgi_app = DebuggedApplication(app.wsgi_app, True)

if __name__ == '__main__':
    
    if os.environ.get('PYDEVD_HOST', None):
        import pydevd
        
        @app.before_first_request
        def _init_pydev():
            pydevd.settrace(host=os.environ['PYDEVD_HOST'], suspend=False)
    
    app.run(host='0.0.0.0' , port=5000, master=True, processes=1, gevent=2)
