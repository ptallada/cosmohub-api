from cosmohub.api import app
from werkzeug.debug import DebuggedApplication

app.wsgi_app = DebuggedApplication(app.wsgi_app, True)

if __name__ == '__main__':
    app.run(debug = True, host='0.0.0.0' , port=5000, master=True, processes=1, gevent=2)
