from cosmohub.api import app
from werkzeug.debug import DebuggedApplication

app.wsgi_app = DebuggedApplication(app.wsgi_app, True)

if __name__ == '__main__':
    app.run(host='0.0.0.0' , port=5001, master=True, processes=1)
