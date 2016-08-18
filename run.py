from cosmohub.api import app

import pydevd
pydevd.settrace(host='wl-tallada.pic.es', suspend=False)

if __name__ == '__main__':
    app.run(host='0.0.0.0' , port=5001)
