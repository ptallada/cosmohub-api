import gevent
import json

from flask import g, current_app

from cosmohub.api import db, ws

from ..db import model
from ..db.session import transactional_session
from ..security.authentication import verify_token
from ..utils import webhcat

@ws.route('/sockets/queries')
def echo_socket(ws):
    hive_rest = webhcat.Hive(
        url = current_app.config['WEBHCAT_BASE_URL'],
        username = 'jcarrete',
        database = current_app.config['HIVE_DATABASE']
    )
    
    try:
        msg = json.loads(ws.receive())
    
        # Do not proceed if there is not valid token
        if not msg['type'] == 'auth' or not verify_token(msg['data']):
            return
    
        while not ws.closed:
            with transactional_session(db.session) as session:
                queries = session.query(model.Query).filter_by(
                    user_id = getattr(g, 'current_user')['id'],
                    status = 'PROCESSING'
                ).order_by(
                    model.Query.ts_submitted
                )
            
            for query in queries:
                status = hive_rest.status(query.job_id)
                progress = status["percentComplete"]
                percent = int(progress[:progress.index('%')])
                
                ws.send(json.dumps({
                    'type' : 'progress',
                    'data' : {
                        query.id : percent,
                    }
                }))
            
            # FIXME: Remove
            import random
            ws.send(json.dumps({
                'type' : 'progress',
                'data' : {
                    42 : random.randint(0, 100),
                }
            }))
            
            gevent.sleep(1)
    
    except TypeError:
        if ws.closed:
            return
        else:
            raise
