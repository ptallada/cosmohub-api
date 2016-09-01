import json
import logging
import time

from flask import g, current_app
from geventwebsocket import WebSocketError

from cosmohub.api import db, ws

from ..db import model
from ..db.session import transactional_session
from ..security.authentication import verify_token
from ..utils import webhcat

log = logging.getLogger(__name__)

@ws.route('/sockets/queries')
def echo_socket(ws):
    log.info("Opened websocket connection")
    
    hive_rest = webhcat.Hive(
        url = current_app.config['WEBHCAT_BASE_URL'],
        username = 'jcarrete',
        database = current_app.config['HIVE_DATABASE']
    )
    
    try:
        msg = json.loads(ws.receive())
    
        # Do not proceed if there is not valid token
        if not msg['type'] == 'auth' or not verify_token(msg['data']['token']):
            return
        
        old_set = set([])
        new_set = set([])
        while not ws.closed:
            with transactional_session(db.session) as session:
                queries = session.query(model.Query).filter_by(
                    user_id = getattr(g, 'current_user')['id'],
                    status = 'PROCESSING'
                ).order_by(
                    model.Query.ts_submitted
                )
            
            data = {}
            new_set.clear()
            for query in queries:
                new_set.add(query.id)
                old_set.remove(query.id)
                
                status = hive_rest.status(query.job_id)
                progress = status["percentComplete"]
                percent = int(progress[:progress.index('%')])
                
                data[query.id] = percent
            
            if old_set:
                # Some tracked query has completed.
                # Send non existing query ID to force refresh
                data[0] = 0

            # Send query progress
            ws.send(json.dumps({
                'type' : 'progress',
                'data' : data,
            }))
            
            old_set = new_set
            
            time.sleep(5)
    
    except WebSocketError:
        pass
    except TypeError:
        if not ws.closed:
            raise

    log.info("Closing websocket connection")
