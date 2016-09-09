import json
import logging
import time

from flask import g, current_app

from cosmohub.api import app, db, ws

from ..db import model
from ..db.session import transactional_session
from ..security.authentication import verify_token
from ..utils import webhcat

log = logging.getLogger(__name__)

@ws.route('/sockets/queries')
def queries(ws):
    log.info("Opened websocket connection")
    with app.request_context(ws.environ):
        hive_rest = webhcat.Hive(
            url = current_app.config['WEBHCAT_BASE_URL'],
            username = 'jcarrete',
            database = current_app.config['HIVE_DATABASE']
        )
        
        try:
            msg = json.loads(ws.receive())
            
            # Do not proceed if there is not valid token
            g.session = {}
            if not msg['type'] == 'auth' or not verify_token(msg['data']['token']):
                return
            
            old_set = set([])
            new_set = set([])
            while ws.connected:
                with transactional_session(db.session) as session:
                    queries = session.query(model.Query).filter_by(
                        user_id = g.session['user'].id,
                        status = 'PROCESSING'
                    ).order_by(
                        model.Query.ts_submitted
                    )
                
                data = {}
                new_set.clear()
                for query in queries:
                    new_set.add(query.id)
                    old_set.discard(query.id)
                    
                    status = hive_rest.status(query.job_id)
                    progress = status["percentComplete"]
                    percent = 0
                    if progress:
                        percent = int(progress[:progress.index('%')])
                    
                    data[query.id] = percent
                
                if old_set:
                    # Some tracked query has completed.
                    # Send non existing query ID to force refresh
                    data[0] = 0
                
                if data:
                    # Send query progress
                    ws.send(json.dumps({
                        'type' : 'progress',
                        'data' : data,
                    }))
                
                old_set = new_set.copy()
                
                time.sleep(5)
                
                # check is websocket still alive
                msg = ws.recv_nb()
        
        except IOError:
            pass
        
        finally:
            log.info("Closing websocket connection")
