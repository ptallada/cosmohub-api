import gevent
import json
import logging
import requests
import time

from flask import g, current_app, request
from urllib import urlencode

from cosmohub.api import app, db, ws

from .. import release
from ..database import model
from ..database.session import transactional_session
from ..security.authentication import verify_token
from ..hadoop import webhcat

log = logging.getLogger(__name__)

def _init_session():
    g.session = {
        'user' : None,
        'privilege' : None,
        'token' : None,
    }
    
    params = {
        'v' : 1,
        'tid' : app.config['GA_TRACKING_ID'],
        'ds' : 'api',
        'cid' : 'cosmohub.api {0}'.format(release.__version__),
        'uip' : request.remote_addr,
    }
    
    if request.headers.get('User-Agent', None):
        params['ua'] = request.headers.get('User-Agent')
    
    if request.referrer:
        params['dr'] = request.referrer
    
    def _send(payload):
        log.debug('Reporting hits to GA: %s', payload)
        requests.post(app.config['GA_URL'], data=payload)
    
    def _track(hit):
        payload = params.copy()
            
        if g.session['user']:
            payload['uid'] = g.session['user']['id']
            payload.update(hit)

        body = []
        for key, data in payload.iteritems():
            if isinstance(data, basestring):
                payload[key] = data.encode('utf-8')
        
        body.append(urlencode(payload))
        
        gevent.spawn(_send, "\n".join(body))
    
    g.session['track'] = _track

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
            _init_session()
            if not msg['type'] == 'auth' or not verify_token(msg['data']['token']):
                return
            
            old_set = set([])
            new_set = set([])
            while ws.connected:
                with transactional_session(db.session) as session:
                    queries = session.query(model.Query).filter_by(
                        user_id = g.session['user']['id'],
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
                    
                    g.session['track']({
                        't' : 'event',
                        'ec' : 'queries',
                        'ea' : 'progress',
                        'el' : g.session['user']['id'],
                        'ev' : len(data),
                    })
                
                old_set = new_set.copy()
                
                time.sleep(5)
                
                # check is websocket still alive
                msg = ws.recv_nb()
        
        except IOError:
            pass
        
        finally:
            log.info("Closing websocket connection")
