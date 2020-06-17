import gevent
import json
import logging
import pandas as pd
import requests
import time

from flask import g, current_app, request
from pyhive import hive

from cosmohub.api import app, ws
from urllib import urlencode

from .. import release
from ..security.authentication import verify_token
from ..hadoop.hive import parse_progress
from ..io.jsonencoder import WSEncoder

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
            payload['uid'] = g.session['user'].id
            payload.update(hit)

        body = []
        for key, data in payload.iteritems():
            if isinstance(data, basestring):
                payload[key] = data.encode('utf-8')
        
        body.append(urlencode(payload))
        
        gevent.spawn(_send, "\n".join(body))
    
    g.session['track'] = _track

class QueryCancelledException(Exception):
    pass

def _raise_if_cancelled(ws):
    msg = ws.recv_nb()
    if not msg:
        return
    
    msg = json.loads(msg)
    if msg['type'] == 'ping':
        ws.send(json.dumps({
            'type' : 'pong',
        }))
    elif msg['type'] == 'cancel':
        raise QueryCancelledException()
    else:
        raise ValueError('Invalid message received')

def _execute_query(ws, cursor, sql):
    try:
        sql = "SELECT * FROM ( {0} ) AS t LIMIT 10001".format(sql)
        start = time.time()
        cursor.execute(sql, async=True)
        
        _raise_if_cancelled(ws)
        
        status = cursor.poll(True)
        # If user disconnects, stop polling and cancel query
        while ws.connected and (status.operationState not in [
            hive.ttypes.TOperationState.FINISHED_STATE,
            hive.ttypes.TOperationState.CANCELED_STATE,
            hive.ttypes.TOperationState.CLOSED_STATE,
            hive.ttypes.TOperationState.ERROR_STATE,
        ]):
            _raise_if_cancelled(ws)
            
            try:
                progress = parse_progress(status.progressUpdateResponse)
            except ValueError:
                log.warning("Cannot parse progress report: %s", status.progressUpdateResponse)
            else:
                ws.send(json.dumps({
                    'type' : 'progress',
                    'data' : {
                        'progress' : progress,
                    }
                }))
            
            
            status = cursor.poll(True)

        if status.operationState != hive.ttypes.TOperationState.FINISHED_STATE:
            raise Exception('Real-time query failed to complete successfully: %s', sql)
        
        _raise_if_cancelled(ws)
        
        data = cursor.fetchall()
        
        _raise_if_cancelled(ws)
        
        finish = time.time()
        
        limited = False
        if len(data) > 10000:
            limited = True
            data = data[:10000]
        
        # col[0][2:] : Remove 't.' prefix from column names
        cols = [col[0][2:] for col in cursor.description]
        df = pd.DataFrame(data, columns=cols)
        
        rs = [
            {'name': name, 'values': values}
            for name, values in df.iteritems()
        ]
        
        ws.send(json.dumps({
            'type' : 'query',
            'data' : {
                'resultset' : rs,
                'limited' : limited,
            }
        }, cls=WSEncoder))
        
        g.session['track']({
            't' : 'event',
            'ec' : 'catalogs',
            'ea' : 'query_completed',
            'el' : g.session['user'].id,
            'ev' : int(finish-start),
        })

    except QueryCancelledException:
        cursor.cancel()
        finish=time.time()
        
        g.session['track']({
            't' : 'event',
            'ec' : 'catalogs',
            'ea' : 'query_cancelled',
            'el' : g.session['user'].id,
            'ev' : int(finish-start),
        })

@ws.route('/sockets/catalog')
def catalog(ws):
    with app.request_context(ws.environ):
        cursor = hive.connect(
            host=current_app.config['HIVE_HOST'],
            port=current_app.config['HIVE_PORT'],
            database=current_app.config['HIVE_DATABASE'],
            auth='KERBEROS',
            kerberos_service_name='hive',
        ).cursor()
        
        sql = "SET tez.queue.name={queue}".format(
            queue = current_app.config['HIVE_YARN_QUEUE']
        )
   
        cursor.execute(sql, async=False)
        
        try:
            msg = ws.receive()
            if not msg:
                return

            msg = json.loads(msg)
            
            # Do not proceed if there is not valid token
            _init_session()
            if not msg['type'] == 'auth' or not verify_token(msg['data']['token']):
                return
            
            msg = ws.receive()
            if not msg:
                return
            
            while ws.connected:
                msg = json.loads(msg)
                
                if msg['type'] == 'query':
                    _execute_query(ws, cursor, msg['data']['sql'])
                
                elif msg['type'] == 'ping':
                    ws.send(json.dumps({
                        'type' : 'pong',
                    }))
                
                else:
                    break
                
                msg = ws.receive()
                if not msg:
                    return
        
        except IOError:
            pass
        
        finally:
            log.info("Closing websocket connection")
            cursor.close()
