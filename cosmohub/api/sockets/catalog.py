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

def _check_syntax(ws, cursor, sql):
    sql = "SELECT * FROM ( {0} ) AS t LIMIT 0".format(sql)

    try:
        start = time.time()
        cursor.execute(sql, async=False)
        finish = time.time()
        
        # col[0][2:] : Remove 't.' prefix from column names
        cols = [col[0][2:] for col in cursor.description]
        ws.send(json.dumps({
            'type' : 'syntax',
            'data' : {
                'columns' : cols,
            }
        }))
        
        g.session['track']({
            't' : 'event',
            'ec' : 'catalogs',
            'ea' : 'syntax_ok',
            'el' : g.session['user'].id,
            'ev' : int(finish-start),
        })

    except hive.OperationalError as e:
        finish = time.time()
        status = e.args[0].status
        prefix = "Error while compiling statement: FAILED: "
        if status.sqlState in ['42000', '42S02']:
            ws.send(json.dumps({
                'type' : 'syntax',
                'error' : {
                    'message' : status.errorMessage[len(prefix):],
                }
            }))
            
            g.session['track']({
                't' : 'event',
                'ec' : 'catalogs',
                'ea' : 'syntax_error',
                'el' : g.session['user'].id,
                'ev' : int(finish-start),
            })
        
        else:
            raise

class QueryCancelledException(Exception):
    pass

def _raise_if_cancelled(ws):
    msg = ws.recv_nb()
    if not msg:
        return
    
    msg = json.loads(msg)
    if msg['type'] == 'cancel':
        raise QueryCancelledException()
    
    raise ValueError('Invalid message received')

def _execute_query(ws, cursor, sql):
    try:
        sql = "SELECT * FROM ( {0} ) AS t LIMIT 10001".format(sql)
        start = time.time()
        cursor.execute(sql, async=True)
        
        _raise_if_cancelled(ws)
        
        status = cursor.poll().operationState
        # If user disconnects, stop polling and cancel query
        while ws.connected and (status not in [
            hive.ttypes.TOperationState.FINISHED_STATE,
            hive.ttypes.TOperationState.CANCELED_STATE,
            hive.ttypes.TOperationState.CLOSED_STATE,
            hive.ttypes.TOperationState.ERROR_STATE,
        ]):
            logs = cursor.fetch_logs()
            
            _raise_if_cancelled(ws)
            
            if logs:
                progress = parse_progress(logs[-1])
                ws.send(json.dumps({
                    'type' : 'progress',
                    'data' : {
                        'progress' : progress,
                    }
                }))

            status = cursor.poll().operationState

        if status != hive.ttypes.TOperationState.FINISHED_STATE:
            raise Exception('Real-time query failed to complete successfully: %s', sql)
        
        _raise_if_cancelled(ws)
        
        data = cursor.fetchall()
        
        _raise_if_cancelled(ws)
        
        finish=time.time()
        
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
            current_app.config['HIVE_HOST'],
            username='jcarrete',
            database=current_app.config['HIVE_DATABASE']
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
                
                if msg['type'] == 'syntax':
                    _check_syntax(ws, cursor, msg['data']['sql'])
                
                elif msg['type'] == 'query':
                    _execute_query(ws, cursor, msg['data']['sql'])
                
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
