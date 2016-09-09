import json
import logging
import pandas as pd

from flask import g, current_app
from pyhive import hive

from cosmohub.api import app, ws

from ..security.authentication import verify_token
from ..utils import hive_progress

log = logging.getLogger(__name__)

def _check_syntax(ws, cursor, sql):
    sql = "SELECT * FROM ( {0} ) AS t LIMIT 0".format(sql)

    try:
        cursor.execute(sql, async=False)
        # col[0][2:] : Remove 't.' prefix from column names
        cols = [col[0][2:] for col in cursor.description]
        ws.send(json.dumps({
            'type' : 'syntax',
            'data' : {
                'columns' : cols,
            }
        }))

    except hive.OperationalError as e:
        status = e.args[0].status
        prefix = "Error while compiling statement: FAILED: "
        if status.sqlState in ['42000', '42S02']:
            ws.send(json.dumps({
                'type' : 'syntax',
                'error' : {
                    'message' : status.errorMessage[len(prefix):],
                }
            }))
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
                progress = hive_progress.parse(logs[-1])
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
        
        limited = False
        if len(data) > 10000:
            limited = True
            data = data[:10000]
        
        # col[0][2:] : Remove 't.' prefix from column names
        cols = [col[0][2:] for col in cursor.description]
        df = pd.DataFrame(data, columns=cols)
        
        _raise_if_cancelled(ws)
        
        ws.send(json.dumps({
            'type' : 'query',
            'data' : {
                'resultset' : df.to_dict('list'),
                'limited' : limited,
            }
        }))

    except:
        cursor.cancel()

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
            g.session = {}
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
                    try:
                        _execute_query(ws, cursor, msg['data']['sql'])
                    except QueryCancelledException:
                        pass
                
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
