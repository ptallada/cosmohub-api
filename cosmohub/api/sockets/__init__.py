import gevent
import json
import pandas as pd
import textwrap
import time
import sasl

from flask import current_app
from pyhive import hive

from . import hive_progress
from ..app import ws

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

def _execute_query(ws, cursor, running, sql):
    try:
        sql = "SELECT * FROM ( {0} ) AS t LIMIT 10000".format(sql)
        cursor.execute(sql, async=True)
        
        if not running.is_set():
            raise gevent.GreenletExit
        
        status = cursor.poll().operationState
        # If user disconnects, stop polling and cancel query
        while (not ws.closed) and (status != hive.ttypes.TOperationState.FINISHED_STATE):
            logs = cursor.fetch_logs()
            
            if logs:
                progress = hive_progress.parse_progress(logs[-1])
                ws.send(json.dumps({
                    'type' : 'progress',
                    'data' : {
                        'progress' : progress,
                    }
                }))
            
            if not running.is_set():
                raise gevent.GreenletExit
            
            status = cursor.poll().operationState
            
        # If user disconnects, stop polling and cancel query
        if ws.closed:
            raise gevent.GreenletExit
        
        data = cursor.fetchall()
        # col[0][2:] : Remove 't.' prefix from column names
        cols = [col[0][2:] for col in cursor.description]
        df = pd.DataFrame(data, columns=cols)
        
        ws.send(json.dumps({
            'type' : 'query',
            'data' : {
                'resultset' : df.to_dict('list'),
            }
        }))
    
    except:
        cursor.cancel()
    
    finally:
        running.clear()

@ws.route('/socket')
def echo_socket(ws):
    cursor = hive.connect(
        current_app.config['HIVE_HOST'],
        username='cosmohub',
        database=current_app.config['HIVE_DATABASE']
    ).cursor()
    
    running = gevent.event.Event()
    query = None
    
    while not ws.closed:
        try:
            msg = json.loads(ws.receive())
        except TypeError:
            if ws.closed:
                break
            else:
                raise
        
        if msg['type'] == 'syntax':
            if running.is_set():
                # Concurrent operations are not supported.
                break
            
            _check_syntax(ws, cursor, msg['data']['sql'])
        
        elif msg['type'] == 'query':
            if running.is_set():
                break
            else:
                running.set()
                query = gevent.spawn(_execute_query, ws, cursor, running, msg['data']['sql'])
        
        elif msg['type'] == 'cancel':
            if query and running.is_set():
                running.clear()
                query.join()
        
        else:
            break
