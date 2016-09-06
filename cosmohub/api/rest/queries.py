import io
import logging
import os

from datetime import datetime
from flask import g, current_app, render_template
from flask_restful import Resource, marshal, reqparse
from pyhdfs import HdfsClient
from pyhive import hive
from sqlalchemy.orm import joinedload
from werkzeug import exceptions as http_exc 

from cosmohub.api import db, api_rest, mail

from .downloads import QueryDownload
from .. import fields
from ..db import model
from ..db.session import transactional_session, retry_on_serializable_error
from ..io.hdfs import HDFSPathReader
from ..security import auth_required, Privilege, Token
from ..utils import webhcat

log = logging.getLogger(__name__)

class QueryCollection(Resource):
    decorators = [auth_required(Privilege(['user']))]

    def get(self):
        with transactional_session(db.session, read_only=True) as session:
            queries = session.query(model.Query).join(
                'user'
            ).filter(
                model.User.id == g.session['user'].id,
            ).all()
            
            data = marshal(queries, fields.Query)
            for query in data:
                token = Token(
                    g.session['user'],
                    Privilege(['download'], ['query'], [query['id']]),
                    expires_in=current_app.config['TOKEN_EXPIRES_IN']['download'],
                )
                    
                url = api_rest.url_for(QueryDownload, id_=query['id'], auth_token=token.dump(), _external=True)
                query['download_results'] = url
            
            return data

    def post(self):
        @retry_on_serializable_error
        def post_query(sql, format_):
            hive_rest = webhcat.Hive(
                url = current_app.config['WEBHCAT_BASE_URL'],
                username = 'jcarrete',
                database = current_app.config['HIVE_DATABASE']
            )
            
            try:
                with transactional_session(db.session) as session:
                    user = session.query(model.User).filter_by(
                        id=g.session['user'].id
                    ).one()
                    
                    query = model.Query(
                        user = user,
                        sql = sql,
                        format = format_,
                    )
                    session.add(query)
                    session.flush()
                    
                    try:
                        format_ = current_app.formats[format_]
                    except KeyError:
                        raise http_exc.BadRequest("Unsupported format requested.")
                    
                    query.job_id = hive_rest.submit(
                        query = sql,
                        path = os.path.join(current_app.config['RESULTS_BASE_DIR'], str(query.id)),
                        format_ = format_,
                        callback_url = api_rest.url_for(QueryCallback, id_=query.id, _external=True)
                    )
                    
                    return marshal(query, fields.Query)
            
            except:
                try:
                    if query and query.job_id:
                        hive_rest.cancel(query.job_id)
                except:
                    pass
                finally:
                    raise
        
        parser = reqparse.RequestParser()
        parser.add_argument('sql', required=True)
        parser.add_argument('format', required=False)
        
        attrs = parser.parse_args(strict=True)
        
        return post_query(attrs['sql'], attrs['format']), 201

api_rest.add_resource(QueryCollection, '/queries')

def finish_query(query, status):
    cursor=hive.connect(
        current_app.config['HIVE_HOST'],
        username='jcarrete',
        database=current_app.config['HIVE_DATABASE']
    ).cursor()
    
    client=HdfsClient(
        hosts=current_app.config['HADOOP_NAMENODES'],
        user_name='jcarrete'
    )
    
    if not model.Query.Status[status['status']['state']].is_final():
        return
    
    # Update query columns upon completion
    query.status = model.Query.Status[status['status']['state']].value
    
    query.ts_started = datetime.utcfromtimestamp(status['status']['startTime']/1000.)
    query.ts_finished = datetime.utcfromtimestamp(status['status']['finishTime']/1000.)
    exit_code = status['exitValue']
    
    if exit_code and int(exit_code) != 0:
        query.status = model.Query.Status.FAILED.value # @UndefinedVariable
        return
    
    if query.status != model.Query.Status.SUCCEEDED.value: # @UndefinedVariable
        return
    
    # Retrieve and store query schema
    sql = "SELECT * FROM ( {0} ) AS t LIMIT 0".format(query.sql)
    cursor.execute(sql, async=False)
    query.schema = [
        [f[0][2:], f[1], f[2], f[3], f[4], f[5], f[6]]
        for f in cursor.description
    ]
    
    path = os.path.join(current_app.config['RESULTS_BASE_DIR'], str(query.id))
    if not path.startswith('/'):
        path = os.path.join(client.get_home_directory(), path)
    
    reader = HDFSPathReader(client, path)
    data = current_app.formats[query.format](reader, query.schema)
    query.size = data.seek(0, io.SEEK_END)

class QueryCancel(Resource):
    decorators = [auth_required(Privilege(['user']))]

    def post(self, id_):
        hive_rest=webhcat.Hive(
            url=current_app.config['WEBHCAT_BASE_URL'],
            username='jcarrete',
            database=current_app.config['HIVE_DATABASE']
        )
        @retry_on_serializable_error
        def cancel_query(id_):
            with transactional_session(db.session) as session:
                query = session.query(model.Query).filter_by(
                    id=id_,
                ).one()
                
                status = hive_rest.cancel(query.job_id)
                
                return finish_query(query, status)
        
        cancel_query(id_)

api_rest.add_resource(QueryCancel, '/queries/<int:id_>/cancel')

class QueryCallback(Resource):
    def get(self, id_):
        hive_rest=webhcat.Hive(
            url=current_app.config['WEBHCAT_BASE_URL'],
            username='jcarrete',
            database=current_app.config['HIVE_DATABASE']
        )
        
        @retry_on_serializable_error
        def check_query(id_):
            with transactional_session(db.session) as session:
                query = session.query(model.Query).options(
                    joinedload('user')
                ).filter_by(
                    id=id_,
                ).with_for_update(of=model.Query).one()
                
                if model.Query.Status(query.status).is_final():
                    return
                
                status = hive_rest.status(query.job_id)
                
                finish_query(query, status)
                
                session.flush()
                
                token = Token(
                    query.user,
                    Privilege(['download'], ['query'], [query.id]),
                    expires_in=current_app.config['TOKEN_EXPIRES_IN']['download'],
                )
                    
                url = api_rest.url_for(QueryDownload, id_=query.id, auth_token=token.dump(), _external=True)
                
                mail.send_message(
                    subject = 'Your catalog is ready',
                    recipients = [query.user.email],
                    body = render_template('query_ready.txt',  user=query.user, url=url),
                    html = render_template('query_ready.html', user=query.user, url=url),
                )

        check_query(id_)

api_rest.add_resource(QueryCallback, '/queries/<int:id_>/callback')
