import humanize
import io
import logging
import os
import urlparse

from datetime import datetime, timedelta
from flask import g, current_app, render_template, render_template_string
from flask_restful import Resource, marshal, reqparse
from hdfs.ext.kerberos import KerberosClient
from pyhive import hive
from sqlalchemy.orm import undefer_group
from werkzeug import exceptions as http_exc 

from cosmohub.api import db, api_rest, mail

from .downloads import QueryDownload
from .. import fields
from ..database import model
from ..database.session import transactional_session, retry_on_serializable_error
from ..hadoop.hdfs import HDFSPathReader, HDFSParquetReader
from ..security import auth_required, Privilege, Token
from ..hadoop import oozie

log = logging.getLogger(__name__)

class QueryCollection(Resource):
    decorators = [auth_required(Privilege('/user'))]

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
                    Privilege('/download/query/{0}'.format(query['id'])),
                    expires_in=current_app.config['TOKEN_EXPIRES_IN']['download'],
                )
                    
                url = api_rest.url_for(QueryDownload, id_=query['id'], auth_token=token.dump(), _external=True)
                query['download_results'] = url
            
            g.session['track']({
                't' : 'event',
                'ec' : 'queries',
                'ea' : 'list',
                'ev' : len(data),
            })
            
            return data

    def post(self):
        @retry_on_serializable_error
        def post_query(sql, format_):
            oozie_rest = oozie.Oozie(
                current_app.config['OOZIE_URL'],
                database = current_app.config['HIVE_DATABASE'],
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
                    
                    url = urlparse.urlparse(
                        api_rest.url_for(QueryDone, id_=query.id, _external=True)
                    )._replace(
                        scheme='http',
                        netloc=current_app.config['WEBHCAT_CALLBACK_NETLOC'],
                        query='status=$status',
                    ).geturl()
                    
                    query.job_id = oozie_rest.submit(
                        query = sql,
                        path = os.path.join(current_app.config['RESULTS_BASE_DIR'], str(query.id)),
                        format_ = format_,
                        callback_url = url,
                    )
                    
                    g.session['track']({
                        't' : 'event',
                        'ec' : 'queries',
                        'ea' : 'requested',
                        'el' : query.format,
                    })
                    
                    return marshal(query, fields.Query)
            
            except:
                try:
                    if query and query.job_id:
                        oozie_rest.cancel(query.job_id)
                except:
                    pass
                finally:
                    raise
        
        parser = reqparse.RequestParser()
        parser.add_argument('sql', required=True)
        parser.add_argument('format', required=True)
        
        attrs = parser.parse_args(strict=True)
        
        return post_query(attrs['sql'], attrs['format']), 201

api_rest.add_resource(QueryCollection, '/queries')

def finish_query(query, status):
    cursor=hive.connect(
        host=current_app.config['HIVE_HOST'],
        port=current_app.config['HIVE_PORT'],
        database=current_app.config['HIVE_DATABASE'],
        auth='KERBEROS',
        kerberos_service_name='hive',
    ).cursor()
    
    
    query.status = model.Query.Status[status['status']].value
    
    # Update query columns upon completion
    if status['startTime']:
        query.ts_started = datetime.strptime(status['startTime'], '%a, %d %b %Y %H:%M:%S %Z')
    else:
        query.ts_started = datetime.strptime(status['createdTime'], '%a, %d %b %Y %H:%M:%S %Z')
    query.ts_finished = datetime.strptime(status['endTime'], '%a, %d %b %Y %H:%M:%S %Z')
    
    if query.status != model.Query.Status.SUCCEEDED.value: # @UndefinedVariable
        return
    
    # Retrieve and store query schema
    sql = "SELECT * FROM ( {0} ) AS t LIMIT 0".format(query.sql)
    cursor.execute(sql, async=False)
    query.schema = [
        [f[0][2:], f[1], f[2], f[3], f[4], f[5], f[6]]
        for f in cursor.description
    ]
    
    url = ';'.join(['http://'+e for e in current_app.config['HADOOP_NAMENODES']])
    client=KerberosClient(
        url=url,
        mutual_auth='OPTIONAL',
    )
    
    path = os.path.join(current_app.config['RESULTS_BASE_DIR'], str(query.id))
    if not path.startswith('/'):
        path = os.path.join(client.get_home_directory(), path)
    
    if query.format == 'parquet':
        reader = HDFSParquetReader(client, path)
    else:
        reader = HDFSPathReader(client, path)
    
    context = {
        'query' : query,
        'duration' : timedelta(seconds=int((query.ts_finished-query.ts_started).total_seconds())),
        'user' : query.user,
    }
    
    comments = render_template_string(current_app.config['QUERY_COMMENTS'], **context)
    
    data = current_app.formats[query.format](reader, query.schema, comments)
    query.size = data.seek(0, io.SEEK_END)

class QueryCancel(Resource):
    decorators = [auth_required(Privilege('/user'))]

    def post(self, id_):
        oozie_rest=oozie.Oozie(
            current_app.config['OOZIE_URL'],
            database=current_app.config['HIVE_DATABASE'],
        )
        @retry_on_serializable_error
        def cancel_query(id_):
            with transactional_session(db.session) as session:
                query = session.query(model.Query).filter_by(
                    id=id_,
                ).options(
                    undefer_group('text'),
                ).with_for_update().one()
                
                oozie_rest.cancel(query.job_id)
                status = oozie_rest.status(query.job_id)
                finish_query(query, status)
                
                g.session['track']({
                    't' : 'event',
                    'ec' : 'queries',
                    'ea' : 'cancelled',
                    'el' : query.format,
                    'ev' : int((query.ts_finished - query.ts_started).total_seconds())
                })
        
        cancel_query(id_)

api_rest.add_resource(QueryCancel, '/queries/<int:id_>/cancel')

class QueryDone(Resource):
    def get(self, id_):
        oozie_rest = oozie.Oozie(
            current_app.config['OOZIE_URL'],
            database=current_app.config['HIVE_DATABASE'],
        )
        
        with transactional_session(db.session) as session:
            query = session.query(model.Query).filter_by(
                id=id_,
            ).join(
                model.Query.user,
            ).options(
                undefer_group('text'),
            ).with_for_update().one()
            
            if model.Query.Status(query.status).is_final():
                return
            
            status = oozie_rest.status(query.job_id)
            
            if not model.Query.Status[status['status']].is_final():
                return
            
            finish_query(query, status)
            
            if query.status != model.Query.Status.SUCCEEDED.value:
                superusers = session.query(
                    model.User
                ).filter_by(
                    is_superuser=True
                ).all()
                
                recipients = [superuser.email for superuser in superusers]
                if recipients:
                    mail.send_message(
                        subject = current_app.config['MAIL_SUBJECTS']['query_failed'].format(id=query.id),
                        recipients = recipients,
                        body = render_template(
                            'mail/query_failed.txt',
                            query=query,
                            exit_code=1
                        ),
                        html = render_template(
                            'mail/query_failed.html',
                            query=query,
                            exit_code=1
                        ),
                    )
                
                return
            
            token = Token(
                query.user,
                Privilege('/download/query/{0}'.format(query.id)),
                expires_in=current_app.config['TOKEN_EXPIRES_IN']['download'],
            )
                
            url = api_rest.url_for(QueryDownload, id_=query.id, auth_token=token.dump(), _external=True)
            
            context = {
                'query' : query,
                'duration' : timedelta(seconds=int((query.ts_finished-query.ts_started).total_seconds())),
                'humanize' : humanize,
                'url' : url,
            }
            
            mail.send_message(
                subject = current_app.config['MAIL_SUBJECTS']['query_ready'].format(id=query.id),
                recipients = [query.user.email],
                body = render_template('mail/query_ready.txt', **context),
                html = render_template('mail/query_ready.html', **context),
            )
            
            g.session['track']({
                't' : 'event',
                'ec' : 'queries',
                'ea' : 'completed',
                'el' : query.format,
                'ev' : int((query.ts_finished - query.ts_started).total_seconds())
            })

api_rest.add_resource(QueryDone, '/queries/<int:id_>')
