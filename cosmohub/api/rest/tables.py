from .. import fields

from cosmohub.api import api_rest, db, mail
from .downloads import QueryDownload
from ..database import model
from ..database.session import transactional_session, retry_on_serializable_error
from datetime import datetime, timedelta
from flask import (current_app, g, render_template, request, render_template_string)
from flask_restful import (marshal, reqparse, Resource)

from ..hadoop import webhcat
import humanize
import io
from ..io import hdfs as hd
from jinja2 import Template
import logging

import os
from pyhdfs import HdfsClient
from pyhive import hive
from queries import QueryDone
from ..security import auth_required, Privilege, Token
from sqlalchemy.orm import undefer_group
import textwrap
import urlparse
import werkzeug.exceptions as http_exc

import bz2file


log = logging.getLogger(__name__)

class TableItem(Resource):
    
    def get(self):
        BZ2_HEADER = "\x42\x5a\x68" # "BZh",
        
        args = request.args
        hdfs_path = args['path']
        user = args['user']

        client = HdfsClient(
            hosts = current_app.config['HADOOP_NAMENODES'],
            user_name = user)
    
        f = hd.HDFSPathReader(client, hdfs_path)
        header = f.read(len(BZ2_HEADER))
        position = f.seek(0, os.SEEK_END)
        f.seek(0)
        
        if header == BZ2_HEADER:           
            f = bz2file.BZ2File(f)
        
        content = f.read(4096)
                
        print("path: " + hdfs_path + " user: " + user)

        return {
            "size" : position,
            "content" : content,
        }

    
    def post(self):

        values = request.get_json(force=True)
        
        cursor = hive.connect(
            current_app.config['HIVE_HOST'],
            username='mriera',
            database=current_app.config['HIVE_DATABASE']
        ).cursor()
        
        sql = "SHOW TABLES "  + "'" + values["table_name"] + "'".format(
        queue = current_app.config['HIVE_YARN_QUEUE']
        )
        print sql
        
        cursor.execute(sql, async=False)
        data_row = cursor.rowcount

        if data_row == 1:
            
            return {"value" : "True"}, 409
                     
        sql = Template(
            textwrap.dedent("""\
                {{ common_config }}
                USE mriera;
                CREATE TEMPORARY EXTERNAL TABLE `{{table}}_ext` (
                    {% for c in columns %}
                        `{{ c.name }}` {{ c.type }} COMMENT '{{ c.comment }}' {{ ',' if not loop.last }}
                    {% endfor %}
                )
                ROW FORMAT DELIMITED
                FIELDS TERMINATED BY '{{ delimiter }}'
                STORED AS TEXTFILE
                LOCATION '{{ path }}'
                tblproperties("skip.header.line.count"="{{skiprows}}");
                CREATE TABLE `{{table}}` (
                    {% for c in columns %}
                        `{{ c.name }}` {{ c.type }} COMMENT '{{ c.comment }}' {{ ',' if not loop.last }}
                    {% endfor %}
                )
                STORED AS ORC
                ;
                INSERT OVERWRITE TABLE {{ table }}
                SELECT * FROM {{ table }}_ext;
                """
            )
        ).render(
            columns = values["columns"], 
            table = values["table_name"],
            delimiter = values["delimiter_fields"],
            path = values["path"],
            skiprows = values["skiprows"],
            common_config = current_app.config['WEBHCAT_SCRIPT_COMMON'],
        )

        hive_rest = webhcat.Hive(
            url = current_app.config['WEBHCAT_BASE_URL'],
            username = values["user"],
        )
        
        with transactional_session(db.session) as session:
            #problema de autenticacio(resultat de consulta = user('mriera'))
            user = session.query(model.User).filter_by(id = 544).one() #FIXME
                
            query = model.Query(
                user = user,
                sql = sql,
                format = 'table',
            )
            session.add(query)
            session.flush()

            url = urlparse.urlparse(
                api_rest.url_for(QueryDone, id_=query.id, _external=True)
            )._replace(
                scheme='http',
                netloc=current_app.config['WEBHCAT_CALLBACK_NETLOC']
            ).geturl()
        
            query.job_id = hive_rest.submit(sql, url)
            
            g.session['track']({
                't' : 'event',
                'ec' : 'queries',
                'ea' : 'requested',
                'el' : query.format,
            })
            
            return query.job_id, 201
    
api_rest.add_resource(TableItem, '/upload')
