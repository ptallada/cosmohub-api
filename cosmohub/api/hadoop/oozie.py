import pkg_resources
import requests

from flask import current_app
from requests_kerberos import HTTPKerberosAuth, OPTIONAL
from string import Template
from urllib import quote
from urlparse import urljoin
from xml.sax.saxutils import escape

class Oozie(object):
    """\
    Simple interface to Oozie REST API.
    """
    
    def __init__(self, oozie_url, database, username=None):
        """\
        Initializes a Hive interface object.
        
        :param url: base url for WebHCat service
        :type url: str
        :param username: username to authenticate with
        :type username: str
        :param database: name of the Hive database to use
        :type database: str
        """
        self._database = database
        self._jobs_url = urljoin(oozie_url, 'jobs')
        self._job_url = urljoin(oozie_url, 'job/')
        self._username = username
        self._kerberos_auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL)
    
    def submit(self, query, path, format_, callback_url=None):
        """\
        Submits a HiveQL query for execution and returns its ID.
        
        Optionally, a URL callback can be defined to be called upon job completion.
        Raises requests.exceptions.HTTPError if the script could not be submitted.
        
        :param query: Base SQL SELECT statement
        :type query: str
        :param path: Where to store the results
        :type path: str
        :param format_: Serialization method for the results on disk
        :type format_: str
        :return: HiveQL script
        :rtype: str
        :param callback_url: URL to call on job completion
        :type callback_url: str
        :return: Job unique identifier
        :rtype: str 
        """
        
        sql = current_app.config['WEBHCAT_SCRIPT_TEMPLATE'].format(
            common_config = current_app.config['WEBHCAT_SCRIPT_COMMON'],
            compression_config = format_.compression_config,
            database = self._database,
            path = path,
            row_format = format_.row_format,
            query = query
        )
        
        tpl = Template(pkg_resources.resource_string('cosmohub.resources', 'job.properties.tpl'))
        props = tpl.safe_substitute(
            oozie_path = current_app.config['OOZIE_WF_PATH'],
            name_node = current_app.config['HDFS_URL'],
            job_tracker = current_app.config['JOB_TRACKER'],
            jdbc_url = current_app.config['JDBC_URL'],
            jdbc_principal = current_app.config['JDBC_PRINCIPAL'],
            query = escape(sql),
            callback_url = callback_url,
        )
        
        headers = {'Content-Type' : 'application/xml;charset=UTF-8'}
        params = {'action':'start'}
        if self._username:
            params.update({'doAs':self._username})
        
        r = requests.post(
            self._jobs_url,
            params=params,
            data=props,
            auth=self._kerberos_auth,
            headers=headers
        )
        r.raise_for_status()
        
        return r.json()['id']
    
    def status(self, id_):
        """\
        Retrieves the status of a job.
        
        Raises requests.exceptions.HTTPError on error.
        
        :param id_: Job unique identifier
        :type id_: str
        :return: a dictionary with status info
        :rtype: dict
        """
        params = {'show':'info'}
        url = urljoin(self._job_url, quote(id_))
        
        r = requests.get(url, params=params, auth=self._kerberos_auth)
        r.raise_for_status()
        
        return r.json()
    
    def cancel(self, id_):
        """\
        Cancels the execution of a job and returns its status.
        
        Raises requests.exceptions.HTTPError on error.
        
        :param id_: Job unique identifier
        :type id_: str
        :return: a dictionary with status info
        :rtype: dict
        """
        params = {'action':'kill'}
        if self._username:
            params.update({'doAs':self._username})
        
        url = urljoin(self._job_url, quote(id_))
        r = requests.put(url, params=params, auth=self._kerberos_auth)
        r.raise_for_status()
