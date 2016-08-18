import requests

from flask import current_app
from requests.compat import quote, urljoin

class Hive(object):
    """\
    Simple interface to Hive WebHCat/Templeton REST API.
    """
    
    _HIVE_ENDPOINT = 'hive'
    _JOBS_ENDPOINT = 'jobs/'
    
    def __init__(self, url, username, database):
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
        self._hive_url = urljoin(url, self._HIVE_ENDPOINT)
        self._jobs_url = urljoin(url, self._JOBS_ENDPOINT)
        self._params = { 'user.name' : username }
    
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
        script = current_app.config['WEBHCAT_SCRIPT_TEMPLATE'].format(
            common_config = current_app.config['WEBHCAT_SCRIPT_COMMON'],
            compression_config = format_.compression_config,
            database = self._database,
            path = path,
            row_format = format_.row_format,
            query = query
        )
        
        data = {
            'execute' : script,
            'statusdir' : 'status',
        }
        if callback_url:
            data.update({
            'callback' : callback_url,
        })
        r = requests.post(self._hive_url, params=self._params, data=data)
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
        url = urljoin(self._jobs_url, quote(id_))
        r = requests.get(url, params=self._params)
        r.raise_for_status()
        
        return r.json()
    
    def status_multi(self, id_, limit=5):
        """\
        Retrieves the status of all jobs with id greater than the one provided.
        
        Raises requests.exceptions.HTTPError on error.
        
        :param id_: Job unique identifier
        :type id_: str
        :param limit: Maximum number of results to retrieve
        :type limit: int
        :return: a list of dictionaries with status info
        :rtype: list<dict>
        """
        params = self._params.copy()
        params.update({
            'jobid' : id_,
            'numrecords' : limit,
        })
        r = requests.get(self._jobs_url, params=params)
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
        
        url = urljoin(self._jobs_url, quote(id))
        r = requests.delete(url, params=self._params)
        r.raise_for_status()
        
        return r.json()
