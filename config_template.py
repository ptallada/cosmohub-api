import binascii
import os
import textwrap

# Statement for enabling the development environment
DEBUG = True

# Define the application directory
BASE_DIR = os.path.abspath(os.path.dirname(__file__))

# Logging configuration file location
LOGCONFIG = "logging.yaml"

# Hadoop settings
HADOOP_NAMENODES = ['localhost:50070']
HADOOP_HDFS_CHUNK_SIZE = 16*1024
HADOOP_HDFS_BUFFER_SIZE = 4

# Hive database settings
HIVE_HOST = 'localhost'
HIVE_PORT = 10000
HIVE_DATABASE = 'default'
HIVE_METASTORE_URI = 'postgresql://user:password@host:port/database'
HIVE_YARN_QUEUE = 'default'

# CosmoHub settings
SQLALCHEMY_DATABASE_URI = 'postgresql://user:password@host:port/database'
SQLALCHEMY_TRACK_MODIFICATIONS = False
DOWNLOADS_BASE_DIR = ''
RESULTS_BASE_DIR = 'cosmohub_results' # Relative to ~

# 64-byte (128 hex-chars) secret key for signing tokens and cookies
# Change this to invalidate all sessions and tokens
SECRET_KEY = binascii.unhexlify(
    '0000000000000000000000000000000000000000000000000000000000000000'
    '0000000000000000000000000000000000000000000000000000000000000000'
)
# Token expiration in seconds
TOKEN_EXPIRE_IN = 3600

# Password settings
PASSLIB_CONTEXT = {
    'schemes' : [
        'pbkdf2_sha512'
    ],

    'pbkdf2_sha512__min_rounds'     :   5000, # Takes 0.01 s
    'pbkdf2_sha512__default_rounds' :  50000, # Takes 0.1 s
    'pbkdf2_sha512__max_rounds'     : 500000, # Takes 1 s
    'pbkdf2_sha512__vary_rounds'    :    0.1,
    'pbkdf2_sha512__salt_size'      :     64,
}

# WebHCat settings
WEBHCAT_BASE_URL = 'http://localhost:50111/templeton/v1/'
WEBHCAT_SCRIPT_COMMON = textwrap.dedent("""\
    SET hive.exec.copyfile.maxsize=1073741824;
    """
)
WEBHCAT_SCRIPT_TEMPLATE = textwrap.dedent("""\
    {common_config}
    {compression_config}
    USE {database};
    INSERT OVERWRITE DIRECTORY '{path}'
    {row_format}
    {query}
    ;
    """
)

MAIL_SERVER = 'localhost'
MAIL_PORT = 25
MAIL_USE_TLS = False
MAIL_USE_SSL = False
MAIL_USERNAME = None
MAIL_PASSWORD = None
MAIL_DEFAULT_SENDER = 'CosmoHub <cosmohub@pic.es>'

REDIRECT_EMAIL_CONFIRMATION_OK = 'http://cosmohub.pic.es/'