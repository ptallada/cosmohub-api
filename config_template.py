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

# CosmoHub settings
SQLALCHEMY_DATABASE_URI = 'postgresql://user:password@host:port/database'
SQLALCHEMY_TRACK_MODIFICATIONS = False
DOWNLOADS_BASE_DIR = ''
RESULTS_BASE_DIR = 'cosmohub_results' # Relative to ~

# 64-byte (128 hex-chars) secret key for signing tokens and passwords
SECRET_KEY = binascii.unhexlify(
    '0000000000000000000000000000000000000000000000000000000000000000'
    '0000000000000000000000000000000000000000000000000000000000000000'
)

# Password settings
PASSLIB_CONTEXT = {
    'schemes' : [
        'pbkdf2_sha512'
    ],

    'pbkdf2_sha512__min_rounds'     :   5000,
    'pbkdf2_sha512__default_rounds' :  50000,
    'pbkdf2_sha512__max_rounds'     : 500000,
    'pbkdf2_sha512__vary_rounds'    :    0.1,
    'pbkdf2_sha512__salt_size'      :     64,
}

# WebHCat settings
WEBHCAT_BASE_URL = 'http://localhost:50111/templeton/v1/'
WEBHCAT_SCRIPT_COMMON = textwrap.dedent("""\
    # Workaround for HIVE-11607. Fixed in 1.3.0 and 2.0.0
    SET hive.exec.copyfile.maxsize=1073741824;
    """
)
WEBHCAT_SCRIPT_ROW_FORMAT = [ 'csv.bz2' ]
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
