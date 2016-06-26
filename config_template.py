import binascii
import os

# Statement for enabling the development environment
DEBUG = True

# Define the application directory
BASE_DIR = os.path.abspath(os.path.dirname(__file__))

# CosmoHub database settings
SQLALCHEMY_DATABASE_URI = 'postgresql://user:password@host:port/database'
SQLALCHEMY_TRACK_MODIFICATIONS = False

# Hive database settings
HIVE_HOST = 'localhost'
HIVE_PORT = 10000
HIVE_DATABASE = 'default'
HIVE_METASTORE_URI = 'postgresql://user:password@host:port/database'

# Hadoop settings
HADOOP_NAMENODE_URI='http://localhost:50070'
HADOOP_HDFS_CHUNK_SIZE=16*1024
DOWNLOADS_BASE_DIR=''

# Logging configuration file location
LOGCONFIG = "logging.yaml"

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
