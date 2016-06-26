#! /usr/bin/env python
# -*- coding: utf-8 -*-

import os

from setuptools import setup

release = {}
here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, 'README.md')) as f:
    README = f.read()
with open(os.path.join(here, 'CHANGELOG.md')) as f:
    CHANGES = f.read()
with open(os.path.join(here, 'cosmohub', 'api', 'release.py')) as f:
    exec(f.read(), release)

requires = [
    'enum34',
    'gevent',
    'gevent-websocket',
    'flask',
    'flask-cors',
    'flask-httpauth',
    'flask-restful',
    'flask-sockets',
    'flask-sqlalchemy',
    'flask-logconfig',
    'hdfs',
    'pandas',
    'passlib',
    'psycogreen',
    'psycopg2',
    'pyhive[hive]',
    'sasl',
    'sqlalchemy',
    'sqlalchemy-utils',
]

setup(
    name='cosmohub.api',
    version=release['__version__'],
    description='CosmoHub REST API',
    long_description=README + '\n\n' + CHANGES,
    
    packages=['cosmohub'],
    namespace_packages = ['cosmohub'],
    install_requires=requires,
    author='Pau Tallada Crespí',
    author_email='pau.tallada@gmail.com',
    
    include_package_data=True,
    zip_safe=False,
    
    entry_points = {
        'console_scripts' : [
            'cosmohub_api_initialize_db = cosmohub.api.scripts.initialize_db:main',
        ],
    },
)
