#! /usr/bin/env python
# -*- coding: utf-8 -*-

import os

from setuptools import setup

here = os.path.abspath(os.path.dirname(__file__))

my_globals = {}
execfile(os.path.join(here, 'cosmohub', 'api', 'release.py'), my_globals)

with open(os.path.join(here, 'README.md')) as f:
    README = f.read()
with open(os.path.join(here, 'CHANGELOG.md')) as f:
    CHANGES = f.read()

setup(
    name = 'cosmohub.api',
    version = my_globals['__version__'],
    description = 'CosmoHub API',
    long_description = README + '\n\n' + CHANGES,

    packages = ['cosmohub'],
    namespace_packages = ['cosmohub'],
    author = 'Pau Tallada Crespí',
    author_email = 'pau.tallada@gmail.com',

    install_requires = [
        'asdf',
        'astropy',
        'enum34',
        'gevent',
        'flask',
        'flask-cors',
        'flask-httpauth',
        'flask-mail',
        'flask-recaptcha',
        'flask-restful',
        'flask-uwsgi-websocket',
        'flask-sqlalchemy',
        'flask-logconfig',
        'humanize',
        'opbeat[flask]',
        'pandas',
        'passlib',
        'psycogreen',
        'psycopg2-binary',
        'pyhive[hive]',
        'pyhdfs',
        'pyparsing',
        'sasl',
        'sqlalchemy',
        'sqlalchemy-utils',
    ],
    
    include_package_data=True,
    zip_safe=False,

    entry_points = {
        'console_scripts' : [
            'cosmohub_api_initialize_db = cosmohub.api.scripts.initialize_db:main',
        ],
        'cosmohub_format' : [
            'csv.bz2 = cosmohub.api.io.format.csv_bz2:CsvBz2File',
            'fits    = cosmohub.api.io.format.fits:FitsFile',
            'asdf    = cosmohub.api.io.format.asdf:AsdfFile',
        ]
    },
)
