import logging
import os
import sys
import warnings

from flask import Flask
from flask_compress import Compress
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.pool import NullPool

logging.basicConfig(
    stream=sys.stdout,
    level=logging.DEBUG,
    format='%(thread)d: %(message)s'
)

logger = logging.getLogger("openalex-formatter")

supported_formats = {'csv': 'csv',
                     'mega-csv': 'csv',
                     'wos-plaintext': 'txt',
                     'group-bys-csv': 'csv',
                     'ris': 'ris',
                     'zip': 'zip'}

libraries_to_mum = [
    'psycopg2',
]

for library in libraries_to_mum:
    library_logger = logging.getLogger(library)
    library_logger.setLevel(logging.WARNING)
    library_logger.propagate = True
    warnings.filterwarnings("ignore", category=UserWarning, module=library)

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('DATABASE_URL').replace('postgres://', 'postgresql://')
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config['SQLALCHEMY_ECHO'] = (os.getenv('SQLALCHEMY_ECHO', False) == 'True')

slice_and_dice_api = os.getenv('SLICE_AND_DICE_API_URL')


class NullPoolSQLAlchemy(SQLAlchemy):
    def apply_driver_hacks(self, flask_app, info, options):
        options['poolclass'] = NullPool
        return super(NullPoolSQLAlchemy, self).apply_driver_hacks(flask_app, info, options)


db = NullPoolSQLAlchemy(app, session_options={"autoflush": False})

Compress(app)

app_url = os.getenv('APP_URL')
mailgun_api_key = os.getenv('MAILGUN_API_KEY')
