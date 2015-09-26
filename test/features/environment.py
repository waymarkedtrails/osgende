from behave import *

import logging
import os
from sqlalchemy import engine, create_engine
from sqlalchemy_utils import database_exists, drop_database, create_database


def before_all(context):
    if not context.config.log_capture:
        logging.basicConfig(level=context.config.logging_level)
    # database setup
    context.test_db = engine.url.URL('postgresql',
                      database=os.environ.get('TEST_DB', 'test_osgende'))
    context.keep_db = 'TEST_KEEP_DB' in os.environ
    context.nodestore_file = os.environ.get('TEST_NODESTORE', '/tmp/nodestore.osgende.test')


def before_scenario(context, scenario):
    # create new database
    if database_exists(context.test_db):
        drop_database(context.test_db)
    create_database(context.test_db)

    context.engine = create_engine(context.test_db, echo='ECHO_SQL' in os.environ)

    with context.engine.begin() as conn:
        conn.execute("CREATE EXTENSION postgis")
        conn.execute("CREATE EXTENSION hstore")

    try:
        os.remove(context.nodestore_file)
    except:
        pass # possibly not there

    context.tables = {}
    context.tagsets = {}


def after_scenario(context, scenario):
    if 'enigne' in context:
        context.engine.dispose()

    try:
        os.remove(context.nodestore_file)
    except:
        pass # possibly not there

    if not context.keep_db and database_exists(context.test_db):
        drop_database(context.test_db)
