import json
import os
import pandas as pd
import pymysql
import logging
import sshtunnel
from sshtunnel import SSHTunnelForwarder
from sqlalchemy import create_engine
from sqlalchemy import text

def read_config(project: str) -> dict:
    file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), f'config/{project}.json')
    with open(file_path, "r") as f:
        return json.load(f)

def open_ssh_tunnel(verbose=False):
    if verbose:
        sshtunnel.DEFAULT_LOGLEVEL = logging.DEBUG
    
    global tunnel
    tunnel = SSHTunnelForwarder(
        (config['ssh_host'], 22),
        ssh_username = config['ssh_username'],
        ssh_password = config['ssh_password'],
        remote_bind_address = (config['localhost'], 3306)
    )
    
    tunnel.start()

def mysql_connect(history):
    global connection
    global cursor_object
    global engine

    database_name = config['database_name']
    if history:
        database_name = 'history'

    if config['connect_via_ssh']:
        try:
            connection = pymysql.connect(
                host=config['localhost'],
                user=config['database_username'],
                passwd=config['database_password'],
                db=database_name,
                port=tunnel.local_bind_port,
                cursorclass=pymysql.cursors.DictCursor,
                autocommit=True
            )
        except pymysql.connector as e:
            logger.error(e)
            raise

        cursor_object = connection.cursor()
    else:
        # not via ssh
        engine = create_engine(f"mysql+pymysql://{config['database_username']}:{config['database_password']}@{config['localhost']}/{database_name}")

def run_query_df(sql):
    if config['connect_via_ssh']:
        return pd.read_sql_query(sql, connection)
    else:
        # not via ssh
        sql_query = pd.read_sql(sql, engine)
        df = pd.DataFrame(sql_query)

        return df

def mysql_disconnect():
    if config['connect_via_ssh']:
        connection.close()
    else:
        # not via ssh
        engine.dispose()

def close_ssh_tunnel():
    tunnel.close

def run(sql):
    if config['connect_via_ssh']:
        cursor_object.execute(sql)
    else:
        # not via ssh
        with engine.connect() as conn:
            conn.execute(sql)

def connect(project: str, history=False):
    global config

    config = read_config(project)

    if config['connect_via_ssh']:
        open_ssh_tunnel()

    mysql_connect(history)

def run_df(sql):
    df = run_query_df(sql)

    return df

def disconnect():
    mysql_disconnect()

    if config['connect_via_ssh']:
        close_ssh_tunnel()
