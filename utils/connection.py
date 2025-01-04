"""
Creating Connections with Databases
"""

import json
from sqlalchemy import create_engine

def db_connect():
    print("\nConnecting to source and destination...")
    db_param = json.load(open("database_connection.json"))

    try:
        mysql_conn_string = 'mysql://'+db_param["mysql_username"]+':'+db_param["mysql_password"]+'@'+db_param["mysql_host"]+':'+db_param["mysql_port"]+'/'+db_param["mysql_database_name"]
        mysql_engine = create_engine(mysql_conn_string, echo = False)
        mysql_conn = mysql_engine.connect()
        print("\nConnection to source Successful.")

    except:
        print("\nError in MySQL Connection.")

    try:
        pg_conn_string = 'postgresql://'+db_param["pg_username"]+':'+db_param["pg_password"]+'@'+db_param["pg_host"]+':'+db_param["pg_port"]+'/'+db_param["pg_database_name"]
        pg_engine = create_engine(pg_conn_string, echo = False)
        pg_conn = pg_engine.connect()
        print("\nConnection to destination Successful.")

    except:
        print("\nError in postgres Connection.")

    return mysql_conn, pg_engine, pg_conn