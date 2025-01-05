"""
Load - Loading datasets for analysis
"""

import json
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, drop_database, create_database

def load(data):
    print("\nLoading data to destination database...")
    db_param = json.load(open("database_connection.json"))

    try:
        pg_conn_string = 'postgresql://'+db_param["pg_username"]+':'+db_param["pg_password"]+'@'+db_param["pg_host"]+':'+db_param["pg_port"]+'/'+db_param["pg_database_name"]

        if database_exists(pg_conn_string):
            drop_database(pg_conn_string)

        create_database(pg_conn_string)

        pg_engine = create_engine(pg_conn_string, echo = False)
        pg_conn = pg_engine.connect()
        print("\nConnection to destination database successful.")

    except:
        print("\nError in PostgreSQL Connection.")

    for key in data.keys():
        var = key
        table = key.replace('_data','')
        data[var].to_sql(table, pg_conn, if_exists='replace', index=False)

    pg_conn.close()
    print("\nData loaded successfully...")
    