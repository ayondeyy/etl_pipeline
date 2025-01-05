"""
Extract - Fetching data from MySQL
"""

import json
from sqlalchemy import create_engine, text
import pandas as pd

def extract():
    db_param = json.load(open("database_connection.json"))

    try:
        print("\nExtracting Data...")
        mysql_conn_string = 'mysql://'+db_param["mysql_username"]+':'+db_param["mysql_password"]+'@'+db_param["mysql_host"]+':'+db_param["mysql_port"]+'/'+db_param["mysql_database_name"]
        mysql_engine = create_engine(mysql_conn_string, echo = False)
        mysql_conn = mysql_engine.connect()
        print("\nConnection to source database successful.")

    except:
        print("\nError in MySQL Connection.")

    raw_table_list = mysql_conn.execute(text('SHOW TABLES')).fetchall()

    tables = {}
    for (table_name,) in raw_table_list:
        df = pd.read_sql(f"SELECT * FROM {table_name}", mysql_conn)
        tables[table_name] = df

    mysql_conn.close()
    print("\nData extracted Successfully...!")

    return tables
