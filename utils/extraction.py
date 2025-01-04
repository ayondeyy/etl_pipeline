"""
# Extract - Fetching data from MySQL to PostgreSQL as staging database
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
        print("\nConnection to source Successful.")

    except:
        print("\nError in MySQL Connection.")

    # Getting the table names
    raw_table_list = mysql_conn.execute(text('SHOW TABLES')).fetchall()
    # table_list = list(map(lambda x: x[0], raw_table_list))

    tables = {}
    for (table_name,) in raw_table_list:
        df = pd.read_sql(f"SELECT * FROM {table_name}", mysql_conn)
        tables[table_name] = df

    mysql_conn.close()
    print("\nData extracted Successfully...!")

    return tables






# from sqlalchemy import text
# import pandas as pd

# def extract(mysql_conn, pg_engine):
#     print("\nExtracting Data...")
#     # Getting the table names
#     raw_table_list = mysql_conn.execute(text('SHOW TABLES')).fetchall()
#     table_list = list(map(lambda x: x[0], raw_table_list))

#     # Transferring data
#     try:
#         for table in table_list:
#             temp = pd.read_sql(sql = text('SELECT * FROM ' + table), con = mysql_conn)
#             temp.to_sql(name = table, con = pg_engine, if_exists='replace', index=False)

#     except:
#         print("\nError in transfering the Data...! Closing connections...")
#         mysql_conn.close()

#     finally:
#         print("\nData extracted Successfully...! Closing source connections...")
#         mysql_conn.close()
