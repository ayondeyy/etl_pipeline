from utils import connection, extraction, transformation, loading
from data_cleaning import clean_data
from data_manipulation import manipulate_data

# print("\n----- Runing ETL Pipeline -----")
# mysql_conn, pg_engine, pg_conn = connection.db_connect()
# extraction.extract(mysql_conn, pg_engine)
# customer_data, business_data, movie_data, movie_business_data = transformation.transform(pg_conn)
# customer_data, business_data, movie_data, movie_business_data = loading.load(customer_data, business_data, movie_data, movie_business_data)

# customer_data, movie_data, business_data, movie_business_data = clean_data(customer_data, business_data, movie_data, movie_business_data)
# manipulate_data(customer_data, movie_data, business_data, movie_business_data)


tables = extraction.extract()