"""
Transform - Transforming tables into dataframes and manipulating them for analysis
"""
# Remove Later
import os
os.chdir('D:/Work/Project/etl_pipeline')
from utils import extraction
tables = extraction.extract()

import pandas as pd
import numpy as np

customer = tables['customer']
address = tables['address']
city = tables['city']
country = tables['country']
store = tables['store']
staff = tables['staff']
payment = tables['payment']
film = tables['film']
film_category = tables['film_category']
category = tables['category']
language = tables['language']
film_actor = tables['film_actor']
actor = tables['actor']
inventory = tables['inventory']
rental = tables['rental']

# Store name by location
store_info = store.join(address, on='address_id', how='left', lsuffix='_store', rsuffix='_add')\
                    .join(city, on='city_id', how='left', lsuffix='_storeadd', rsuffix='_city')\
                    .join(country, on='country_id', how='left', lsuffix='_storecity', rsuffix='_country')
store_info['store_name'] = "Store at " + store_info['country']

# Customer Data
customer_data = pd.merge(customer, address, on='address_id', how='left')
customer_data = pd.merge(customer_data, city, on='city_id', how='left')
customer_data = pd.merge(customer_data, country, on='country_id', how='left', suffixes=('_data', '_country'))
customer_data = pd.merge(customer_data, store_info, on='store_id', how='left')

customer_data['active'] = np.where(customer_data['active'] == 1, 'Active', 'Inactive')
customer_data = customer_data[['store_name', 'active', 'city_x', 'country_x']]
customer_data.rename(columns={'city_x': 'city', 'country_x': 'country'}, inplace=True)


# Business Data
business_data = pd.merge(payment, staff, on='staff_id', how='left')
business_data = pd.merge(business_data, store_info, on='store_id', how='left')
business_data = pd.merge(business_data, address, left_on ='address_id_x', right_on='address_id', how='left')
business_data = pd.merge(business_data, city, left_on='city_id_x', right_on='city_id', how='left', suffixes=('_bus', '_city'))
business_data = pd.merge(business_data, country, left_on='country_id_bus', right_on='country_id', how='left', suffixes=('_data', '_country'))

business_data = business_data[['payment_date', 'active', 'store_name', 'city_bus', 'country_country', 'amount']]
business_data.rename(columns={'city_bus': 'city', 'country_country': 'country'}, inplace=True)

business_data['active'] = np.where(business_data['active'] == 1, 'Active', 'Inactive')

business_data["payment_year"] = business_data['payment_date'].dt.year
business_data["payment_month"] = business_data['payment_date'].dt.month_name()
business_data["payment_day_month"] = business_data['payment_date'].dt.day
business_data["payment_day_week"] = business_data['payment_date'].dt.day_name()


# Movie Data
raw_movie = pd.merge(film, film_category, on='film_id', how='left')
raw_movie = pd.merge(raw_movie, category, on='category_id', how='left')

movie_data = movie_data[['title', 'rental_duration', 'rental_rate', 'length', 'rating', 'special_features', 'name_film', 'rental_date', 'return_date', 'amount', 'payment_date']]
movie_data.rename(columns={'name_film': 'category'}, inplace=True)


# Movie Business Data
raw_movie = pd.merge(raw_movie, inventory, on='film_id', how='left')
raw_movie = pd.merge(raw_movie, rental, on='inventory_id', how='left', suffixes=('_film2', '_rental'))
raw_movie = pd.merge(raw_movie, payment, on='rental_id', how='left')




# Actor Data
actor_data = pd.merge(film_actor, actor, on='actor_id', how='outer')
actor_data = pd.merge(actor_data, film, on='film_id', how='left')
# movie_data = pd.merge(movie_data, inventory, on='film_id', how='left')
# movie_data = pd.merge(movie_data, rental, on='inventory_id', how='left')
# movie_data = pd.merge(movie_data, payment, on='rental_id', how='left')

















# from sqlalchemy import text
# import pandas as pd

# def rename_duplicates(columns): 
#     seen = {} 
#     new_columns = []

#     for col in columns: 
#         if col not in seen: 
#             seen[col] = 0 
#             new_columns.append(col) 
#         else: 
#             seen[col] += 1 
#             new_columns.append(f"{col}_duplicated_{seen[col]}")

#     return new_columns


# def transform(pg_conn):
#     try:
#         customer_data = pd.read_sql(text("""
#                                     SELECT
#                                         customer.*,
#                                         ':',
#                                         address.*,
#                                         ':',
#                                         city.*,
#                                         ':',
#                                         country.*
#                                     FROM
#                                         customer
#                                         FULL JOIN address ON customer.address_id = address.address_id
#                                         FULL JOIN city ON address.city_id = city.city_id
#                                         FULL JOIN country ON city.country_id = country.country_id;
#                                             """), con = pg_conn)
        
#         customer_data.columns = rename_duplicates(customer_data.columns)

#         business_data = pd.read_sql(text("""
#                                     SELECT
#                                         staff.*,
#                                         ':',
#                                         payment.*,
#                                         ':',
#                                         store.*,
#                                         ':',
#                                         address.*,
#                                         ':',
#                                         city.*,
#                                         ':',
#                                         country.*
#                                     FROM
#                                         staff
#                                         FULL JOIN payment ON staff.staff_id = payment.staff_id
#                                         FULL JOIN store ON staff.store_id = store.store_id
#                                         FULL JOIN address ON store.address_id = address.address_id
#                                         FULL JOIN city ON address.city_id = city.city_id
#                                         FULL JOIN country ON city.country_id = country.country_id;
#                                             """), con = pg_conn)
        
#         business_data.columns = rename_duplicates(business_data.columns)

#         movie_data = pd.read_sql(text("""
#                                 SELECT
#                                     film.*,
#                                     ':',
#                                     film_category.*,
#                                     ':',
#                                     category.*,
#                                     ':',
#                                     language.*,
#                                     ':',
#                                     film_actor.*,
#                                     ':',
#                                     actor.*
#                                 FROM
#                                     film
#                                     FULL JOIN film_category ON film.film_id = film_category.film_id
#                                     FULL JOIN category ON film_category.category_id = category.category_id
#                                     FULL JOIN language ON film.language_id = language.language_id
#                                     FULL JOIN film_actor ON film.film_id = film_actor.film_id
#                                     FULL JOIN actor ON film_actor.actor_id = actor.actor_id;
#                                         """), con = pg_conn)
        
#         movie_data.columns = rename_duplicates(movie_data.columns)

#         movie_business_data = pd.read_sql(text("""
#                                         SELECT
#                                             film.*,
#                                             ':',
#                                             inventory.*,
#                                             ':',
#                                             rental.*,
#                                             ':',
#                                             payment.*
#                                         FROM
#                                             film
#                                             FULL JOIN inventory ON film.film_id = inventory.film_id
#                                             FULL JOIN rental ON inventory.inventory_id = rental.inventory_id
#                                             FULL JOIN payment ON rental.rental_id = payment.rental_id;
#                                                 """), con = pg_conn)
        
#         movie_business_data.columns = rename_duplicates(movie_business_data.columns)
    
#     finally:
#         print("\nData transformed Successfully...! Closing destination connection...")
#         pg_conn.close()

#     return customer_data, business_data, movie_data, movie_business_data