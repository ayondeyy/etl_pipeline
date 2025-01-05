"""
Transform - Transforming tables into dataframes and manipulating them for analysis

Steps involved:
Step 1: Merging tables
Step 2: Converting integer to string
Step 3: Creating new columns
Step 4: Change cases to title and lower cases
Step 5: Removing appropriate rows with null values
Step 6: Selecting required columns
Step 7: Renaming columns
"""

import pandas as pd
import numpy as np

pd.options.mode.copy_on_write = True

def transform(tables):
    print("\nTransforming data...")

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

    business_data['active'] = np.where(business_data['active'] == 1, 'Active', 'Inactive')

    business_data["payment_year"] = business_data['payment_date'].dt.year
    business_data["payment_month"] = business_data['payment_date'].dt.month_name()
    business_data["payment_day_month"] = business_data['payment_date'].dt.day
    business_data["payment_day_week"] = business_data['payment_date'].dt.day_name()

    business_data = business_data[['payment_date', 'payment_year', 'payment_month', 'payment_day_month', 'payment_day_week', 'active', 'store_name', 'city_bus', 'country_country', 'amount']]
    business_data.rename(columns={'city_bus': 'city', 'country_country': 'country'}, inplace=True)


    # Common Movie Features
    raw_movie = pd.merge(film, film_category, on='film_id', how='left')
    raw_movie = pd.merge(raw_movie, category, on='category_id', how='left')
    raw_movie['rental_amount'] = (raw_movie['rental_duration'] * raw_movie['rental_rate']).round(2)
    raw_movie['title'] = raw_movie['title'].str.title()


    # Movie Data
    movie_data = raw_movie[['title', 'rental_duration', 'rental_rate', 'rental_amount', 'length', 'rating', 'special_features', 'name']]
    movie_data.rename(columns={'length': 'length_in_min', 'name': 'category'}, inplace=True)


    # Movie Business Data
    movie_business_data = pd.merge(raw_movie, inventory, on='film_id', how='left', suffixes=('_raw', None))
    movie_business_data = pd.merge(movie_business_data, rental, on='inventory_id', how='left', suffixes=('_film2', '_rental'))
    movie_business_data = pd.merge(movie_business_data, payment, on='rental_id', how='left')

    movie_business_data["payment_year"] = movie_business_data['payment_date'].dt.year
    movie_business_data["payment_month"] = movie_business_data['payment_date'].dt.month_name()
    movie_business_data["payment_day_month"] = movie_business_data['payment_date'].dt.day
    movie_business_data["payment_day_week"] = movie_business_data['payment_date'].dt.day_name()

    movie_business_data = movie_business_data[['title', 'rental_duration', 'rental_rate', 'rental_amount', 'length', 'replacement_cost', 'rating', 'special_features', 'name', 'rental_date', 'payment_date', 'payment_year', 'payment_month', 'payment_day_month', 'payment_day_week', 'amount']]
    movie_business_data.rename(columns={'length': 'length_in_min', 'name': 'category'}, inplace=True)


    # Actor Data
    actor_data = pd.merge(film_actor, actor, on='actor_id', how='left')
    actor_data = pd.merge(actor_data, film, on='film_id', how='left')
    actor_data = pd.merge(actor_data, film_category, on='film_id', how='left', suffixes=('_data', '_cat'))
    actor_data = pd.merge(actor_data, category, on='category_id', how='left')
    actor_data = pd.merge(actor_data, inventory, on='film_id', how='left', suffixes=('_data2', '_inv'))
    actor_data = pd.merge(actor_data, rental, on='inventory_id', how='left')
    actor_data = pd.merge(actor_data, payment, on='rental_id', how='left', suffixes=('_data3', '_pay'))

    actor_data['actor'] = actor_data['first_name'] + " " + actor_data['last_name']
    actor_data['rental_amount'] = (actor_data['rental_duration'] * actor_data['rental_rate']).round(2)
    actor_data["payment_year"] = actor_data['payment_date'].dt.year
    actor_data["payment_month"] = actor_data['payment_date'].dt.month_name()
    actor_data["payment_day_month"] = actor_data['payment_date'].dt.day
    actor_data["payment_day_week"] = actor_data['payment_date'].dt.day_name()

    actor_data['actor'] = actor_data['actor'].str.title()
    actor_data['title'] = actor_data['title'].str.title()

    actor_data = actor_data[['actor', 'title', 'rental_duration', 'rental_rate', 'rental_amount', 'length', 'rating', 'special_features', 'name', 'payment_date', 'payment_year', 'payment_month', 'payment_day_month', 'payment_day_week', 'amount']]
    actor_data.rename(columns={'title': 'movie_title', 'length': 'length_in_min', 'name': 'category'}, inplace=True)

    data = {
        'customer_data': customer_data,
        'business_data': business_data,
        'movie_data': movie_data,
        'movie_business_data': movie_business_data,
        'actor_data': actor_data
    }

    print("\nData Transformed Successfully...!")

    return data
