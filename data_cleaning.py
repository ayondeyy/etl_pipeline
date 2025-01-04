"""
Data Cleaning in Python using Polars

Steps involved:
Step 1: Removing Duplicate Columns
Step 2: Change cases to Title Case and Lower Case
Step 3: Merging Columns
Step 4: Removing unwanted columns
Step 5: Change datatype of columns
Step 6: Renaming Columns
Step 7: Reordering the columns for better readability
Step 8: Removing appropriate rows with null values
Step 9: Exporting to Pandas Dataframe for Manipulation
"""
import re
from utils import clean

def remove_duplicate_cols(df, duplicate_col_string):
    """
    Removes Duplicate Columns from DataFrame
    """    
    for col in df.columns:
        if re.search(duplicate_col_string, col):
            df = df.drop(col, axis = 1)

    return df

def clean_data(customer_data, movie_data, business_data, movie_business_data):
    print("\n----- Cleaning Data -----")

    # Cleaning Customer Data
    customer_data = remove_duplicate_cols(customer_data, "duplicated")
    # customer_data = clean.to_title_case(customer_data, ["first_name", "last_name"])
    customer_data["first_name"] = customer_data["first_name"].str.title()
    customer_data["last_name"] = customer_data["last_name"].str.title()
    # customer_data = clean.to_lower_case(customer_data, ["email"])
    customer_data["email"] = customer_data["email"].str.lower()
    customer_data["customer_name"] = customer_data["first_name"].str.cat(customer_data["last_name"], sep = " ")
    # customer_data = clean.remove_col(customer_data, ["store_id", "first_name", "last_name", "address_id", "create_date", "last_update", "?column?", "address2", "city_id", "country_id"])
    customer_data.drop(["store_id", "first_name", "last_name", "address_id", "create_date", "last_update", "?column?", "address2", "city_id", "country_id"], axis = 1, inplace = True)
    customer_data = customer_data.loc[:, ["customer_id", "customer_name", "active", "phone", "email", "location", "address", "district", "city", "postal_code", "country"]]

    customer_data = customer_data[customer_data[["customer_id", "phone", "address"]].notnull().all(1)]
    customer_data.reset_index(drop = True, inplace = True)


    # Cleaning movie data
    movie_data = clean.remove_duplicate_cols(movie_data, "duplicated")
    # movie_data = movie_data.rename(columns = {"title": "movie_title"})
    # movie_data = clean.to_title_case(movie_data, ["movie_title", "first_name", "last_name"])
    movie_data = clean.to_title_case(movie_data, ["first_name", "last_name"])
    movie_data["actor_name"] = movie_data["first_name"].str.cat(movie_data["last_name"], sep = " ")
    movie_data = clean.remove_col(movie_data, ["film_id", "language_id", "original_language_id", "last_update", "?column?", "category_id", "actor_id", "first_name", "last_name"])
    movie_data = movie_data.rename(columns = {"name": "category_name"})

    movie_data = movie_data[movie_data[["movie_title"]].notnull().all(1)]
    movie_data.reset_index(drop = True, inplace = True)


    # Cleaning business data
    business_data = clean.remove_duplicate_cols(business_data, "duplicated")
    business_data = clean.to_lower_case(business_data, ["email"])
    business_data["staff_name"] = business_data["first_name"].str.cat(business_data["last_name"], sep = " ")
    business_data = clean.remove_col(business_data, ["staff_id", "first_name", "last_name", "address_id", "picture", "username", "password", "last_update", "?column?", "address2", "city_id", "postal_code", "phone", "country_id"])
    business_data = business_data.loc[:, ["staff_name", "email", "address", "district", "city", "country", "location", "active", "store_id", "manager_staff_id", "payment_id", "customer_id", "rental_id", "amount", "payment_date"]]

    business_data = business_data[business_data[["staff_name"]].notnull().all(1)]
    business_data.reset_index(drop = True, inplace = True)


    # Cleaning movie business data
    movie_business_data = clean.remove_duplicate_cols(movie_business_data, "duplicated")
    movie_business_data = movie_business_data.rename(columns = {"title": "movie_title"})
    # movie_business_data = clean.to_title_case(movie_business_data, ["movie_title"])
    movie_business_data = clean.remove_col(movie_business_data, ["film_id", "description", "language_id", "original_language_id", "last_update", "?column?", "customer_id", "payment_id"])


    print("\nData Cleaned Successfully...")

    return customer_data, movie_data, business_data, movie_business_data
