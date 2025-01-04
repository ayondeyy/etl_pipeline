"""
Data Manipulation in Python using Pandas

Creating additional columns from existing ones for Analysis

Steps Involved:
Step 1: Calculating Amount (Rate * Duration)
Step 2: Extracting Year, month, date and day from datetime column
Step 3: WWriting to Parquet File for Analysis (to save space and improve reading time)
"""

import pandas as pd

from utils import manipulate

def manipulate_data(customer_data, movie_data, business_data, movie_business_data):
    print("\n----- Manipulating Data -----")

    # Manipulating Customer Data
    customer_data.to_parquet("./data/analyze_customer_data.parquet")

    # Manipulating Movie Data
    movie_data["rental_amount"] = (movie_data["rental_duration"] * movie_data["rental_rate"]).round(2)

    movie_data.to_parquet("./data/analyze_movie_data.parquet")

    # Manipulating Business Data
    business_data["payment_date"] = pd.to_datetime(business_data["payment_date"])
    business_data = manipulate.expand_date_col(business_data, "payment_date", "payment")

    business_data.to_parquet("./data/analyze_business_data.parquet")

    # Manipulating Movie Business Data
    movie_business_data["rental_amount"] = (movie_business_data["rental_duration"] * movie_business_data["rental_rate"]).round(2)
    movie_business_data["rental_date"] = pd.to_datetime(movie_business_data["rental_date"])
    movie_business_data["return_date"] = pd.to_datetime(movie_business_data["return_date"])
    movie_business_data["payment_date"] = pd.to_datetime(movie_business_data["payment_date"])
    movie_business_data["rental_period"] = (movie_business_data["return_date"] - movie_business_data["rental_date"]).dt.days
    movie_business_data["rental_period"] = movie_business_data["rental_period"].astype("Int64")
    movie_business_data = manipulate.expand_date_col(movie_business_data, "rental_date", "rental")

    movie_business_data.to_parquet("./data/analyze_movie_business_data.parquet")

    print("\nData Manipulated Successfully...")
