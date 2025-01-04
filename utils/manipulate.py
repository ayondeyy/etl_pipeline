def expand_date_col(df, date_col_name, new_col_name):
    """
    Expands datetime column to generate year, month, day of the month and day of the week

    Args:
        df (dataframe): Pandas Dataframe
        date_col_name (str): Actual column name of date
        new_col_name (str): New column prefix. Example: payment will be generated as payment_year, payment_month, and so on

    Returns:
        dataframe: Pandas Dataframe
    """    
    df[new_col_name + "_year"] = df[date_col_name].dt.year
    df[new_col_name + "_month"] = df[date_col_name].dt.month_name()
    df[new_col_name + "_day_month"] = df[date_col_name].dt.day
    df[new_col_name + "_day_week"] = df[date_col_name].dt.day_name()

    return df