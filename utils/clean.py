import pandas as pd
import re




def to_title_case(df, list_of_cols):
    """
    change title case
    """
    for col_name in list_of_cols:
        df[col_name] = df[col_name].str.title()

    return df


def to_lower_case(df, list_of_cols):
    """
    change lower case
    """
    for col_name in list_of_cols:
        df[col_name] = df[col_name].str.lower()

    return df


def remove_col(df, list_of_cols):
    """
    remove cols
    """
    for col in list_of_cols:
        df = df.drop(col, axis = 1)

    return df


def float_to_int(df, list_of_cols):
    """
    Convert columns with float dtype to int dtype
    """
    for col_name in list_of_cols:
        df[col_name] = df[col_name].astype("Int64")
        
    return df

def remove_row_with_index(df, index_list, reshuffle_index):
    """
    Removes rows when specific index list is provided
    Note: reshuffle_index should be Boolean
    """
    for i in index_list:
        df = df.drop(i, axis = 0)

    if reshuffle_index == True:
        df.reset_index(drop = True, inplace = True)
        
    return df
