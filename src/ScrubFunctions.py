# Data cleansing

import pandas as pd
import datetime
import numpy as np

colnames = ['date', 'gender', 'amount']

def datetime_check(values):
    #date_format = "%Y-%m-%d"
    comments = ""
    count = 0

    for i in range(len(values)):
        try :
            values[i] = pd.to_datetime(values[i])
        except:
            actual_value = values[i]
            values[i] = np.nan
            comments = comments + f"Could not validate date for value {actual_value}" + "\n"
            count += 1
            
    return(values, comments, count)

def character_check(values):
    comments = ""
    count = 0

    for i in range(len(values)):
        if is_numeric(values[i]):
            actual_value = values[i]
            values[i] = np.nan
            comments = comments + f"Value is actually a number -- {actual_value}" + "\n"
            count += 1
        elif is_datetime(values[i]):
            actual_value = values[i]
            values[i] = np.nan
            comments = comments + f"Value is actually a date -- {actual_value}" + "\n"
            count += 1

    return(values, comments, count)

def numeric_range_check(value, lower_b, upper_b):
    if lower_b <= float(value) <= upper_b:
        return True
    else:
        return False

def is_datetime(val):
    is_datetime = False
    try:
        pd.to_datetime(val)
        is_datetime = True
        return is_datetime
    except:
        return is_datetime
    
    
def is_numeric(num):
    isfloat = False
    isint = False
    try:
        float(num)
        isfloat = True
        return isfloat
    except ValueError:
        return isfloat
    
    if not isfloat:
        try:
            int(num)
            isint = True
            return isint
        except ValueError:
            return isint

def numeric_check(values):
    comments = ""
    count = 0

    for i in range(len(values)):
        if is_numeric(values[i]):
            values[i] = values[i]
        else:
            actual_value = values[i]
            values[i] = np.nan
            comments = comments + f"The value {actual_value} is not a number" + "\n"
            count += 1

    return(values, comments, count)

def dtype_check(df, colname, constrained_dt):
    if constrained_dt == 'datetime_dtype':
        ''' Actions to check datetime '''
        values_list = df[colname].tolist()
        output_column, comments, error_corrected = datetime_check(values_list)
        
    elif constrained_dt == 'character_dtype':
        ''' Actions for character '''
        values_list = df[colname].tolist()
        output_column, comments, error_corrected = character_check(values_list)
    elif constrained_dt == 'numeric_dtype':
        ''' Actions for character '''
        values_list = df[colname].tolist()
        output_column, comments, error_corrected = numeric_check(values_list)

    return(output_column, comments, error_corrected)

def range_check(values, value_range):
    '''
    Checks if column values are in range
    If not, assign None
    '''
    comments = ''
    count = 0
    lower_bound = min(value_range)
    upper_bound = max(value_range)

    for i in range(len(values)):
        if numeric_range_check(values[i], lower_bound, upper_bound):
            values[i] = values[i]
        else:
            actual_value = values[i]
            values[i] = np.nan
            count += 1
            comments = comments + f"Value out of range {actual_value}" + "\n"

    return(values, comments, count)

def membership_check(values, membership_set):
    comments = ''
    count = 0
    membership_set.append(np.nan)
    
    for i in range(len(values)):
        
        if not values[i] in membership_set:
            lower_string = values[i].lower()
            if lower_string in ['male', 'm']:
                actual_value = values[i]
                values[i] = 'MALE'
                count += 1
                comments = comments + f"Value '{actual_value}' changed to 'MALE'" + "\n"
            elif lower_string in ['female', 'f']:
                actual_value = values[i]
                values[i] = 'FEMALE'
                count += 1
                comments = comments + f"Value '{actual_value}' changed to 'FEMALE'" + "\n"
            else:
                actual_value = values[i]
                values[i] = np.nan
                count += 1
                comments = comments + f"Value {actual_value} is not in membership set {membership_set}" + "\n"
    
    return(values, comments, count)
            
        




def scrub(df, constraints_dict, range_dict, membership_dict):
    '''
    Input: Raw Data
    Output: scrubbed Data, comments, number of error
    '''
    columns = list(df.columns)
    dtype_errors_dict = {col:[] for col in columns}
    dtype_comments_dict = {col:[] for col in columns}

    range_errors_dict = {col:[] for col in range_dict.keys()}
    range_comments_dict = {col:[] for col in range_dict.keys()}

    membership_errors_dict = {col:[] for col in membership_dict.keys()}
    membership_comments_dict = {col:[] for col in membership_dict.keys()}

    for col in columns:
        # For each column in a df run dtype_check based on appropriate dtype
        scrubbed_column, comments, num_errors = dtype_check(df, col, constraints_dict[col][0])
        df[col] = scrubbed_column
        dtype_errors_dict[col] = num_errors
        dtype_comments_dict[col] = comments

    for col in range_dict.keys():
        scrubbed_col, comments, num_errors = range_check(df[col].values, range_dict[col])
        df[col] = scrubbed_col
        range_errors_dict[col] = num_errors
        range_comments_dict[col] = comments

    for col in membership_dict.keys():
        scrubbed_col, comments, num_errors = membership_check(df[col].values, membership_dict[col])
        df[col] = scrubbed_col
        membership_errors_dict[col] = num_errors
        membership_comments_dict[col] = comments
    

    return(df, dtype_comments_dict, dtype_errors_dict, range_comments_dict, range_errors_dict, membership_comments_dict, membership_errors_dict)





