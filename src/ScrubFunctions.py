# Data cleansing

import pandas as pd
import datetime
import numpy as np

colnames = ['date', 'gender', 'amount']

def datetime_check(values):
    #date_format = "%Y-%m-%d"
    error_values = []
    count = 0

    for i in range(len(values)):
        try :
            values[i] = pd.to_datetime(values[i])
        except:
            actual_value = values[i]
            values[i] = np.nan
            error_values.append(actual_value)
            count += 1
            
    return(values, error_values, count)

def character_check(values):
    error_values = []
    count = 0

    for i in range(len(values)):
        if is_numeric(values[i]):
            actual_value = values[i]
            values[i] = np.nan
            error_values.append(actual_value)
            count += 1
        elif is_datetime(values[i]):
            actual_value = values[i]
            values[i] = np.nan
            error_values.append(actual_value)
            count += 1

    return(values, error_values, count)

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
    error_values = []
    count = 0

    for i in range(len(values)):
        if is_numeric(values[i]):
            values[i] = values[i]
        else:
            actual_value = values[i]
            values[i] = np.nan
            error_values.append(actual_value)
            count += 1

    return(values, error_values, count)

def dtype_check(df, colname, constrained_dt):
    if constrained_dt == 'datetime_dtype':
        ''' Actions to check datetime '''
        values_list = df[colname].tolist()
        output_column, error_values, error_corrected = datetime_check(values_list)
        
    elif constrained_dt == 'character_dtype':
        ''' Actions for character '''
        values_list = df[colname].tolist()
        output_column, error_values, error_corrected = character_check(values_list)
    elif constrained_dt == 'numeric_dtype':
        ''' Actions for character '''
        values_list = df[colname].tolist()
        output_column, error_values, error_corrected = numeric_check(values_list)

    return(output_column, error_values, error_corrected)

def range_check(values, value_range):
    '''
    Checks if column values are in range
    If not, assign None
    '''
    error_values = []
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
            error_values.append(actual_value)

    return(values, error_values, count)

def membership_check(values, membership_set):
    error_values = []
    count = 0
    membership_set.append(np.nan)
    
    for i in range(len(values)):
        
        if not values[i] in membership_set:
            lower_string = values[i].lower()
            if lower_string in ['male', 'm']:
                actual_value = values[i]
                values[i] = 'MALE'
                count += 1
                error_values.append(actual_value)
            elif lower_string in ['female', 'f']:
                actual_value = values[i]
                values[i] = 'FEMALE'
                count += 1
                error_values.append(actual_value)
            else:
                actual_value = values[i]
                values[i] = np.nan
                count += 1
                error_values.append(actual_value)
    
    return(values, error_values, count)
            

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
        scrubbed_column, error_values, num_errors = dtype_check(df, col, constraints_dict[col][0])
        df[col] = scrubbed_column
        dtype_errors_dict[col] = num_errors
        dtype_comments_dict[col] = error_values

    for col in range_dict.keys():
        scrubbed_col, error_values, num_errors = range_check(df[col].values, range_dict[col])
        df[col] = scrubbed_col
        range_errors_dict[col] = num_errors
        range_comments_dict[col] = error_values

    for col in membership_dict.keys():
        scrubbed_col, error_values, num_errors = membership_check(df[col].values, membership_dict[col])
        df[col] = scrubbed_col
        membership_errors_dict[col] = num_errors
        membership_comments_dict[col] = error_values
    

    return(df, dtype_comments_dict, dtype_errors_dict, range_comments_dict, range_errors_dict, membership_comments_dict, membership_errors_dict)



