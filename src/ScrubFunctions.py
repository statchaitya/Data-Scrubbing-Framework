# Data cleansing

import pandas as pd
import datetime
import numpy as np

colnames = ['date', 'gender', 'amount']





# def range_check(values, value_range):
#     '''
#     Checks if column values are in range
#     If not, assign None
#     '''
#     error_values = []
#     count = 0
#     lower_bound = min(value_range)
#     upper_bound = max(value_range)

#     for i in range(len(values)):
#         if numeric_range_check(values[i], lower_bound, upper_bound):
#             values[i] = values[i]
#         else:
#             actual_value = values[i]
#             values[i] = np.nan
#             count += 1
#             error_values.append(actual_value)

#     return(values, error_values, count)

# def membership_check(values, membership_set, col):
#     error_values = []
#     count = 0
#     membership_set.append(np.nan)
    
#     for i in range(len(values)):
        
#         if not values[i] in membership_set:
#             lower_string = values[i].lower()
#             if lower_string in ['male', 'm']:
#                 actual_value = values[i]
#                 values[i] = 'MALE'
#                 count += 1
#                 error_values.append(actual_value)
#             elif lower_string in ['female', 'f']:
#                 actual_value = values[i]
#                 values[i] = 'FEMALE'
#                 count += 1
#                 error_values.append(actual_value)
#             else:
#                 actual_value = values[i]
#                 values[i] = np.nan
#                 count += 1
#                 error_values.append(actual_value)

#     return(values, error_values, count)


def datetime_check(values):
    #date_format = "%Y-%m-%d"
    count = 0

    for i in range(len(values)):
        try :
            values[i] = pd.to_datetime(values[i])
        except:
            values[i] = np.nan
            count += 1

    return(values, count)

def character_check(values):
    count = 0

    for i in range(len(values)):
        if is_numeric(values[i]):
            # Not assigning null because some columns are mixed type
            # So a value like '55232' can be a legit categorical value
            # But count += 1 will ensure we count it atleast.
            values[i] = values[i]
            count += 1
        elif is_datetime(values[i]):
            # Same explaination as above
            values[i] = values[i]
            count += 1
    

    return(values, count)

# def numeric_range_check(value, lower_b, upper_b):
#     if lower_b <= float(value) <= upper_b:
#         return True
#     else:
#         return False

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
    count = 0

    for i in range(len(values)):
        if is_numeric(values[i]):
            values[i] = values[i]
        else:
            values[i] = np.nan
            count += 1
    
    return(values, count)

def dtype_check(df, colname, constrained_dt):
    if not constrained_dt in ['datetime_dtype', 'numeric_dtype', 'character_dtype']:
        raise ValueError("Not a valid constrained_dt value")
    
    if constrained_dt == 'datetime_dtype':
        ''' Actions to check datetime '''
        values_list = df[colname].tolist()
        print(f"Checking dtype {constrained_dt} for column {colname}")
        output_column, errors_corrected = datetime_check(values_list)
        
    elif constrained_dt == 'character_dtype':
        ''' Actions for character '''
        values_list = df[colname].tolist()
        print(f"Checking dtype {constrained_dt} for column {colname}")
        output_column, errors_corrected = character_check(values_list)
    elif constrained_dt == 'numeric_dtype':
        ''' Actions for character '''
        values_list = df[colname].tolist()
        print(f"Checking dtype {constrained_dt} for column {colname}")
        output_column, errors_corrected = numeric_check(values_list)

    return(output_column, errors_corrected)


def scrub(dataframe, constraints_dict):
    '''
    Input: Raw Data
    Output: scrubbed Data, comments, number of error
    '''

    df = dataframe.copy()

    goal_id = 1
    customer_id = 1
    batch_date = datetime.datetime.today()

    output_data_row = {'goal_id': goal_id,
                       'customer_id': customer_id,
                       'batch_date': batch_date,
                       'dataset_type': 'training',
                       'colwise_numeric_errors': dict(),
                       'total_numeric_errors': 0,
                       'colwise_character_errors': dict(),
                       'total_character_errors': 0,
                       'colwise_datetime_errors': dict(),
                       'total_datetime_errors': 0,
                       'colwise_perc_missing_values': dict(),
                       'total_missing_values': 0}

    columns = list(df.columns)
    # dtype_errors_dict = {col:[] for col in columns}
    # dtype_comments_dict = {col:[] for col in columns}
    # dtype_results_dict = {col:[] for col in columns}

    # range_errors_dict = {col:[] for col in range_dict.keys()}
    # range_comments_dict = {col:[] for col in range_dict.keys()}

    # membership_errors_dict = {col:[] for col in membership_dict.keys()}
    # membership_comments_dict = {col:[] for col in membership_dict.keys()}

    for col in columns:
        
        missing_count = df[col].isnull().sum()
        num_rows_data = df.shape[0]
        missing_percent = (missing_count/num_rows_data)*100

        if missing_percent > 0:
            output_data_row['colwise_perc_missing_values'][col] = np.round(missing_percent, 2)

        print(f"Missing percent for column {col} are {missing_percent}")

        scrubbed_column, num_errors = dtype_check(df, col, constraints_dict[col][0])

        df[col] = scrubbed_column

        if constraints_dict[col][0] == 'datetime_dtype':
            output_data_row['colwise_datetime_errors'][col] = num_errors
        elif constraints_dict[col][0] == 'numeric_dtype':
            output_data_row['colwise_numeric_errors'][col] = num_errors
        elif constraints_dict[col][0] == 'character_dtype':
            output_data_row['colwise_character_errors'][col] = num_errors
        else:
            raise ValueError(f"Not an acceptable value - {constraints_dict[col][0]}")

    # for col in range_dict.keys():
    #     scrubbed_col, error_values, num_errors = range_check(df[col].values, range_dict[col])
    #     df[col] = scrubbed_col
    #     range_errors_dict[col] = num_errors
    #     range_comments_dict[col] = error_values

    # if membership_dict['run_or_not']:
    #     for col in membership_dict.keys():
    #         scrubbed_col, error_values, num_errors = membership_check(df[col].values, membership_dict[col], col)
    #         df[col] = scrubbed_col
    #         membership_errors_dict[col] = num_errors
    #         membership_comments_dict[col] = error_values
    
    # return(df, dtype_comments_dict, dtype_errors_dict, range_comments_dict, range_errors_dict, membership_comments_dict, membership_errors_dict)
    return(df, output_data_row)


