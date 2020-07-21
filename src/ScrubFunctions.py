import pandas as pd
import datetime
import numpy as np
import math

def datetime_check(values):
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
        try:
            a = math.isnan(values[i])
            if a:
                count += 0
        except:
            values[i] = values[i].strip().lower()
            count += 0
    
    return(values, count)

    # for i in range(len(values)):
    #     if is_numeric(values[i]):

    #         try:
    #             a = math.isnan(values[i])
    #             if a == True:
    #                 values[i] = values[i]
    #                 count += 0
    #             else:
    #                 values[i] = values[i].strip().lower()
    #                 count += 0
        
        
    #     elif is_datetime(values[i]):
    #         values[i] = values[i].strip().lower()
    #         count += 0
        
        
    
    # return(values, count)


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


def scrub(dataframe, constraints_dict, g_id, c_id):
    '''
    Input: Raw Data
    Output: scrubbed Data, comments, number of error
    '''

    df = dataframe.copy()

    goal_id = g_id
    customer_id = c_id
    batch_date = datetime.datetime.today()

    pre_scrub_missing_vc = sum(df.isnull().sum())
    total_values = sum(df.count())
    pre_scrub_missing_percent = np.round((pre_scrub_missing_vc/total_values)*100, 2)

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
                       'colwise_perc_prescrub_missing_values': dict(),
                       'pre_scrub_missing_percent': pre_scrub_missing_percent,
                       'pre_scrub_missing_values': pre_scrub_missing_vc}

    columns = list(df.columns)

    

    for col in columns:
        
        missing_count = df[col].isnull().sum()
        num_rows_data = df.shape[0]
        missing_percent = (missing_count/num_rows_data)*100

        if missing_percent > 0:
            output_data_row['colwise_perc_prescrub_missing_values'][col] = np.round(missing_percent, 2)

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


    post_scrub_missing_vc = sum(df.isnull().sum())
    total_values = sum(df.count())
    post_scrub_missing_percent = np.round((post_scrub_missing_vc/total_values)*100, 2)


    output_data_row['post_scrub_missing_values'] = post_scrub_missing_vc
    output_data_row['post_scrub_missing_percent'] = post_scrub_missing_percent
    output_data_row['DTRULE_VIOLATION_PERCENT'] = post_scrub_missing_percent - pre_scrub_missing_percent


    return(df, output_data_row)


