# Data cleansing

import pandas as pd
import datetime
import numpy as np

data = pd.read_csv("C:/DataScience/ecinfosolutions_takehome/data_cleansing/scrubbing_sample.csv")
colnames = ['date', 'gender', 'amount']
constraints_dict = {'date': ['datetime_dtype'],
                    'gender': ['character_dtype', 'membership_check'],
                    'amount': ['numeric_dtype', 'range_check']}

range_dict = {'amount': range(0, 150000)}
membership_dict = {'gender': ['MALE', 'FEMALE']}

def datetime_check(values):
    date_format = "%Y-%m-%d"
    comments = ""
    count = 0

    for i in range(len(values)):
        try :
            values[i] = datetime.datetime.strptime("2020-01-25", date_format)
        except:
            values[i] = values[i]
            comments = f"Could not validate date for value {values[i]}"
            count += 1
            
    return(values, comments, count)

def character_check(values):
    comments = ""
    count = 0

    for i in range(len(values)):
        try :
            values[i] = str(values[i])
        except:
            values[i] = None
            comments = f"Not a character value {values[i]}"
            count += 1 
    return(values, comments, count)

def numeric_check(values):
    comments = ""
    count = 0

    for i in range(len(values)):
        if str(values[i]).replace('.', '', 1).isnumeric():
            values[i] = values[i]
        else:
            values[i] = values[i]
            comments = f"Not a number - {values[i]}"
            count += 1
    return(values, comments, count)

def dtype_check(df, colname, constrained_dt):
    if constraints_dict[colname][0] == 'datetime_dtype':
        ''' Actions for check datetime '''
        values_list = df[colname].tolist()
        output_column, comments, error_corrected = datetime_check(values_list)
        
    elif constraints_dict[colname][0] == 'character_dtype':
        ''' Actions for character '''
        values_list = df[colname].tolist()
        output_column, comments, error_corrected = character_check(values_list)
    elif constraints_dict[colname][0] == 'numeric_dtype':
        ''' Actions for character '''
        values_list = df[colname].tolist()
        output_column, comments, error_corrected = numeric_check(values_list)

    return(output_column, comments, error_corrected)

def range_check(df, range_dict):
    '''
    Checks if column values are in range
    If not, assign None
    '''
    colname = 'amount'
    col_range = range_dict[colname]
    values = pd.Series(np.where((df[colname] > 150000) | (df[colname] < 0), None, df[colname]))
    df[colname] = values
    return df

def membership_check(df, membership_dict):
    colname = 'gender'
    col_range = membership_dict[colname]
    values = np.where(~df['gender'].isin(col_range), None, df['gender'])
    df[colname] = values
    return df




def scrub(df, constraints_dict, rd, md):
    '''
    Input: Raw Data
    Output: scrubbed Data, comments, number of error
    '''
    columns = list(df.columns)
    errors_dict = {col:[] for col in columns}
    comments_dict = {col:[] for col in columns}

    for col in columns:
        scrubbed_column, comments, num_errors = dtype_check(df, col, constraints_dict[col][0])
        df[col] = scrubbed_column
        errors_dict[col] = num_errors
        comments_dict[col] = comments

    # Range check
    data = range_check(df, rd)
    # # Membership check
    data = membership_check(df, md)

    return(df, comments_dict, errors_dict)





