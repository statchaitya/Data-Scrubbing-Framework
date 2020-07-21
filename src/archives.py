# ----------------- SCRUB FUNCTIONS.py archives -------------------- #

'''
Un used code from all the files is pasted here just incase we need to reimplement something we left behind
'''

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

def membership_check(values, membership_set, col):
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


def numeric_range_check(value, lower_b, upper_b):
    if lower_b <= float(value) <= upper_b:
        return True
    else:
        return False

## --- inside scrub() function

dtype_errors_dict = {col:[] for col in columns}
dtype_comments_dict = {col:[] for col in columns}
dtype_results_dict = {col:[] for col in columns}

range_errors_dict = {col:[] for col in range_dict.keys()}
range_comments_dict = {col:[] for col in range_dict.keys()}

membership_errors_dict = {col:[] for col in membership_dict.keys()}
membership_comments_dict = {col:[] for col in membership_dict.keys()}

for col in range_dict.keys():
    scrubbed_col, error_values, num_errors = range_check(df[col].values, range_dict[col])
    df[col] = scrubbed_col
    range_errors_dict[col] = num_errors
    range_comments_dict[col] = error_values

if membership_dict['run_or_not']:
    for col in membership_dict.keys():
        scrubbed_col, error_values, num_errors = membership_check(df[col].values, membership_dict[col], col)
        df[col] = scrubbed_col
        membership_errors_dict[col] = num_errors
        membership_comments_dict[col] = error_values
    
# return(df, dtype_comments_dict, dtype_errors_dict, range_comments_dict, range_errors_dict, membership_comments_dict, membership_errors_dict)