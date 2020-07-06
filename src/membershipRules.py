
cover_membership_functions_dict = {'gender': cover_memberships_gender}


def cover_memberships_gender(string, count, error_values):
    '''
        For a string which is not a valid member of a column, try to catch it
        Ex: If FEMALE is encoded as Fem, try to convert it to FEMALE
        This will result in coverage of more data
    '''
    lower_string = string
    member_found = False

    if lower_string in ['male', 'm']:
        actual_value = string
        string = 'MALE'
        member_found = True
    elif lower_string in ['female', 'f']:
        actual_value = string
        string = 'FEMALE'
        member_found = True
    else:
        actual_value = string
        string = np.nan
        member_found = False
    
    return(string, actual_value, member_found)
    