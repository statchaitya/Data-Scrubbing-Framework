
colname = "date"
coltype = "datetime"

# HDC
correct_dtypes_dict = {'date': 'datetime',
                       'gender': 'character',
                       'amount': 'numeric'}

def inspect_dtypes(pd_df, correct_dtypes_dict):
    '''
    Input: pandas dataframe
    Output: Dict:
                key1: list of columns violating data type constraints
                key2: int - number of columns who passed the inspection
    '''
    out_dict = {}
    success = 0
    dtypes_dict = pd_df.dtypes.to_dict()
    
    for column in pd_df.columns:
        interpreted_coltype = pd_df[column].dtype

        


def data_type(colname, coltype):
    '''
    Input: List of column values
    Output: List of column converted to a certain dtype
    '''


