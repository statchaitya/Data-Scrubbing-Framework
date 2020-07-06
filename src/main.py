import os
os.chdir("C:/DataScience/Github/Data-Scrubbing-Framework/src/")
import ScrubFunctions as SF
import pandas as pd
import datetime

# essential_cols = ['Per Nbr', 'Dt of Svc', 'CPT4', 'Diag 1', 'Diag 2', 'Payer Name', 'Tran Cd', 'Bt Dt',\
#                     'Proc Dt', 'Alwd Amt', 'Pay Amt']

data = pd.read_csv("C:/Coherence/data/goal_01_customer_01.csv")


goal_id = 1
customer_id = 1
batch_date = datetime.datetime.today()
'''
Assumptions:
    1. We know the incoming paramter name (column name) and their dtype before they come in
    2. Following dictionaries use that information.
'''

constraints_dict = {'Loc Name': ['numeric_dtype'],
                    'Post Date': ['datetime_dtype'],
                    'Proc Dt': ['datetime_dtype'],
                    'Bt Dt': ['datetime_dtype'],
                    'Bt Nbr': ['numeric_dtype'],
                    'Bt Desc': ['character_dtype'],
                    'Track Desc': ['character_dtype'],
                    'CHK': ['character_dtype'],
                    'E/I/A/B': ['numeric_dtype'],
                    'Src Type': ['character_dtype'],
                    'Per Nbr': ['numeric_dtype'],
                    'Md Rc': ['numeric_dtype'],
                    'Billed Amt': ['numeric_dtype'],
                    'Aprv Amt': ['numeric_dtype'],
                    'Alwd Amt': ['numeric_dtype'],
                    'Tran Dt': ['datetime_dtype'],
                    'Tran Cd': ['character_dtype'],
                    'Pay Amt': ['numeric_dtype'],
                    'Tran Status': ['character_dtype'],
                    'Source': ['character_dtype'],
                    'Reason Cds/Remarks': ['character_dtype'],
                    'Reason Cds/Remarks Desc': ['character_dtype'],
                    'Reason Code SubGrp 1': ['character_dtype'],
                    'Reason Code SubGrp 2': ['character_dtype'],
                    'Ded Amt': ['numeric_dtype'],
                    'Dt of Svc': ['datetime_dtype'],
                    'CPT4': ['character_dtype'],
                    'CPT4 Desc': ['character_dtype'],
                    'Sv It': ['character_dtype'],
                    'Sv It Desc': ['character_dtype'],
                    'Primary': ['numeric_dtype'],
                    'Secondary': ['numeric_dtype'],
                    'Tertiary': ['numeric_dtype'],
                    'Pat Amt': ['numeric_dtype'],
                    'Unit Price':['numeric_dtype'],
                    'Count': ['numeric_dtype'],
                    'Diag 1': ['character_dtype'],
                    'Diag 2': ['character_dtype'],
                    'Department': ['character_dtype'],
                    'Modality': ['character_dtype'],
                    'Component': ['character_dtype'],
                    'Place Of Serv': ['character_dtype'],
                    'Payer Name': ['character_dtype'],
                    'Fin Class': ['character_dtype'],
                    'Rendering': ['character_dtype'],
                    'Referring': ['character_dtype'],
                    'First Bill Dt': ['datetime_dtype'],
                    'Lst Bill Dt': ['datetime_dtype'],
                    'Crt Dt': ['datetime_dtype'],
                    'Created By':['character_dtype'],
                    'Mod Dt': ['datetime_dtype'],
                    'Modified By': ['character_dtype']}

# range_dict = {'line_srvc_cnt': range(0, 150000),
#             'bene_unique_cnt': range(0, 150000),
#             'bene_day_srvc_cnt': range(0, 150000),
#             'average_Medicare_allowed_amt': range(0, 150000),
#             'average_submitted_chrg_amt': range(0, 150000),
#             'average_Medicare_payment_amt': range(0, 150000),
#             'average_Medicare_standard_amt': range(0, 150000)}

# membership_dict = {'run_or_not': False,
#                 'gender': ['MALE', 'FEMALE']}

data_new, out_dict = SF.scrub(data, constraints_dict)