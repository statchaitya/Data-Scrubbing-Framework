import boto3
import pandas as pd

scrub_bucket = 'predictable-ml-testing'
fileidcsv = 'fileids_2.csv'

s3_client = boto3.client('s3')
data_s3_object = s3_client.get_object(Bucket=scrub_bucket, Key=fileidcsv)
data = pd.read_csv(data_s3_object['Body'])
data.head()

import datetime

dt_right_now = datetime.datetime.today()
# **** Following parsing should work. If not the logic will fail
# **** Timezones of both the dates should be the same for comparability
fileid_dates = pd.to_datetime(data['upload_date'])
time_differences = fileid_dates - pd.to_datetime(dt_right_now)
min_id = time_differences.idxmin()

most_recent_cid = data.loc[min_id, 'cid']
most_recent_gid = data.loc[min_id, 'gid']
most_recent_wf_id = data.loc[min_id, 'wf_id']

from sagemaker import get_execution_role

role = get_execution_role()
folder = 'working-files'
tr_filename = '_'.join(["wf", str(most_recent_wf_id)]) + ".csv"
data_location = 's3://{}/{}/{}'.format(scrub_bucket, folder, tr_filename)

tr_data = pd.read_csv(data_location)
tr_data.head()


import os
parent_dir = "/home/ec2-user/SageMaker"
os.chdir(parent_dir)
from ScrubbingEngine import ScrubFunctions as SF

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


goal_id = 1
customer_id = 1
batch_date = datetime.datetime.today()

data_new, out_dict = SF.scrub(tr_data, constraints_dict)


od = {key:[str(value)] for key, value in out_dict.items()}
pddf = pd.DataFrame(od)

outgoing_folder = 'scrub-reports'
outgoing_filename = '_'.join(['SR', tr_filename])
pddf.to_csv('s3://{}/{}/{}'.format(scrub_bucket, outgoing_folder, outgoing_filename))