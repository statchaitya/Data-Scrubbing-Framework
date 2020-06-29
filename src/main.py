import os
os.chdir("C:/DataScience/Github/Data-Scrubbing-Framework/src/")
import ScrubFunctions as SF
import pandas as pd

data = pd.read_csv("C:/DataScience/scrubbing_sample.csv")

constraints_dict = {'date': ['datetime_dtype'],
                    'gender': ['character_dtype', 'membership_check'],
                    'amount': ['numeric_dtype', 'range_check']}

range_dict = {'amount': range(0, 150000)}
membership_dict = {'gender': ['MALE', 'FEMALE']}

data_new, dtype_com, dtype_err, range_com, range_err, mem_com, mem_err = SF.scrub(data, constraints_dict, range_dict, membership_dict)