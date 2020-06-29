'''
Simulating erreneous data for scrubbing
'''

import pandas as pd
import numpy as np
from random import sample

# Creating date column between a range
date1 = pd.date_range(start="2018-09-09", end="2020-02-02").tolist()
# Randomly selecting 40 dates
dates_replace = sample(range(len(date1)), 40) 
# Creating 3 values outside our date range to replace the original dates
replace_date = [pd.to_datetime("21-05-2022"), pd.to_datetime("04-29-2033"), pd.to_datetime("2050-06-30")]
# Replacing 40 dates
for i in dates_replace:
    date1[i] = replace_date[sample(range(len(replace_date)), 1)[0]]

data_num_rows = len(date1)

gender = ['MALE', 'FEMALE']
gender_column = []
for i in range(data_num_rows):
    gender_column.append(sample(gender, 1)[0])

gender_replace = sample(range(len(gender_column)), 30) 
replace_gender = ['Male', 'f', 'M', 'Female']

for i in gender_replace:
    gender_column[i] = sample(replace_gender, 1)[0]
    

amount = sample(range(150000), 512)
outliers = [10000000, 999999999, -234, -5000]

replace_amount = sample(range(len(amount)), 25)

for i in replace_amount:
    amount[i] = sample(outliers, 1)[0]

df = pd.DataFrame({'date':date1,
                   'gender': gender_column,
                   'amount': amount})

df.to_csv("scrubbing_sample.csv", index=False)

# --------


