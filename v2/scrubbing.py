'''
Problem this is solving: Data can come in from various customers having varied types. This algorithm allows us to identify
the right data types for a column and then convert it to those types for further use. This algorithm also does reporting
on missing values etc.

Ideal process

1. Data is imported into pandas
2. Data is messy. Most or all columns are interpreted as objects. Our algorithm needs to
    - Assess each columns
    - Determine whether it is date, number or character
    - Convert it and coerce all other values to NAs
    - Report the metrics around this process
3. 
'''