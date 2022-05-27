import pandas as pd
from pyspark.sql.functions import *

# reads CSV file worker_activity.csv in the same directory as the script and puts it in a DataFrame
# index_col drops the indexing in the dataframe
# parse_dates=True allows pandas to infer the format of the date 
def read_csv():
    df = pd.read_csv('worker_activity.csv', parse_dates=True, index_col=0)
    df = df.sort_values('Worker')
    print(df.head())

read_csv()