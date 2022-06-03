from os import kill
import pandas as pd
import numpy as np
from datetime import timedelta
from pyspark.sql.functions import *

# reads CSV file worker_activity.csv in the same directory as the script and puts it in a DataFrame
# parse_dates=False as dates are managed as numpy datetime during processing
def read_csv():
    df = pd.read_csv('worker_activity.csv', parse_dates=False)#, index_col=0)
    # sort df to order by Worker
    df = df.sort_values('Worker')
    # change Date format from string to datetime
    df['Date'] = pd.to_datetime(df['Date'])
    worker_list = set(df['Worker'])

    # retrieve ordered list of all dates for each record per worker
    for worker in worker_list:
        # creating a dataframe for each individual worker in the for loop
        worker_df = df[df.Worker == worker]
        # define list of all the dates
        worker_dates = worker_df['Date'].values
        # order list
        worker_dates.sort()
        # inverse list so that we process data from most recent day worked as that is most relevant in terms of continuity
        worker_dates = worker_dates[::-1]

        # defining constants which need to be reset for each worker
        prev_date = np.datetime64('2021-12-01T00:00')
        days_worked = 0

        for date in worker_dates:
        # compare each date with the next one to see if diff > 6

         # calculate activity length by comparing the most recent date with the second most recent date 
            if len(worker_dates) > 1:
                activity_length = np.datetime64(prev_date) - np.datetime64(date)
                activity_length = activity_length.astype('timedelta64[D]')
            else:
                activity_length = np.datetime64('2021-12-01T00:00') - np.datetime64(date)
                activity_length = activity_length.astype('timedelta64[D]')     

            # calculating six_days in timedelta64 - numpy doesn't seem to have a clean way of defining a set number
            # of days as datetime64 which is needed to compare the activity_length
            six_days = np.datetime64(6, 'D') - np.datetime64(0,'Y')
            six_days = six_days.astype('timedelta64[D]')
            
            # if the two dates being compared are within a six day time period, the number of days worked is incremented
            if activity_length <= six_days:
                days_worked += 1
            else:
                continue
            
            print(days_worked)

read_csv()