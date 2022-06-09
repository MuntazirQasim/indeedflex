from os import kill
import pandas as pd
import numpy as np
from datetime import timedelta
from pyspark.sql.functions import *

# reads CSV file worker_activity.csv in the same directory as the script and puts it in a DataFrame
# parse_dates=False as dates are managed as numpy datetime during processing
def read_csv():
    final_df = pd.DataFrame(columns=['Worker','Continuity'])
    df = pd.read_csv('worker_activity.csv', parse_dates=True)#, index_col=0)
    # sort df to order by Worker
    df = df.sort_values('Worker')
    # change Date format from string to datetime
    #df['Date'] = np.datetime64(df['Date'])
    #df['Date'].astype('timedelta64[D]')
    #df['Date'] = pd.to_datetime(df['Date'])
    worker_list = set(df['Worker'])
    #print(df['Date'])

    #df = df.tail()
    #print(df)

    # retrieve ordered list of all dates for each record per worker
    for worker in worker_list:
        # creating a dataframe for each individual worker in the for loop
        worker_df = df[df.Worker == worker]
        #print(df.head)
        #print(worker_df)
        # define list of all the dates
        worker_dates = worker_df['Date'].values
        # order list
        worker_dates.sort()
        # inverse list so that we process data from most recent day worked as that is most relevant in terms of continuity
        worker_dates = worker_dates[::-1]

        # defining constants which need to be reset for each worker
        # prev_date set to 1st DEC to compare most recent day worked to the date where report is run
        prev_date = np.datetime64('2021-12-01T00:00')
        days_worked = 0

        for date in worker_dates:
        # compare each date with the next one to see if diff > 6

         # calculate activity length by comparing the most recent date with the second most recent date 
            activity_length = np.datetime64(prev_date) - np.datetime64(date)
            activity_length = activity_length.astype('timedelta64[D]')

            # calculating six_days in timedelta64 - numpy doesn't seem to have a clean way of defining a set number
            # of days as datetime64 which is needed to compare the activity_length
            six_days = np.datetime64(6, 'D') - np.datetime64(0,'Y')
            six_days = six_days.astype('timedelta64[D]')

            seven_days = np.datetime64(7, 'D') - np.datetime64(0,'Y')
            seven_days = seven_days.astype('timedelta64[D]')

            prev_employer = 0
            prev_role = 0

            employer_and_role_df = df.loc[(df['Date'] == date) & (df['Worker'] == worker)]
            current_role = employer_and_role_df['Role'].values
            current_employer = employer_and_role_df['Employer'].values

            # if the role or employer of the previous working day and current day aren't equal, activity length is set to
            # greater than six days so that the days_worked isn't incremented in the next step 
            if int(prev_role) == 0 and int(prev_employer) == 0:
                if activity_length <= six_days:
                    days_worked += 1

            elif int(current_role[0]) == int(prev_role) and int(current_employer[0]) == int(prev_employer):
                if activity_length <= six_days:
                    days_worked += 1

            else:
                activity_length = seven_days
                break
            # if the two dates being compared are within a six day time period, the number of days worked is incremented

                ### need this to verify whether this breaks out of the for loop so that it doesn't process 
                ### any more dates for the worker
            prev_date = date
            prev_employer = current_employer
            prev_role = current_role
            #print(days_worked)
            
        final_df = final_df.append({'Worker': worker,'Continuity': days_worked}, ignore_index=True)
        print(final_df)

read_csv()