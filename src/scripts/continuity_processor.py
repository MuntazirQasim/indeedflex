#!/bin/env python

import pandas as pd
import numpy as np
from datetime import timedelta

def read_csv(csv_file):
    df = pd.read_csv(csv_file, parse_dates=True)
    df = df.sort_values('Worker')

    return df

def process_data(df):

    worker_list = set(df['Worker'])

    # creating empty dataframe to be populated with final data of workers and continuity to be written into csv
    final_df = pd.DataFrame(columns=['Worker','Continuity'])

    for worker in worker_list:
        worker_df = df[df.Worker == worker]

        worker_dates = worker_df['Date'].values
        worker_dates.sort()
        # inverse list so that we process data from most recent day worked as that is most relevant in terms of continuity
        worker_dates = worker_dates[::-1]

        # prev_date set to 1st DEC to compare most recent day worked to the date where report is run
        prev_date = np.datetime64('2021-12-01T00:00')
        days_worked = 0

        prev_employer = 0
        prev_role = 0

        check_first_iteration = True

        # compare each date with the next one to see if diff > 6
        for date in worker_dates:

            activity_length = np.datetime64(prev_date) - np.datetime64(date)
            activity_length = activity_length.astype('timedelta64[D]')

            # calculating six_days in timedelta64 - numpy doesn't seem to have a clean way of defining a set number of days as datetime64 which is needed to compare the activity_length
            six_days = np.datetime64(6, 'D') - np.datetime64(0,'Y')
            six_days = six_days.astype('timedelta64[D]')

            # if activity_length is more than six days from 1st December then the last day worked should still be processed and continuity should be calculated from that day (i.e. each worker should have Continuity >= 1)
            # check_first_iteration allows us to determine if the most recent day is more than six days from 1st December
            if activity_length > six_days and check_first_iteration == True:
                days_worked += 1
                prev_date = date
                check_first_iteration = False
                continue

            # check_first_iteration is now set to False as the first date is now processed so, if any subsequent activity lengths are greater than six days, continuity should stop being processed
            if check_first_iteration == True:
                check_first_iteration = False
            
            # define dataframes where the date = date in this iteration and worker = worker in this iteration are then compared against employer and role
            employer_and_role_df = df.loc[(df['Date'] == date) & (df['Worker'] == worker)]
            current_role = employer_and_role_df['Role'].values
            current_employer = employer_and_role_df['Employer'].values

            if int(current_role[0]) == int(prev_role) and int(current_employer[0]) == int(prev_employer):
                if activity_length <= six_days:
                    days_worked += 1

            elif int(prev_role) == 0 and int(prev_employer) == 0:
                if activity_length <= six_days:
                    days_worked += 1
                else:
                    break

            else:
                break

            # setting prev variables in preparation for the next loop
            prev_date = date
            prev_employer = current_employer[0]
            prev_role = current_role[0]

        final_df = final_df.append({'Worker': worker,'Continuity': days_worked}, ignore_index=True)

    return final_df

def write_csv(final_df):
    final_df = final_df.sort_values(by=['Continuity'], ascending=False)

    final_df.to_csv('../processed_data/results.csv', index=False)

    return final_df
    

def main():
    df = read_csv('../source_data/worker_activity.csv')
    final_df = process_data(df)
    write_csv(final_df)

if __name__ == "__main__":
    main()