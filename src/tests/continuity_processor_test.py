#!/bin/env python

import unittest
import pandas as pd

import sys
import os

# Grabbing path of processor.py to import into test file
test_dir = os.path.dirname( __file__ )
processor_dir = os.path.join( test_dir, '..', 'scripts')
sys.path.append(processor_dir)

import continuity_processor as processor

class TestStringMethods(unittest.TestCase):

    # checks if function loads data and does so in the correct format 
    def test_read_csv(self):
        self.data = processor.read_csv('../source_data/worker_activity.csv')
        self.assertIsInstance(self.data, pd.DataFrame)

    # passes in test data in README.md and verifies if output from process_data is the same as the results in README.md
    def test_process_data(self):
        self.df = pd.DataFrame({'Worker': [1435, 1435, 1435, 135, 135, 135, 456, 456], 'Employer': [234, 234, 234, 45, 45, 45, 78, 78],
                                'Role': [86, 86, 86, 696, 95, 95, 576, 576], 'Date': ['2021-01-01 17:00:00', '2021-01-04 12:30:00', '2021-01-08 07:00:00', '2021-01-25 18:00:00', '021-01-27 18:00:00', '2021-01-29 22:15:00', '2021-02-02 05:00:00', '2021-11-29 14:30:00']})
        self.processed_df = processor.process_data(self.df)
        self.comparison_df = pd.DataFrame({'Worker': [456, 1435, 135], 'Continuity': [1, 3, 2]})

        pd.testing.assert_frame_equal(self.processed_df, self.comparison_df, check_dtype=False)        

    # checks if final dataframe is being sorted in descending order
    def test_write_csv(self):
        self.df = pd.DataFrame({'Worker': [135, 456, 1435], 'Continuity': [2, 1, 3]})
        self.processed_df = processor.write_csv(self.df)
        self.comparison_df = pd.DataFrame({'Worker': [1435, 135, 456], 'Continuity': [3, 2, 1]})

        pd.testing.assert_frame_equal(self.processed_df.reset_index(drop=True), self.comparison_df.reset_index(drop=True), check_dtype=False)

if __name__ == '__main__':
    unittest.main()