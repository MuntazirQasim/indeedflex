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

    def test_process_data(self):
        self.test_data = [[1435, 234, 86, '2021-01-01 17:00:00'], [1435, 234, 86, '2021-01-04 12:30:00'], [1435, 234, 86, '2021-01-08 07:00:00'], [135, 45, 696, '2021-01-25 18:00:00'], [135, 45, 95, '2021-01-27 18:00:00'], [135, 45, 95, '2021-01-29 22:15:00'], [456, 78, 576, '2021-02-02 05:00:00'], [456, 78, 576, '2021-11-29 14:30:00']]
        self.df = pd.DataFrame(self.test_data, columns=['Worker', 'Employer', 'Role', 'Date'])
        self.processed_df = processor.process_data(self.df)

        self.comparison_data = [[1435, 3], [135, 2], [456, 1]]
        self.comparison_df = pd.DataFrame(self.comparison_data, columns=['Worker', 'Continuity'])

        print(self.processed_df)
        print(self.comparison_df)

        pd.testing.assert_frame_equal(self.processed_df, self.comparison_df)

    def write_csv(self):
        pass

if __name__ == '__main__':
    unittest.main()