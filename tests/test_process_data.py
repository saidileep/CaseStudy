"""
test_etl_job.py
~~~~~~~~~~~~~~~

This module contains unit tests for the transformation steps of the ETL
job defined in etl_job.py. It makes use of a local version of PySpark
that is bundled with the PySpark package.
"""
import unittest

import json

from pyspark.sql.functions import col


class SparkETLTests(unittest.TestCase):
    """Test suite for transformation in process_data.py
    """

    def setUp(self):
        """Start Spark, define config and path to test data
        """
        self.config_data_path = 'configs/params.json'
        self.config = json.loads(self.config_data_path)
        self.spark, *_ = start_spark()
        

    def tearDown(self):
        """Stop Spark
        """
        self.spark.stop()

    def test_data(self):
        """Test data transformer.

        Using small chunks of input data and expected output data, we
        test the transformation step to make sure it's working as
        expected.
        """
        # assemble
        input_data = (
            self.spark
            .read
            .parquet(self.test_data_path + 'employees'))

        # TO DO - some code to test the functionality

        # assert
        self.assertEqual(expected_cols, cols)
        self.assertEqual(expected_rows, rows)
        self.assertEqual(expected_avg_steps, avg_steps)
        self.assertTrue([col in expected_data.columns
                         for col in data_transformed.columns])


if __name__ == '__main__':
    unittest.main()
