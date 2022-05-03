import pyspark
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import *

import sys
sys.path.append('../TaskII')
from q4_1 import most_popular_nation_positions

import pandas as pd

def test_most_popular_nation_positions():
    
    expected_output = pd.DataFrame(
        [['SUB', 587],
         ['GK', 49],
         ['LCB', 49],
         ['RCB', 49],
         ['LB', 46]],
        columns = ['nation_position', 'count']
    )

    real_output = most_popular_nation_positions(2020,5)

    pd.testing.assert_frame_equal(
        expected_output,
        real_output,
        check_like=True,
    )