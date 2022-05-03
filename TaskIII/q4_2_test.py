import pyspark
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import *

import sys
sys.path.append('../TaskII')
from q4_2 import most_popular_team_positions

import pandas as pd

def test_most_popular_team_positions():
    
    expected_output = pd.DataFrame(
        [['SUB', 7820],
         ['RES', 2958],
         ['GK', 662],
         ['LCB', 660],
         ['RCB', 660]],
        columns = ['team_position', 'count']
    )

    real_output = most_popular_team_positions(2020,5)

    pd.testing.assert_frame_equal(
        expected_output,
        real_output,
        check_like=True,
    )
