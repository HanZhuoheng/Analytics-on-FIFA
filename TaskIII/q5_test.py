import pyspark
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import *

import sys
sys.path.append('../TaskII')
from q5 import most_popular_nationality

import pandas as pd

def test_most_popular_nationality():
    
    expected_output = pd.DataFrame(
        [['England', 1667],
         ['Germany', 1216],
         ['Spain', 1035],
         ['France', 984],
         ['Argentina', 886]],
        columns = ['nationality', 'count']
    )

    real_output = most_popular_nationality(2020,5)

    pd.testing.assert_frame_equal(
        expected_output,
        real_output,
        check_like=True,
    )