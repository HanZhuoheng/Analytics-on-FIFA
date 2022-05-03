import pyspark
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import *

import sys
sys.path.append('../TaskII')
from q1 import highest_improvement

import pandas as pd

def test_highest_improvement():
    
    expected_output = pd.DataFrame(
        [['M. Al Buraik', 41.0],
         ['S. Milinković-Savić', 36.2],
         ['E. Pulgar', 35.6],
         ['K. Acosta', 32.6],
         ['R. Mukiibi', 32.6]],
        columns = ['short_name', 'improvement']
    )

    real_output = highest_improvement(5)

    pd.testing.assert_frame_equal(
        expected_output,
        real_output,
        check_like=True,
    )