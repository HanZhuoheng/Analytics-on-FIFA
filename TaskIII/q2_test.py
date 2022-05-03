import pyspark
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import *

import sys
sys.path.append('../TaskII')
from q2 import clubs_with_most_contracts_until_2021

import pandas as pd

def test_clubs_with_most_contracts_until_2021():
    
    expected_output = pd.DataFrame(
        [['1. FC Kaiserslautern', 18],
         ['FC Ingolstadt 04', 18],
         ['FC Girondins de Bordeaux', 17],
         ['SV Wehen Wiesbaden', 16],
         ['Kasimpaşa SK', 16]],
        columns = ['club', 'count']
    )

    real_output = clubs_with_most_contracts_until_2021(2020,5)

    pd.testing.assert_frame_equal(
        expected_output,
        real_output,
        check_like=True,
    )