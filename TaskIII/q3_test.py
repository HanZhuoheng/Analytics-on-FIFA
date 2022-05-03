import pyspark
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import *

import sys
sys.path.append('../TaskII')
from q3 import clubs_with_most_players

import pandas as pd

def test_clubs_with_most_players():
    
    expected_output = pd.DataFrame(
        [['1. FC Union Berlin', 33],
         ['SC Paderborn 07', 33],
         ['Hellas Verona', 33],
         ['Real Valladolid CF', 33],
         ['Manchester United', 33]],
        columns = ['club', 'count']
    )

    real_output = clubs_with_most_players(2020,5)

    pd.testing.assert_frame_equal(
        expected_output,
        real_output,
        check_like=True,
    )