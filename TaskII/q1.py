import pyspark
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import *
import os
import pandas as pd

#Input: x is the total number of players who achieved average highest improvement across all skillsets.
#Output: a data frame with names and improvement with top x rows

def highest_improvement(x):

    appName = "FIFA"
    master = "local"

    conf = pyspark.SparkConf()\
        .set('spark.driver.host','127.0.0.1')\
        .setAppName(appName)\
        .setMaster(master)

    sc = SparkContext.getOrCreate(conf=conf)

    sqlContext = SQLContext(sc)

    spark = SparkSession.builder.getOrCreate()

    #read data back from database table via PgAdmin

    db_properties={}
    db_properties['username']="postgres"
    db_properties['password']="postgres"
    db_properties['url']= "jdbc:postgresql://localhost:5432/postgres"
    db_properties['driver']="org.postgresql.Driver"

    df_read=sqlContext.read.format("jdbc")\
    .option("url", "jdbc:postgresql://localhost:5432/postgres")\
    .option("dbtable", "fifa.players")\
    .option("user", "postgres")\
    .option("password", "Hzh@981214")\
    .option("Driver", "org.postgresql.Driver")\
    .load()

    #Filter out the year 2015 and 2020 and calculate the mean of skillsets for those 2 years
    
    df_read_15=df_read.filter(df_read.year == '2015')\
        .select('sofifa_id','short_name',((col('skill_dribbling_imputed')+col('skill_curve_imputed')
                                          +col('skill_fk_accuracy_imputed')+col('skill_long_passing_imputed')
                                          +col('skill_ball_control_imputed'))/5).alias('mean'))

    df_read_20=df_read.filter(df_read.year == '2020')\
        .select('sofifa_id','short_name',((col('skill_dribbling_imputed')+col('skill_curve_imputed')
                                          +col('skill_fk_accuracy_imputed')+col('skill_long_passing_imputed')
                                          +col('skill_ball_control_imputed'))/5).alias('mean'))

    #Joined two datasets above and calculate the improvement by differencing the mean of skills of 2020 and the mean of skills of 2015.

    df_improvement=df_read_15.join(df_read_20, df_read_15.sofifa_id == df_read_20.sofifa_id, 'inner')\
        .select(df_read_15.short_name,bround(df_read_20.mean-df_read_15.mean,1).alias('improvement'))\
        .sort(desc('improvement')).toPandas()
   
    return df_improvement.head(x)

print(highest_improvement(5))