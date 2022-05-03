import pyspark
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import *
import os

#Input: year is the year from 2015 to 2020.
#       x is the number of clubs with most players in that year
#Output: a data frame with club names and counts with top x rows

def clubs_with_most_players(year,x):
    appName = "FIFA"
    master = "local"

    conf = pyspark.SparkConf()\
        .set('spark.driver.host','127.0.0.1')\
        .setAppName(appName)\
        .setMaster(master)

    sc = SparkContext.getOrCreate(conf=conf)

    sqlContext = SQLContext(sc)

    spark = SparkSession.builder.getOrCreate()
    
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
    
    #Filter out the year (2015-2020) then group by the club and count how many players for each club. Order the result descendingly

    if year == 2015:
        df_players = df_read.filter(df_read.club.isNotNull())\
        .filter(df_read.year == '2015')\
        .groupby('club')\
        .count()\
        .sort(desc('count')).toPandas()
    elif year == 2016:
        df_players = df_read.filter(df_read.club.isNotNull())\
        .filter(df_read.year == '2016')\
        .groupby('club')\
        .count()\
        .sort(desc('count')).toPandas()
    elif year == 2017:
        df_players = df_read.filter(df_read.club.isNotNull())\
        .filter(df_read.year == '2017')\
        .groupby('club')\
        .count()\
        .sort(desc('count')).toPandas()
    elif year == 2018:
        df_players = df_read.filter(df_read.club.isNotNull())\
        .filter(df_read.year == '2018')\
        .groupby('club')\
        .count()\
        .sort(desc('count')).toPandas()
    elif year == 2019:
        df_players = df_read.filter(df_read.club.isNotNull())\
        .filter(df_read.year == '2019')\
        .groupby('club')\
        .count()\
        .sort(desc('count')).toPandas()
    elif year == 2020:
        df_players = df_read.filter(df_read.club.isNotNull())\
        .filter(df_read.year == '2020')\
        .groupby('club')\
        .count()\
        .sort(desc('count')).toPandas()
    else:
        print('wrong input')
        
    return df_players.head(x)

print(clubs_with_most_players(2020,5))