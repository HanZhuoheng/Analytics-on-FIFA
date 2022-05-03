import pyspark
from pyspark.sql import SparkSession, SQLContext
from pyspark.ml.feature import Imputer
from pyspark.sql.functions import *

#This function is used to ingest the dataset contains Soccer player statistics for 2015-2020. 
#Its output 'final_df' is the cleaned, merged FIFA players data from 2015-2020.

def create_data():

    appName = "FIFA"
    master = "local"

    # Create Configuration object for Spark.

    conf = pyspark.SparkConf()\
        .set('spark.driver.host','127.0.0.1')\
        .setAppName(appName)\
        .setMaster(master)

    # Create Spark Context with the new configurations rather than rely on the default one

    sc = SparkContext.getOrCreate(conf=conf)

    # Create the session from the Spark Context

    spark = SparkSession.builder.getOrCreate()

    #Import the 6 datasets

    players15_df = (spark.read
         .format("csv")
         .option("inferSchema", "true")
         .option("header","true")
         .load("../Data/players_15.csv"))

    players16_df = (spark.read
         .format("csv")
         .option("inferSchema", "true")
         .option("header","true")
         .load("../Data/players_16.csv"))

    players17_df = (spark.read
         .format("csv")
         .option("inferSchema", "true")
         .option("header","true")
         .load("../Data/players_17.csv"))

    players18_df = (spark.read
         .format("csv")
         .option("inferSchema", "true")
         .option("header","true")
         .load("../Data/players_18.csv"))

    players19_df = (spark.read
         .format("csv")
         .option("inferSchema", "true")
         .option("header","true")
         .load("../Data/players_19.csv"))

    players20_df = (spark.read
         .format("csv")
         .option("inferSchema", "true")
         .option("header","true")
         .load("../Data/players_20.csv"))

    #Change the data types for each dataset

    casted_types15_df = (players15_df.withColumn("sofifa_id", players15_df["sofifa_id"].cast("string"))
                   .withColumn("dob", to_date(col("dob"), "MM/dd/yyyy"))
                   .withColumn("joined", to_date(col("joined"), "MM/dd/yyyy"))
                   .distinct())
    casted_types15_dropped_df = (casted_types15_df.drop("release_clause_eur","player_tags","loaned_from","nation_jersey_number",
                                        "gk_diving","gk_handling","gk_kicking","gk_reflexes","gk_speed","gk_positioning",
                                        "player_traits","mentality_composure"))

    casted_types16_df = (players16_df.withColumn("sofifa_id", players16_df["sofifa_id"].cast("string"))
                   .withColumn("dob", to_date(col("dob"), "MM/dd/yyyy"))
                   .withColumn("joined", to_date(col("joined"), "MM/dd/yyyy"))
                   .distinct())
    casted_types16_dropped_df = (casted_types16_df.drop("release_clause_eur","player_tags","loaned_from","nation_jersey_number",
                                        "gk_diving","gk_handling","gk_kicking","gk_reflexes","gk_speed","gk_positioning",
                                        "player_traits","mentality_composure"))

    casted_types17_df = (players17_df.withColumn("sofifa_id", players17_df["sofifa_id"].cast("string"))
                   .withColumn("dob", to_date(col("dob"), "MM/dd/yyyy"))
                   .withColumn("joined", to_date(col("joined"), "MM/dd/yyyy"))
                   .distinct())
    casted_types17_dropped_df = (casted_types17_df.drop("release_clause_eur","player_tags","loaned_from","nation_jersey_number",
                                        "gk_diving","gk_handling","gk_kicking","gk_reflexes","gk_speed","gk_positioning",
                                        "player_traits","mentality_composure"))

    casted_types18_df = (players18_df.withColumn("sofifa_id", players18_df["sofifa_id"].cast("string"))
                   .withColumn("dob", to_date(col("dob"), "MM/dd/yyyy"))
                   .withColumn("joined", to_date(col("joined"), "MM/dd/yyyy"))
                   .distinct())
    casted_types18_dropped_df = (casted_types18_df.drop("release_clause_eur","player_tags","loaned_from","nation_jersey_number",
                                        "gk_diving","gk_handling","gk_kicking","gk_reflexes","gk_speed","gk_positioning",
                                        "player_traits","mentality_composure"))

    casted_types19_df = (players19_df.withColumn("sofifa_id", players19_df["sofifa_id"].cast("string"))
                   .withColumn("dob", to_date(col("dob"), "MM/dd/yyyy"))
                   .withColumn("joined", to_date(col("joined"), "MM/dd/yyyy"))
                   .distinct())
    casted_types19_dropped_df = (casted_types19_df.drop("release_clause_eur","player_tags","loaned_from","nation_jersey_number",
                                        "gk_diving","gk_handling","gk_kicking","gk_reflexes","gk_speed","gk_positioning",
                                        "player_traits","mentality_composure"))

    casted_types20_df = (players20_df.withColumn("sofifa_id", players20_df["sofifa_id"].cast("string"))
                   .withColumn("dob", to_date(col("dob"), "MM/dd/yyyy"))
                   .withColumn("joined", to_date(col("joined"), "MM/dd/yyyy"))
                   .distinct())
    casted_types20_dropped_df = (casted_types20_df.drop("release_clause_eur","player_tags","loaned_from","nation_jersey_number",
                                        "gk_diving","gk_handling","gk_kicking","gk_reflexes","gk_speed","gk_positioning",
                                        "player_traits","mentality_composure"))

    #This are the columns that contain missing values with '+' or '-' that needed to be cleaned up.

    columns = ['pace','shooting','passing','dribbling','defending','physic','attacking_crossing','attacking_finishing',
           'attacking_heading_accuracy','attacking_short_passing','attacking_volleys','skill_dribbling','skill_curve',
           'skill_fk_accuracy','skill_long_passing','skill_ball_control','movement_acceleration','movement_sprint_speed',
           'movement_agility','movement_reactions','movement_balance','power_shot_power','power_jumping','power_stamina',
           'power_strength','power_long_shots','mentality_aggression','mentality_interceptions','mentality_positioning',
           'mentality_vision','mentality_penalties','defending_marking','defending_standing_tackle','defending_sliding_tackle',
           'goalkeeping_diving','goalkeeping_handling','goalkeeping_kicking','goalkeeping_positioning','goalkeeping_reflexes',
           'ls','st','rs','lw','lf','cf','rf','rw','lam','cam','ram','lm','lcm','cm','rcm','rm','lwb','ldm','cdm','rdm',
           'rwb','lb','lcb','cb','rcb','rb']

    for c in columns:

    #Create new columns to deal with values like '90+1': 'plus_1' is the left number of '+' and 'plus_2' is the right number of '+'. 
    #Similarly, 'minus_1' is the left number of '-' and 'minus_2' is the right number of '-'.
    #'remain' is the original number when there are no '+' or '-'. 'sumup' is 'plus_1'+'plus_2' and 'diff' is 'diff_1'-'diff_2'. 
    #At last, replace the columns mentioned above with 'sumup', 'diff', or 'remain'.

        plus_1 = c+'_plus_1'
        plus_2 = c+'_plus_2'
        minus_1 = c+'_minus_1'
        minus_2 = c+'_minus_2'
        remain = c+'_remain'
        sumup = c+'_sum'
        diff = c+'_diff'

        casted_types15_dropped_df = casted_types15_dropped_df.withColumn(plus_1, regexp_extract(col(c), '(\w+)(\+)(\w+)', 1).cast('int'))\
	    .withColumn(plus_2, regexp_extract(col(c), '(\w+)(\+)(\w+)', 3).cast('int'))\
	    .withColumn(minus_1, regexp_extract(col(c), '(\w+)(\-)(\w+)', 1).cast('int'))\
	    .withColumn(minus_2, regexp_extract(col(c), '(\w+)(\-)(\w+)', 3).cast('int'))\
	    .withColumn(remain, casted_types15_dropped_df[c].cast('int'))\
	    .withColumn(sumup, col(plus_1)+col(plus_2))\
	    .withColumn(diff, col(minus_1)-col(minus_2))\
	    .na.fill(0,[remain,sumup,diff])\
	    .withColumn(c, when(col(c).isNull() == False, col(remain)+col(sumup)+col(diff)))\
	    .drop(plus_1,plus_2,minus_1,minus_2,remain,sumup,diff)

        casted_types16_dropped_df = casted_types16_dropped_df.withColumn(plus_1, regexp_extract(col(c), '(\w+)(\+)(\w+)', 1).cast('int'))\
	    .withColumn(plus_2, regexp_extract(col(c), '(\w+)(\+)(\w+)', 3).cast('int'))\
	    .withColumn(minus_1, regexp_extract(col(c), '(\w+)(\-)(\w+)', 1).cast('int'))\
	    .withColumn(minus_2, regexp_extract(col(c), '(\w+)(\-)(\w+)', 3).cast('int'))\
	    .withColumn(remain, casted_types16_dropped_df[c].cast('int'))\
	    .withColumn(sumup, col(plus_1)+col(plus_2))\
	    .withColumn(diff, col(minus_1)-col(minus_2))\
	    .na.fill(0,[remain,sumup,diff])\
	    .withColumn(c, when(col(c).isNull() == False, col(remain)+col(sumup)+col(diff)))\
	    .drop(plus_1,plus_2,minus_1,minus_2,remain,sumup,diff)

        casted_types17_dropped_df = casted_types17_dropped_df.withColumn(plus_1, regexp_extract(col(c), '(\w+)(\+)(\w+)', 1).cast('int'))\
	    .withColumn(plus_2, regexp_extract(col(c), '(\w+)(\+)(\w+)', 3).cast('int'))\
	    .withColumn(minus_1, regexp_extract(col(c), '(\w+)(\-)(\w+)', 1).cast('int'))\
	    .withColumn(minus_2, regexp_extract(col(c), '(\w+)(\-)(\w+)', 3).cast('int'))\
	    .withColumn(remain, casted_types17_dropped_df[c].cast('int'))\
	    .withColumn(sumup, col(plus_1)+col(plus_2))\
	    .withColumn(diff, col(minus_1)-col(minus_2))\
	    .na.fill(0,[remain,sumup,diff])\
	    .withColumn(c, when(col(c).isNull() == False, col(remain)+col(sumup)+col(diff)))\
	    .drop(plus_1,plus_2,minus_1,minus_2,remain,sumup,diff)

        casted_types18_dropped_df = casted_types18_dropped_df.withColumn(plus_1, regexp_extract(col(c), '(\w+)(\+)(\w+)', 1).cast('int'))\
	    .withColumn(plus_2, regexp_extract(col(c), '(\w+)(\+)(\w+)', 3).cast('int'))\
	    .withColumn(minus_1, regexp_extract(col(c), '(\w+)(\-)(\w+)', 1).cast('int'))\
	    .withColumn(minus_2, regexp_extract(col(c), '(\w+)(\-)(\w+)', 3).cast('int'))\
	    .withColumn(remain, casted_types18_dropped_df[c].cast('int'))\
	    .withColumn(sumup, col(plus_1)+col(plus_2))\
	    .withColumn(diff, col(minus_1)-col(minus_2))\
	    .na.fill(0,[remain,sumup,diff])\
	    .withColumn(c, when(col(c).isNull() == False, col(remain)+col(sumup)+col(diff)))\
	    .drop(plus_1,plus_2,minus_1,minus_2,remain,sumup,diff)

        casted_types19_dropped_df = casted_types19_dropped_df.withColumn(plus_1, regexp_extract(col(c), '(\w+)(\+)(\w+)', 1).cast('int'))\
	    .withColumn(plus_2, regexp_extract(col(c), '(\w+)(\+)(\w+)', 3).cast('int'))\
	    .withColumn(minus_1, regexp_extract(col(c), '(\w+)(\-)(\w+)', 1).cast('int'))\
	    .withColumn(minus_2, regexp_extract(col(c), '(\w+)(\-)(\w+)', 3).cast('int'))\
	    .withColumn(remain, casted_types19_dropped_df[c].cast('int'))\
	    .withColumn(sumup, col(plus_1)+col(plus_2))\
	    .withColumn(diff, col(minus_1)-col(minus_2))\
	    .na.fill(0,[remain,sumup,diff])\
	    .withColumn(c, when(col(c).isNull() == False, col(remain)+col(sumup)+col(diff)))\
	    .drop(plus_1,plus_2,minus_1,minus_2,remain,sumup,diff)

        casted_types20_dropped_df = casted_types20_dropped_df.withColumn(plus_1, regexp_extract(col(c), '(\w+)(\+)(\w+)', 1).cast('int'))\
	    .withColumn(plus_2, regexp_extract(col(c), '(\w+)(\+)(\w+)', 3).cast('int'))\
	    .withColumn(minus_1, regexp_extract(col(c), '(\w+)(\-)(\w+)', 1).cast('int'))\
	    .withColumn(minus_2, regexp_extract(col(c), '(\w+)(\-)(\w+)', 3).cast('int'))\
	    .withColumn(remain, casted_types20_dropped_df[c].cast('int'))\
	    .withColumn(sumup, col(plus_1)+col(plus_2))\
	    .withColumn(diff, col(minus_1)-col(minus_2))\
	    .na.fill(0,[remain,sumup,diff])\
	    .withColumn(c, when(col(c).isNull() == False, col(remain)+col(sumup)+col(diff)))\
	    .drop(plus_1,plus_2,minus_1,minus_2,remain,sumup,diff)

    #Impute the missing values using imputer function with mean

    filled15_df = casted_types15_dropped_df.fillna(0,[c for c in columns])
    filled16_df = casted_types16_dropped_df.fillna(0,[c for c in columns])
    filled17_df = casted_types17_dropped_df.fillna(0,[c for c in columns])
    filled18_df = casted_types18_dropped_df.fillna(0,[c for c in columns])
    filled19_df = casted_types19_dropped_df.fillna(0,[c for c in columns])
    filled20_df = casted_types20_dropped_df.fillna(0,[c for c in columns])

    imputer = Imputer (
        inputCols=[c for c in columns],
        outputCols=["{}_imputed".format(c) for c in columns]
        ).setStrategy("mean").setMissingValue(0)

    imputed15_df = imputer.fit(filled15_df).transform(filled15_df)
    imputed16_df = imputer.fit(filled16_df).transform(filled16_df)
    imputed17_df = imputer.fit(filled17_df).transform(filled17_df)
    imputed18_df = imputer.fit(filled18_df).transform(filled18_df)
    imputed19_df = imputer.fit(filled19_df).transform(filled19_df)
    imputed20_df = imputer.fit(filled20_df).transform(filled20_df)

    final15_df = imputed15_df.drop(*columns)
    final16_df = imputed16_df.drop(*columns)
    final17_df = imputed17_df.drop(*columns)
    final18_df = imputed18_df.drop(*columns)
    final19_df = imputed19_df.drop(*columns)
    final20_df = imputed20_df.drop(*columns)

    #Merge all datasets into one dataset for Task II

    final15_with_year_column_df = final15_df.withColumn('year', lit('2015'))
    final16_with_year_column_df = final16_df.withColumn('year', lit('2016'))
    final17_with_year_column_df = final17_df.withColumn('year', lit('2017'))
    final18_with_year_column_df = final18_df.withColumn('year', lit('2018'))
    final19_with_year_column_df = final19_df.withColumn('year', lit('2019')) 
    final20_with_year_column_df = final20_df.withColumn('year', lit('2020'))

    final_df = final15_with_year_column_df.union(final16_with_year_column_df)\
                .union(final17_with_year_column_df)\
                .union(final18_with_year_column_df)\
                .union(final19_with_year_column_df)\
                .union(final20_with_year_column_df)
    
    # populate the table with our data.

    db_properties={}
    db_properties['username']="postgres"
    db_properties['password']="postgres"
    db_properties['url']= "jdbc:postgresql://localhost:5432/postgres"
    db_properties['driver']="org.postgresql.Driver"

    final_df.write.format("jdbc")\
    .mode("overwrite")\
    .option("url", "jdbc:postgresql://localhost:5432/postgres")\
    .option("dbtable", "fifa.players")\
    .option("user", "postgres")\
    .option("password", "Hzh@981214")\ 
    .option("Driver", "org.postgresql.Driver")\
    .save()

    final_df.printSchema()

create_data()