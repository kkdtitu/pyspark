import pyspark
import random
import os
import shutil
import pandas as pd 
import time
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark import SparkConf

#from pyspark.sql.functions import udf
import pyspark.sql.functions as func
from pyspark.sql.window import Window

from pyspark.sql.types import *
from pyspark.sql.types import FloatType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType


import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

#Python functions 
def twice_val (x):
    return 2*x

def hello_name (abc):
    return "Hello " + abc

#Spark UDFs creation from Python functions
#udf_twice_val = udf(lambda z: twice_val(z), IntegerType())
#udf_hello_name = udf(lambda xyz: hello_name(xyz), StringType())
udf_twice_val = func.udf(lambda z: twice_val(z), IntegerType())
udf_hello_name = func.udf(lambda xyz: hello_name(xyz), StringType())

#spark = SparkSession \
#    .builder \
#    .appName("Python Spark SQL basic example") \
#    .config("spark.some.config.option", "some-value") \
#    .getOrCreate()

ss = pyspark.sql.SparkSession.builder.getOrCreate()
print("Config 1:", ss.sparkContext.getConf().getAll(), "\n")
print("Config 2:", ss, "\n")


df_pd_csv = pd.read_csv('biostats-sql-udf-matplotlib.csv')      #First creating a Pandas DF  from csv
df_ss_csv = ss.createDataFrame(df_pd_csv)           #creating a Spark DF from Pandas DF

#First creating a Pandas DF from json. JSON format: https://kanoki.org/2019/12/12/how-to-work-with-json-in-pandas/ 
df_pd_json = pd.read_json('people_orient_columns.json', orient='columns')
df_ss_json = ss.createDataFrame(df_pd_json)           #creating a Spark DF from Pandas DF


#count
print("df_ss_csv count \n", df_ss_csv.count(), "\n")
print("df_ss_json count", df_ss_json.count(), "\n")


#selecting specific columns
print("df_ss_csv select\n", df_ss_csv.select(df_ss_csv['Name'], df_ss_csv['Age (yrs)'], df_ss_csv['Sex']).show(), "\n")
print("df_ss_json select\n", df_ss_json.select(df_ss_json['name'], df_ss_json['age'], df_ss_json['sex']).show(), "\n")

#selecting specific columns alternate method
print("df_ss_csv select alt\n", df_ss_csv.select('Name', 'Age (yrs)', 'Sex').show(), "\n")
print("df_ss_json select alt\n", df_ss_json.select('name', 'age', 'sex').show(), "\n")

#selecting specific columns, dropDuplicates and sort
print("df_ss_csv select dropDuplicates and sort\n", df_ss_csv.select('Name').dropDuplicates().sort('Name').show(), "\n")
print("df_ss_json select dropDuplicates and sort\n", df_ss_json.select('name').dropDuplicates().sort('name').show(), "\n")

#Calling Spark UDFs
#csv
df_ss_csv_new_1 = df_ss_csv.select("Name", udf_hello_name("Name").alias("Hello Name"), "Age (yrs)", udf_twice_val("Age (yrs)").alias("Age doubled !"))
#json, string single quote
df_ss_json_new_sq_1 = df_ss_json.select('name', udf_hello_name('name').alias('Hello name'), 'salary', udf_twice_val('salary').alias('salary doubled !'))
#json, string double quote
df_ss_json_new_dq_1 = df_ss_json.select("name", udf_hello_name("name").alias("Hello name"), "salary", udf_twice_val("salary").alias("salary doubled !"))
print("df_ss_csv_new_1 \n", df_ss_csv_new_1.show() ,"\n")
print("df_ss_json_new_sq_1 \n", df_ss_json_new_sq_1.show() ,"\n")
print("df_ss_json_new_dq_1 \n", df_ss_json_new_dq_1.show() ,"\n")

#Using Spark UDFs to add columns to a Spark DF
#csv
df_ss_csv_new_2 = df_ss_csv.withColumn("Hello Name", udf_hello_name("Name"))
#json, string single quote
df_ss_json_new_2 = df_ss_json.withColumn('Salary Doubled !', udf_twice_val('salary'))
print("df_ss_csv_new_2 \n", df_ss_csv_new_2.show() ,"\n")
print("df_ss_json_new_2 \n", df_ss_json_new_2.show() ,"\n")


#Window functions

#1: Aggregate functions 

# Max
Windowspec_json=Window.partitionBy(df_ss_json["sex"]).orderBy(df_ss_json["salary"].asc()).rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)
df_ss_window_json_max_in_partition = df_ss_json.withColumn("Max Salary within M/F category", func.max(df_ss_json["salary"]).over(Windowspec_json))
print("Aggregate func df_ss_window_json_max_in_partition \n", df_ss_window_json_max_in_partition.show() ,"\n")
#Max ALT
Windowspec_json=Window.partitionBy("sex").orderBy(df_ss_json["salary"].desc()).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
df_ss_window_json_max_in_partition = df_ss_json.withColumn("Max Salary within M/F category", func.max("salary").over(Windowspec_json))
print("Aggregate func df_ss_window_json_max_in_partition ALT \n", df_ss_window_json_max_in_partition.show() ,"\n")

#Min
Windowspec_json=Window.partitionBy(df_ss_json["sex"]).orderBy(df_ss_json["salary"].asc()).rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)
df_ss_window_json_min_in_partition = df_ss_json.withColumn("Min Salary within M/F category", func.min("salary").over(Windowspec_json))
print("Aggregate func df_ss_window_json_min_in_partition \n", df_ss_window_json_min_in_partition.show() ,"\n")
#Min Alt
Windowspec_json=Window.partitionBy(df_ss_json["sex"]).orderBy(df_ss_json["salary"].asc()).rowsBetween(-1000, 1000)
df_ss_window_json_min_in_partition = df_ss_json.withColumn("Min Salary within M/F category", func.min("salary").over(Windowspec_json))
print("Aggregate func df_ss_window_json_min_in_partition ALT \n", df_ss_window_json_min_in_partition.show() ,"\n")

#Avg
Windowspec_json=Window.partitionBy(df_ss_json["sex"]).orderBy(df_ss_json["salary"].asc()).rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)
df_ss_window_json_avg_in_partition = df_ss_json.withColumn("Avg Salary within M/F category", func.avg("salary").over(Windowspec_json))
print("Aggregate func df_ss_window_json_avg_in_partition \n", df_ss_window_json_avg_in_partition.show() ,"\n")
#Avg ALT
Windowspec_json=Window.partitionBy(df_ss_json["sex"]).orderBy(df_ss_json["salary"].asc()).rowsBetween(-1000, 1000)
df_ss_window_json_avg_in_partition = df_ss_json.withColumn("Avg Salary within M/F category", func.avg("salary").over(Windowspec_json))
print("Aggregate func df_ss_window_json_avg_in_partition ALT\n", df_ss_window_json_avg_in_partition.show() ,"\n")

#Sum
Windowspec_json=Window.partitionBy(df_ss_json["sex"]).orderBy(df_ss_json["salary"].asc()).rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)
df_ss_window_json_sum_in_partition = df_ss_json.withColumn("Sum Salary within M/F category", func.sum("salary").over(Windowspec_json))
print("Aggregate func df_ss_window_json_sum_in_partition \n", df_ss_window_json_sum_in_partition.show() ,"\n")
#Sum ALT
Windowspec_json=Window.partitionBy(df_ss_json["sex"]).orderBy(df_ss_json["salary"].asc()).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
df_ss_window_json_sum_in_partition = df_ss_json.withColumn("Sum Salary within M/F category", func.sum("salary").over(Windowspec_json))
print("Aggregate func df_ss_window_json_sum_in_partition \n", df_ss_window_json_sum_in_partition.show() ,"\n")

#Count
Windowspec_json=Window.partitionBy(df_ss_json["sex"]).orderBy(df_ss_json["salary"].asc()).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
df_ss_window_json_count_in_partition = df_ss_json.withColumn("Count within M/F category", func.count("salary").over(Windowspec_json))
print("Aggregate func df_ss_window_json_count_in_partition \n", df_ss_window_json_count_in_partition.show() ,"\n")
#Count ALT
Windowspec_json=Window.partitionBy(df_ss_json["sex"]).orderBy(df_ss_json["salary"].asc()).rowsBetween(-20, 20)
df_ss_window_json_count_in_partition = df_ss_json.withColumn("Count within M/F category", func.count("salary").over(Windowspec_json))
print("Aggregate func df_ss_window_json_count_in_partition \n", df_ss_window_json_count_in_partition.show() ,"\n")


#2: Ranking functions

#rank
Windowspec_json=Window.partitionBy("sex").orderBy(df_ss_json["salary"].desc())
df_ss_window_json_rank_in_partition = df_ss_json.withColumn("Rank based on Salary within M/F category", func.rank().over(Windowspec_json))
print("Ranking func df_ss_window_json_rank_in_partition \n", df_ss_window_json_rank_in_partition.show() ,"\n")
#rank ALT
Windowspec_json=Window.partitionBy("sex").orderBy(df_ss_json["salary"].desc()).rowsBetween(Window.unboundedPreceding, 0)
df_ss_window_json_rank_in_partition = df_ss_json.withColumn("Rank based on Salary within M/F category", func.rank().over(Windowspec_json))
print("Ranking func df_ss_window_json_rank_in_partition ALT \n", df_ss_window_json_rank_in_partition.show() ,"\n")

#dense rank
Windowspec_json=Window.partitionBy("sex").orderBy(df_ss_json["salary"].desc())
df_ss_window_json_dense_rank_in_partition = df_ss_json.withColumn("Dense Rank based on Salary within M/F category", func.dense_rank().over(Windowspec_json))
print("Ranking func df_ss_window_json_dense_rank_in_partition \n", df_ss_window_json_dense_rank_in_partition.show() ,"\n")
#dense rank ALT
Windowspec_json=Window.partitionBy("sex").orderBy(df_ss_json["salary"].desc()).rowsBetween(Window.unboundedPreceding, 0)
df_ss_window_json_dense_rank_in_partition = df_ss_json.withColumn("Dense Rank based on Salary within M/F category", func.dense_rank().over(Windowspec_json))
print("Ranking func df_ss_window_json_dense_rank_in_partition ALT \n", df_ss_window_json_dense_rank_in_partition.show() ,"\n")

#percent rank
Windowspec_json=Window.partitionBy("sex").orderBy(df_ss_json["salary"].desc())
df_ss_window_json_percent_rank_in_partition = df_ss_json.withColumn("Percent Rank based on Salary within M/F category", func.percent_rank().over(Windowspec_json))
print("Ranking func df_ss_window_json_percent_rank_in_partition \n", df_ss_window_json_percent_rank_in_partition.show() ,"\n")
#percent rank ALT
Windowspec_json=Window.partitionBy("sex").orderBy(df_ss_json["salary"].desc()).rowsBetween(Window.unboundedPreceding, 0)
df_ss_window_json_percent_rank_in_partition = df_ss_json.withColumn("Percent Rank based on Salary within M/F category", func.percent_rank().over(Windowspec_json))
print("Ranking func df_ss_window_json_percent_rank_in_partition ALT \n", df_ss_window_json_percent_rank_in_partition.show() ,"\n")

#ntile
Windowspec_json=Window.partitionBy("sex").orderBy(df_ss_json["salary"].desc())
df_ss_window_json_ntile_in_partition = df_ss_json.withColumn("Ntile based on Salary within M/F category", func.ntile(2).over(Windowspec_json))
print("Ranking func df_ss_window_json_ntile_in_partition N=2 \n", df_ss_window_json_ntile_in_partition.show() ,"\n")
#ntile ALT
Windowspec_json=Window.partitionBy("sex").orderBy(df_ss_json["salary"].desc()).rowsBetween(Window.unboundedPreceding, 0)
df_ss_window_json_ntile_in_partition = df_ss_json.withColumn("Ntile based on Salary within M/F category", func.ntile(2).over(Windowspec_json))
print("Ranking func df_ss_window_json_ntile_in_partition N=2 ALT \n", df_ss_window_json_ntile_in_partition.show() ,"\n")

#row number
Windowspec_json=Window.partitionBy("sex").orderBy(df_ss_json["salary"].desc())
df_ss_window_json_row_number_in_partition = df_ss_json.withColumn("Row number based on Salary within M/F category", func.row_number().over(Windowspec_json))
print("Ranking func df_ss_window_json_row_number_in_partition \n", df_ss_window_json_row_number_in_partition.show() ,"\n")
#row number ALT
Windowspec_json=Window.partitionBy("sex").orderBy(df_ss_json["salary"].desc()).rowsBetween(Window.unboundedPreceding, 0)
df_ss_window_json_row_number_in_partition = df_ss_json.withColumn("Row number based on Salary within M/F category", func.row_number().over(Windowspec_json))
print("Ranking func df_ss_window_json_row_number_in_partition ALT \n", df_ss_window_json_row_number_in_partition.show() ,"\n")


#2: Analytical functions
#lag
Windowspec_json=Window.partitionBy(df_ss_json["sex"]).orderBy(df_ss_json["salary"].desc()) 
df_ss_window_json_lag_in_partition = df_ss_json.withColumn("Lag based on Salary within M/F category", func.lag(df_ss_json["salary"]).over(Windowspec_json))
print("Ranking func df_ss_window_json_lag_in_partition \n", df_ss_window_json_lag_in_partition.show() ,"\n")
#lag ALT
Windowspec_json=Window.partitionBy("sex").orderBy(df_ss_json["salary"].desc()).rowsBetween(-1, -1)
df_ss_window_json_lag_in_partition_1 = df_ss_json.withColumn("Lag based on Salary within M/F category", func.lag("salary").over(Windowspec_json))
print("Ranking func df_ss_window_json_lag_in_partition ALT \n", df_ss_window_json_lag_in_partition_1.show() ,"\n")

#lead
Windowspec_json=Window.partitionBy(df_ss_json["sex"]).orderBy(df_ss_json["salary"].desc()) 
df_ss_window_json_lead_in_partition = df_ss_json.withColumn("Lead based on Salary within M/F category", func.lead(df_ss_json["salary"]).over(Windowspec_json))
print("Ranking func df_ss_window_json_lead_in_partition \n", df_ss_window_json_lead_in_partition.show() ,"\n")
#lead ALT
Windowspec_json=Window.partitionBy("sex").orderBy(df_ss_json["salary"].desc()).rowsBetween(1, 1)
df_ss_window_json_lead_in_partition_1 = df_ss_json.withColumn("Lead based on Salary within M/F category", func.lead("salary").over(Windowspec_json))
print("Ranking func df_ss_window_json_lead_in_partition ALT \n", df_ss_window_json_lead_in_partition_1.show() ,"\n")

#first_value
Windowspec_json=Window.partitionBy(df_ss_json["sex"]).orderBy(df_ss_json["salary"].desc())
df_ss_window_json_first_in_partition = df_ss_json.withColumn("First based on Salary within M/F category", func.first(df_ss_json["salary"]).over(Windowspec_json))
print("Ranking func df_ss_window_json_first_in_partition \n", df_ss_window_json_first_in_partition.show() ,"\n")
#first_value ALT 1
Windowspec_json=Window.partitionBy("sex").orderBy(df_ss_json["salary"].desc()).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
df_ss_window_json_first_in_partition_1 = df_ss_json.withColumn("First based on Salary within M/F category", func.first("salary").over(Windowspec_json))
print("Ranking func df_ss_window_json_first_in_partition ALT 1 \n", df_ss_window_json_first_in_partition_1.show() ,"\n")
#first_value ALT 2
Windowspec_json=Window.partitionBy("sex").orderBy(df_ss_json["salary"].desc()).rowsBetween(Window.unboundedPreceding, 0)
df_ss_window_json_first_in_partition_1 = df_ss_json.withColumn("First based on Salary within M/F category", func.first("salary").over(Windowspec_json))
print("Ranking func df_ss_window_json_first_in_partition ALT 2 \n", df_ss_window_json_first_in_partition_1.show() ,"\n")

#last_value
Windowspec_json=Window.partitionBy(df_ss_json["sex"]).orderBy(df_ss_json["salary"].desc()).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
df_ss_window_json_last_in_partition = df_ss_json.withColumn("Last based on Salary within M/F category", func.last(df_ss_json["salary"]).over(Windowspec_json))
print("Ranking func df_ss_window_json_last_in_partition \n", df_ss_window_json_last_in_partition.show() ,"\n")
#last_value ALT
Windowspec_json=Window.partitionBy("sex").orderBy(df_ss_json["salary"].desc()).rowsBetween(0,Window.unboundedFollowing)
df_ss_window_json_last_in_partition = df_ss_json.withColumn("Last based on Salary within M/F category", func.last("salary").over(Windowspec_json))
print("Ranking func df_ss_window_json_last_in_partition ALT \n", df_ss_window_json_last_in_partition.show() ,"\n")


