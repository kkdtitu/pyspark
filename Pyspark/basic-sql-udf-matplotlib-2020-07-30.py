import pyspark
import random
import os
import shutil
import pandas as pd 
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from pyspark.sql.types import FloatType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark import SparkContext
from pyspark import SparkConf

import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

#Python functions
def twice_val (x):
    return 2*x

def hello_name (abc):
    return "Hello " + abc

#Spark UDFs creation from Python functions
udf_twice_val = udf(lambda z: twice_val(z), IntegerType())
udf_hello_name = udf(lambda xyz: hello_name(xyz), StringType())

#spark = SparkSession \
#    .builder \
#    .appName("Python Spark SQL basic example") \
#    .config("spark.some.config.option", "some-value") \
#    .getOrCreate()

ss = pyspark.sql.SparkSession.builder.getOrCreate()
print("Config 1:", ss.sparkContext.getConf().getAll(), "\n")
print("Config 2:", ss, "\n")

source_dict = {'integers': [1, 2, 3], 'floats': [-1.0, 0.5, 2.7], 'integer_arrays': [[1, 2], [3, 4, 5], [6, 7, 8, 9]]}
df_pd_1 = pd.DataFrame(data=source_dict)        #First creating a Pandas DF 
df_ss_1 = ss.createDataFrame(df_pd_1)           #creating a Spark DF from Pandas DF

df_pd_csv = pd.read_csv('biostats-sql-udf-matplotlib.csv')      #First creating a Pandas DF  from csv
df_ss_csv = ss.createDataFrame(df_pd_csv)           #creating a Spark DF from Pandas DF

#First creating a Pandas DF from json. JSON format: https://kanoki.org/2019/12/12/how-to-work-with-json-in-pandas/ 
df_pd_json = pd.read_json('people_orient_columns.json', orient='columns')
df_ss_json = ss.createDataFrame(df_pd_json)           #creating a Spark DF from Pandas DF


if os.path.exists('csv_sql_udf_matplotlib_dir'):
        shutil.rmtree('csv_sql_udf_matplotlib_dir')
if os.path.exists('json_sql_udf_matplotlib_dir'):
        shutil.rmtree('json_sql_udf_matplotlib_dir')

df_ss_csv.write.csv('csv_sql_udf_matplotlib_dir', header=True)
df_ss_json.write.json('json_sql_udf_matplotlib_dir')

#printSchema
print("df_ss_1 schema\n", df_ss_1.printSchema(), "\n")
print("df_ss_csv schema\n", df_ss_csv.printSchema(), "\n")
print("df_ss_json schema\n", df_ss_json.printSchema(), "\n")

#describe
print("df_ss_1 describe\n", df_ss_1.describe(), "\n")
print("df_ss_csv describe\n", df_ss_csv.describe(), "\n")
print("df_ss_json describe\n", df_ss_json.describe(), "\n")

#describe.show
print("df_ss_1 describe show\n", df_ss_1.describe().show(), "\n")
print("df_ss_csv describe show\n", df_ss_csv.describe().show(), "\n")
print("df_ss_json describe show\n", df_ss_json.describe().show(), "\n")

#show
print("df_ss_1 show \n", df_ss_1.show(), "\n")
print("df_ss_csv show \n", df_ss_csv.show(), "\n")
print("df_ss_json show", df_ss_json.show(), "\n")

#count
print("df_ss_1 count \n", df_ss_1.count(), "\n")
print("df_ss_csv count \n", df_ss_csv.count(), "\n")
print("df_ss_json count", df_ss_json.count(), "\n")

#take the first N elements
print("df_ss_csv first 5 rows", df_ss_csv.take(5), "\n")
print("df_ss_json first 5 rows", df_ss_json.take(5), "\n")

#collect -- returns a list of tuples. Each row is a tuple
print("df_ss_1 collect\n", df_ss_1.collect(), "\n")
print("df_ss_csv collect\n", df_ss_csv.collect(), "\n")
print("df_ss_json collect\n", df_ss_json.collect(), "\n")

#converting Spark dataframe column to a list 
list_json = df_ss_json.collect()
for row in list_json:
        print("row_json : ", row.age, row.salary)

#creating two lists list_csv_height,list_csv_weight from spark DF columns
list_csv = df_ss_csv.collect()
list_csv_height = list()
list_csv_weight = []
for row in list_csv:
        print("row_csv : ", row["Height (in)"], row["Weight (lb)"])
        list_csv_height.append(row["Height (in)"])
        list_csv_weight.append(row["Weight (lb)"])
print("list_csv_height: ", list_csv_height)
print("list_csv_weight: ", list_csv_weight)

#Converting Spark DF back to Pandas DF
df_ss_csv_back_to_pd = df_ss_csv.toPandas()
#creating two lists list_csv_height_pd,list_csv_weight_pd from Pandas DF columns
list_csv_height_pd = df_ss_csv_back_to_pd["Height (in)"].tolist()
list_csv_weight_pd = df_ss_csv_back_to_pd["Weight (lb)"].tolist()

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



#Generating the plots using matplotlib (lines, scatter, bar all in one plot)

# plot with x-axis and Y-axis values ... the values should be in 2 lists 
#Method 1: using the lists created from Spark DF columns list_csv_height,list_csv_weight
plt.plot(list_csv_height,list_csv_weight, color='blue') 
plt.scatter(list_csv_height,list_csv_weight, color='green') 
plt.bar(list_csv_height,list_csv_weight, color='yellow') 

plt.title('SS DF Weight vs Height', fontsize=14)
plt.xlabel('Height', fontsize=14)
plt.ylabel('Weight', fontsize=14)
plt.grid(True)
plt.show()
#time.sleep(5)
#PdfPages(r'Charts.pdf').savefig()
plt.close()

#Method 2: using the lists created from Pandas DF columns list_csv_weight_pd, list_csv_height_pd
plt.plot(list_csv_height_pd,list_csv_weight_pd, color='red') 
plt.bar(list_csv_height_pd,list_csv_weight_pd, color='grey')

plt.title('PD DF Weight vs Height', fontsize=14)
plt.xlabel('Height', fontsize=14)
plt.ylabel('Weight', fontsize=14)
plt.grid(True)
plt.show()







