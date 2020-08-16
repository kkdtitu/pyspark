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

#Alternae way of creatimng UDFs
@udf
def udf_square_val (x):
        return x*x

@udf
def udf_hey_name (abc):
        return "Hey " + abc

# **** sc will have to be created BEFORE ss, else ERROR
sc = pyspark.SparkContext()

#spark = SparkSession \
#    .builder \
#    .appName("Python Spark SQL basic example") \
#    .config("spark.some.config.option", "some-value") \
#    .getOrCreate()

# **** ss will have to be created AFTER sc, else ERROR
ss = pyspark.sql.SparkSession.builder.getOrCreate()

print("Config sc:", sc, "\n")
print("Config ss 1:", ss.sparkContext.getConf().getAll(), "\n")
print("Config ss 2:", ss, "\n")



#RDDs to/from Spark DF (Schema inferred at READ in these examples)
rdd1 = sc.parallelize([("KKD","M",5.5), \
        ("Neepa","F",5.0), \
        ("Ronak","M",5.1),\
        ("Ronik", "M", 4.4)])
print("RDD1 :", rdd1.collect(), "\n RDD1 count:", rdd1.count())

srcFile="/Users/ronakronik/Documents/KKD/Technical/pyspark/Pyspark/people_orient_columns.txt"
rdd2 = sc.textFile(srcFile, 4).cache()   #.cache() is optional, number of RDD partitions = 4 is also optional
print("RDD2 :", rdd2.collect(), "\n RDD2 count:", rdd2.count())  #Each rdd2 element == line in srcFile. Each rdd2 element is a string 
rdd3 = rdd2.map(lambda x: x.split(",")).map(lambda x: tuple(x)) #first convert each rdd2 string element to rdd2 list using in-built split function; 
                                                                #then convert each rdd2 list element to rdd2 tuple element using in-built tuple function                                                             #  
print("RDD3 :", rdd3.collect(), "\n RDD3 count:", rdd3.count()) #so rdd3 is a list of tuples

rdd4 = rdd2.map(lambda x: x.split(",")).map(tuple)    #in-built tuple function applies to each rdd list element / converts to an rdd tuple element 
                                                      # using .map(func) is a more efficient form than .map(lambda x: func(x))
print("RDD4 :", rdd4.collect(), "\n RDD4 count:", rdd4.count()) #so rdd4 is a list of tuples

#RDD to Spark DF
df_ss_from_rdd1 = rdd1.toDF(["Name","Sex","Height"])  #the RDD needs to be a list of tuples, where each tuple represents a row
print("df_ss_from_rdd1.printSchema() :", df_ss_from_rdd1.printSchema())
print("df_ss_from_rdd1.show() :", df_ss_from_rdd1.show())
print("df_ss_from_rdd1.collect() :", df_ss_from_rdd1.collect())

df_ss_from_rdd3 = rdd3.toDF(["Name","Sex","Height","Country","State"]) #the RDD needs to be a list of tuples, where each tuple represents a row
print("df_ss_from_rdd3.printSchema() :", df_ss_from_rdd3.printSchema())
print("df_ss_from_rdd3.show() :", df_ss_from_rdd3.show())
print("df_ss_from_rdd3.collect() :", df_ss_from_rdd3.collect())

df_ss_from_rdd4 = rdd4.toDF(["Name","Sex","Height","Country","State"]) #the RDD needs to be a list of tuples, where each tuple represents a row
print("df_ss_from_rdd4.printSchema() :", df_ss_from_rdd4.printSchema())
print("df_ss_from_rdd4.show() :", df_ss_from_rdd4.show())
print("df_ss_from_rdd4.collect() :", df_ss_from_rdd4.collect())

#Spark DF back to RDD
print("df_ss_from_rdd1.rdd :", df_ss_from_rdd1.rdd.collect())  #df_ss_from_rdd1.rdd creates RDD of row objects
print("df_ss_from_rdd3.rdd :", df_ss_from_rdd3.rdd.collect())  #df_ss_from_rdd3.rdd creates RDD of row objects
print("df_ss_from_rdd4.rdd :", df_ss_from_rdd4.rdd.collect())   #df_ss_from_rdd3.rdd creates RDD of row objects
print("df_ss_from_rdd1.rdd.map(list) :", df_ss_from_rdd1.rdd.map(list).collect())   #.map(list) uses in-built list function to convert each row-object element to list
print("df_ss_from_rdd3.rdd.map(list) :", df_ss_from_rdd3.rdd.map(list).collect())
print("df_ss_from_rdd4.rdd.map(list) :", df_ss_from_rdd4.rdd.map(list).collect())
print("df_ss_from_rdd1.rdd.map(tuple) :", df_ss_from_rdd1.rdd.map(tuple).collect())
print("df_ss_from_rdd3.rdd.map(tuple) :", df_ss_from_rdd3.rdd.map(tuple).collect())
print("df_ss_from_rdd4.rdd.map(tuple) :", df_ss_from_rdd4.rdd.map(tuple).collect())

# Pandas to/from Spark DF (Schema inferred at READ in these examples)
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

#collect -- returns a list of row objects / tuples. Each row is a row object / tuple
print("df_ss_1 collect\n", df_ss_1.collect(), "\n")
print("df_ss_csv collect\n", df_ss_csv.collect(), "\n")
print("df_ss_json collect\n", df_ss_json.collect(), "\n")


#Method 1 : Creating lists for matplotlib.pyplot / plt
#converting Spark dataframe column to a list 
list_json = df_ss_json.collect()
for row in list_json:
        print("row_json : ", row.age, row.salary)

#creating two lists list_csv_height,list_csv_weight from spark DF columns. Lists will be required in matplotlib plots
list_csv = df_ss_csv.collect()
list_csv_height = list()
list_csv_weight = []
for row in list_csv:
        print("row_csv : ", row["Height (in)"], row["Weight (lb)"])
        list_csv_height.append(row["Height (in)"])
        list_csv_weight.append(row["Weight (lb)"])
print("list_csv_height: ", list_csv_height)
print("list_csv_weight: ", list_csv_weight)

#Method 2 : Creating lists for matplotlib.pyplot / plt
#Converting Spark DF back to Pandas DF
df_ss_csv_back_to_pd = df_ss_csv.toPandas()
#creating two lists list_csv_height_pd,list_csv_weight_pd from Pandas DF columns. Lists will be required in matplotlib plots
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


#Using ALT Spark UDFs to add columns to a Spark DF
#csv
df_ss_csv_new_3 = df_ss_csv.withColumn("Age Squared", udf_square_val("Age (yrs)"))
print("df_ss_csv_new_3 \n", df_ss_csv_new_3.show() ,"\n")
#json, string single quote
df_ss_json_new_3 = df_ss_json.withColumn('Salary Squared !', udf_square_val('salary'))
print("df_ss_json_new_3 \n", df_ss_json_new_3.show() ,"\n")

#Using ALT Spark UDFs 
#csv
df_ss_csv_new_4 = df_ss_csv.select("Name", udf_hey_name("Name").alias("Hey Name"), "Age (yrs)", udf_square_val("Age (yrs)").alias("Age Squared !"))
#json, string single quote
df_ss_json_new_sq_4 = df_ss_json.select('name', udf_hey_name('name').alias('Hey name'), 'salary', udf_square_val('salary').alias('salary Squared  !'))
#json, string double quote
df_ss_json_new_dq_4 = df_ss_json.select("name", udf_hey_name("name").alias("Hey name"), "salary", udf_square_val("salary").alias("salary Squared  !"))
print("df_ss_csv_new_4 \n", df_ss_csv_new_4.show() ,"\n")
print("df_ss_json_new_sq_4 \n", df_ss_json_new_sq_4.show() ,"\n")
print("df_ss_json_new_dq_4 \n", df_ss_json_new_dq_4.show() ,"\n")


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







