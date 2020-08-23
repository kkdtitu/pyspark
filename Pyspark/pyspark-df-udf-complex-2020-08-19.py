import pyspark
import random
import os
import shutil
import pandas as pd 
import time
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from pyspark.sql.types import FloatType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.types import ArrayType
from pyspark.sql.types import MapType
from pyspark import SparkContext
from pyspark import SparkConf

import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

#Python functions
def twice_val (x):
    return 2*(int(x))

def hello_name (abc):
    return "Hello " + abc

#Spark UDFs creation from Python functions
udf_twice_val = udf(lambda z: twice_val(z), IntegerType())
udf_hello_name = udf(lambda xyz: hello_name(xyz), StringType())

udf_twice_val_1 = udf(twice_val, IntegerType())
udf_hello_name_1 = udf(hello_name, StringType())

#Alternae way of creatimng UDFs
@udf(returnType=IntegerType())
def udf_square_val (x):
        return (int(x))*(int(x))

@udf(returnType=StringType())
def udf_hey_name (abc):
        return "Hey " + abc

@udf(returnType=FloatType())
def udf_ratio_cols (a,b):
        if a and b:
                return float(a)/float(b)
        else:
                return 0

#Complex UDFs that returns ArrayType i.e. returns a list 
@udf(returnType=ArrayType(StringType()))  #ArrayType(StringType) => returns a list of strings
def udf_split_string_comma_to_list(string1):
        pattern = "([A-Za-z0-9]+),([A-Za-z0-9]+),([A-Za-z0-9]+)"
        result = re.search(pattern, string1)
        if result is None:
                return(string1, 0)
        list1 = list()
        for i in [1,2,3]:
                if not result.group(i) is None:
                        list1.append(result.group(i))   #i-th group from pattern
                else:
                        list1.append(0)

        return list1

@udf(returnType=ArrayType(StringType())) #ArrayType(StringType) => returns a list of strings
def udf_split_string_space_to_list(string1):
        pattern = "([A-Za-z0-9]+)(\s+)([A-Za-z0-9]+)(\s+)([A-Za-z0-9]+)"
        result = re.search(pattern,string1)
        if result is None:
                return(string1, 0)
        list1 = []
        for i in [1, 3, 5]:
                if not result.group(i) is None:
                        list1.append(result.group(i))   #i-th group from pattern
                else:
                        list1.append(0)

        return list1

#complex UDF with returnType = MapType, returns a dict/json of key/value strings
@udf(returnType=MapType(StringType(),StringType())) #MapType(StringType, StringType) => returns a dict/json of K/V strings
def udf_split_string_comma_to_dict(string1):
        pattern="([A-Za-z0-9]+),([A-Za-z0-9]+),([A-Za-z0-9]+)"
        result = re.search(pattern, string1)
        if result is None:
                return(string1, 0)
        dict1 = dict()
        for (x,y) in [("c1",1), ("c2",2), ("c3",3)]:   #iterating through a list of tuples
                if not result.group(y) is None:
                        dict1[x]=result.group(y)     #i-th group from pattern
                else:
                        dict1[x]=0
        
        return dict1

@udf(returnType=MapType(StringType(), StringType()))    #MapType(StringType, StringType) => returns a dict/json of K/V strings
def udf_split_string_space_to_dict(string1):
        pattern="([a-zA-Z0-9]+)(\s+)([0-9a-zA-Z]+)(\s+)([A-Za-z0-9]+)"
        result = re.search(pattern, string1)
        if result is None:
                return(string1, 0)
        dict1 = {}
        for (x,y) in [("fn",1), ("mn",3), ("ln",5)]:    #iterating through a list of tuples
                if not result.group(y) is None:
                        dict1[x] = result.group(y)     #i-th group from pattern
                else:
                        dict1[x]=0

        return dict1

#spark = SparkSession \
#    .builder \
#    .appName("Python Spark SQL basic example") \
#    .config("spark.some.config.option", "some-value") \
#    .getOrCreate()

ss = pyspark.sql.SparkSession.builder.getOrCreate()
print("Config 1:", ss.sparkContext.getConf().getAll(), "\n")
print("Config 2:", ss, "\n")

df_ss_1 = ss.createDataFrame([["KKD", "M", 5.6, "USA,India,India"], \
        ["Neepa", "F", 5.0, "USA,India,India"], \
        ["Ronak", "M", 5.1, "USA,India,USA"], \
        ["Ronik", "M", 4.4, "USA,India,USA"]], \
        ["Name", "Sex", "Height", "Countries"])

df_ss_json = ss.read.json("people-udf-complex.json", multiLine=True)
#df_json = ss.read.json("people.json", multiLine=True)
df_ss_csv = ss.read.csv('biostats-udf-complex.csv', header=True)
# Drop rows if they have null values
df_ss_json_valid = df_ss_json.dropna()


if os.path.exists('csv_udf_complex_dir'):
        shutil.rmtree('csv_udf_complex_dir')
if os.path.exists('json_udf_complex_dir'):
        shutil.rmtree('json_udf_complex_dir')

df_ss_csv.write.csv('csv_udf_complex_dir', header=True)
df_ss_json.write.json('json_udf_complex_dir')

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
print("df_ss_1 show \n", df_ss_1.show(29), "\n")
print("df_ss_csv show \n", df_ss_csv.show(), "\n")
print("df_ss_json show", df_ss_json.show(29), "\n")

#count
print("df_ss_1 count \n", df_ss_1.count(), "\n")
print("df_ss_csv count \n", df_ss_csv.count(), "\n")
print("df_ss_json count", df_ss_json.count(), "\n")

#take the first N elements
print("df_ss_1 take 3\n", df_ss_1.take(3), "\n")
print("df_ss_csv take 5", df_ss_csv.take(5), "\n")
print("df_ss_json take 5", df_ss_json.take(5), "\n")

#take the first N elements
print("df_ss_1 head 3\n", df_ss_1.head(3), "\n")
print("df_ss_csv head 5", df_ss_csv.head(5), "\n")
print("df_ss_json head 5", df_ss_json.take(5), "\n")

#collect -- returns a list of row object / tuples. Each row is a row object / tuple
print("df_ss_1 collect\n", df_ss_1.collect(), "\n")
print("df_ss_csv collect\n", df_ss_csv.collect(), "\n")
print("df_ss_json collect\n", df_ss_json.collect(), "\n")





#Calling Spark UDFs
#csv
df_ss_csv_new_1 = df_ss_csv.select("Name", udf_hello_name_1("Name").alias("Hello Name"), \
        "Age (yrs)", udf_twice_val_1("Age (yrs)").alias("Age doubled !"))
#json, string single quote
df_ss_json_new_sq_1 = df_ss_json.select('name', udf_hello_name_1('name').alias('Hello name'), \
        'salary', udf_twice_val_1('salary').alias('salary doubled !'))
#json, string double quote
df_ss_json_new_dq_1 = df_ss_json.select("name", udf_hello_name_1("name").alias("Hello name"), \
        "salary", udf_twice_val_1("salary").alias("salary doubled !"))
print("df_ss_csv_new_1 \n", df_ss_csv_new_1.show() ,"\n")
print("df_ss_json_new_sq_1 \n", df_ss_json_new_sq_1.show(29) ,"\n")
print("df_ss_json_new_dq_1 \n", df_ss_json_new_dq_1.show(29) ,"\n")

#Using Spark UDFs to add columns to a Spark DF
#csv
df_ss_csv_new_2 = df_ss_csv.withColumn("Hello Name", udf_hello_name("Name"))
#json, string single quote
df_ss_json_new_2 = df_ss_json.withColumn('Salary Doubled !', udf_twice_val('salary'))
print("df_ss_csv_new_2 \n", df_ss_csv_new_2.show(29) ,"\n")
print("df_ss_json_new_2 \n", df_ss_json_new_2.show(29) ,"\n")


#Using ALT Spark UDFs to add columns to a Spark DF
#csv
df_ss_csv_new_3 = df_ss_csv.withColumn("Age Squared", udf_square_val("Age (yrs)"))
print("df_ss_csv_new_3 \n", df_ss_csv_new_3.show(29, truncate=False) ,"\n")
#json, string single quote
df_ss_json_new_3 = df_ss_json.withColumn('Salary Squared !', udf_square_val('salary'))
print("df_ss_json_new_3 \n", df_ss_json_new_3.show(29, truncate=False) ,"\n")

#Using ALT Spark UDFs 
#csv
df_ss_csv_new_sq_4 = df_ss_csv.select("Name", udf_hey_name("Name").alias("Hey Name"), "Age (yrs)", \
        udf_square_val("Age (yrs)").alias("Age Squared !"))
df_ss_csv_new_ratio_4 = df_ss_csv.select("Name", "Height (in)", "Weight (lb)", \
        udf_ratio_cols("Weight (lb)", "Height (in)").alias("Ration Wt/Ht"))
#json, string single quote
df_ss_json_new_sq_4 = df_ss_json.select('name', udf_hey_name('name').alias('Hey name'), 'salary', \
        udf_square_val('salary').alias('salary Squared  !'))
#json, string double quote
df_ss_json_new_dq_4 = df_ss_json.select("name", udf_hey_name("name").alias("Hey name"), "salary", \
        udf_square_val("salary").alias("salary Squared  !"))
print("df_ss_csv_new_sq_4 \n", df_ss_csv_new_sq_4.show(28, truncate=False) ,"\n")
print("df_ss_csv_new_ratio_4 \n", df_ss_csv_new_ratio_4.show(29, truncate=False) ,"\n")
print("df_ss_json_new_sq_4 \n", df_ss_json_new_sq_4.show(29, truncate=False) ,"\n")
print("df_ss_json_new_dq_4 \n", df_ss_json_new_dq_4.show(27, truncate=False) ,"\n")


#using complex UDFs
# converting string column to a list column 
df_ss_1_udf_split_string_comma_to_list = df_ss_1.withColumn("countries-list", udf_split_string_comma_to_list("Countries"))
print("df_ss_1_udf_split_string_comma_to_list printSchema:", df_ss_1_udf_split_string_comma_to_list.printSchema() ,"\n")
print("df_ss_1_udf_split_string_comma_to_list :", df_ss_1_udf_split_string_comma_to_list.show(28, truncate=False) ,"\n")

df_ss_csv_split_string_space_to_list = df_ss_csv.withColumn("split-name-list", udf_split_string_space_to_list("Name"))
print("df_ss_csv_split_string_space_to_list printSchema:", df_ss_csv_split_string_space_to_list.printSchema(), "\n")
print("df_ss_csv_split_string_space_to_list :", df_ss_csv_split_string_space_to_list.show(28, truncate=False), "\n")


df_ss_json_split_string_space_to_list = df_ss_json.withColumn("split-name-list", udf_split_string_space_to_list("name"))
print("df_ss_json_split_string_space_to_list printSchema:", df_ss_json_split_string_space_to_list.printSchema())
print("df_ss_json_split_string_space_to_list toPandas:", \
        df_ss_json_split_string_space_to_list.toPandas()) #pandas DF print better !
print("df_ss_json_split_string_space_to_list :", \
        df_ss_json_split_string_space_to_list.show(28, truncate=False))

#converting a string column to a dict/json column
df_ss_1_udf_split_string_comma_to_dict = df_ss_1.withColumn("countriesdict", udf_split_string_comma_to_dict("Countries"))
print("df_ss_1_udf_split_string_comma_to_dict printSchema:", df_ss_1_udf_split_string_comma_to_dict.printSchema() ,"\n")
print("df_ss_1_udf_split_string_comma_to_dict toPandas:", \
        df_ss_1_udf_split_string_comma_to_dict.toPandas() ,"\n") #pandas DF print better !
print("df_ss_1_udf_split_string_comma_to_dict :", \
        df_ss_1_udf_split_string_comma_to_dict.show(28, truncate=False) ,"\n")

df_ss_csv_split_string_space_to_dict = df_ss_csv.withColumn("splitnamedict", \
        udf_split_string_space_to_dict("Name"))
print("df_ss_csv_split_string_space_to_dict printSchema:", df_ss_csv_split_string_space_to_dict.printSchema())
print("df_ss_csv_split_string_space_to_dict toPandas:", \
        df_ss_csv_split_string_space_to_dict.toPandas()) #pandas DF print better !
print("df_ss_csv_split_string_space_to_dict :", df_ss_csv_split_string_space_to_dict.show(28, truncate=False))

df_ss_json_split_string_space_to_dict = df_ss_json.withColumn("splitnamedict", \
        udf_split_string_space_to_dict("name"))
print("df_ss_json_split_string_space_to_dict printSchema: ", df_ss_json_split_string_space_to_dict.printSchema())
print("df_ss_json_split_string_space_to_dict toPandas: ", \
        df_ss_json_split_string_space_to_dict.toPandas()) #pandas DF print better !
print("df_ss_json_split_string_space_to_dict: ", df_ss_json_split_string_space_to_dict.show(28, truncate=False))

# Now we are going to split the dict column into multiple columns
list_c = ["c1", "c2", "c3"]
list_n = ["fn", "mn", "ln"]

list_c_for_select_expr = list()
list_n_for_select_expr = []
for item in list_c:
        new_item = "countriesdict['"+item+"']"
        list_c_for_select_expr.append(new_item)
print(list_c_for_select_expr)
for item in list_n:
        new_item = "splitnamedict['" + item + "']"
        list_n_for_select_expr.append(new_item)
print(list_n_for_select_expr)

df_ss_1_udf_split_string_comma_to_dict_mult_cols = df_ss_1_udf_split_string_comma_to_dict.selectExpr("*", *list_c_for_select_expr)
print("df_ss_1_udf_split_string_comma_to_dict_mult_cols toPandas:", df_ss_1_udf_split_string_comma_to_dict_mult_cols.toPandas())
print("df_ss_1_udf_split_string_comma_to_dict_mult_cols :", df_ss_1_udf_split_string_comma_to_dict_mult_cols.show(45, truncate=False))

df_ss_csv_split_string_space_to_dict_mult_cols = df_ss_csv_split_string_space_to_dict.selectExpr("*", *list_n_for_select_expr)
print("df_ss_csv_split_string_space_to_dict_mult_cols toPandas", df_ss_csv_split_string_space_to_dict_mult_cols.toPandas())
print("df_ss_csv_split_string_space_to_dict_mult_cols : ", df_ss_csv_split_string_space_to_dict_mult_cols.show(45, truncate=False))

df_ss_json_split_string_space_to_dict_mult_cols = df_ss_json_split_string_space_to_dict.selectExpr("*", *list_n_for_select_expr)
print("df_ss_json_split_string_space_to_dict_mult_cols toPandas ", df_ss_json_split_string_space_to_dict_mult_cols.toPandas())
print("df_ss_json_split_string_space_to_dict_mult_cols :", df_ss_json_split_string_space_to_dict_mult_cols.show(46, truncate=False))