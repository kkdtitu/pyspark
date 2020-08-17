import pyspark
import random
import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark import SparkContext
from pyspark import SparkConf

def avg_list (rdd_list):
    return (rdd_list.sum()/rdd_list.count())

#spark = SparkSession \
#    .builder \
#    .appName("Python Spark SQL basic example") \
#    .config("spark.some.config.option", "some-value") \
#    .getOrCreate()

ss = pyspark.sql.SparkSession.builder.getOrCreate()
print("Config 1:", ss.sparkContext.getConf().getAll(), "\n")
print("Config 2:", ss, "\n")

#sc = ss.sparkContext()

df_direct = ss.createDataFrame([["KKD", "M", 5.6, ["USA", "India", "Europe"]], \
        ["Neepa", "F", 5.0, ["USA", "India", "Europe"]], \
        ["Ronak", "M", 5.1, ["USA", "India"]], \
        ["Ronik", "M", 4.4, ["USA", "India"]]], \
        ["Name", "Sex", "Height", "Countries visited"])

df_json = ss.read.json("people-sql-filter-select-groupby.json", multiLine=True)
#df_json = ss.read.json("people.json", multiLine=True)
df_csv = ss.read.csv('biostats-sql-filter-select-groupby.csv', header=True)
# Drop rows if they have null values
df_json_valid = df_json.dropna()

if os.path.exists('csv_sql_dir'):
        shutil.rmtree('csv_sql_dir')
if os.path.exists('json_sql_dir'):
        shutil.rmtree('json_sql_dir')

df_csv.write.csv('csv_sql_dir', header=True)
df_json.write.json('json_sql_dir')

#printSchema
print("df_direct printSchema :", df_direct.printSchema(), "\n")
print("df_csv printSchema :", df_csv.printSchema(), "\n")
print("df_json printSchema :", df_json.printSchema(), "\n")

#show'
print("df_direct show :", df_direct.show(), "\n")
print("df_csv.show :", df_csv.show(), "\n")
print("df_json.show :", df_json.show(), "\n")
print("df_json_valid.show :", df_json_valid.show(), "\n")  #rows with any null value dropped from df_json_valid 

#count
print("df_direct count :", df_direct.count(), "\n")
print("df_csv.count :", df_csv.count(), "\n")
print("df_json.count :", df_json.count(), "\n")
print("df_json_valid.count :", df_json_valid.count(), "\n")  #rows with any null value dropped from df_json_valid 

#describe
print("df_direct describe :", df_direct.describe(), "\n")
print("df_csv describe :", df_csv.describe(), "\n")
print("df_json describe :", df_json.describe(), "\n")

#describe.show
print("df_direct describe show :", df_direct.describe().show(), "\n")
print("df_csv describe show :", df_csv.describe().show(), "\n")
print("df_json describe show :", df_json.describe().show(), "\n")

#describe.show for a particular column
print("df_direct describe show particular col :", df_direct.describe('Name').show(), "\n")
print("df_csv describe show particular col :", df_csv.describe('Name').show(), "\n")
print("df_json describe show particular col :", df_json.describe('name').show(), "\n")

#show n=3
print("df_direct row 3", df_direct.show(n=3), "\n")
print("df_csv row 3", df_csv.show(n=3), "\n")
print("df_json row 3", df_json.show(n=3), "\n")

#take first 5 rows
print("df_direct first 3 rows", df_direct.take(3), "\n")
print("df_csv first 5 rows", df_csv.take(5), "\n")
print("df_json first 5 rows", df_json.take(5), "\n")

#collect. List of row objects / tuples. Each row object / tuple corresponds to a row
print("df_direct collect", df_direct.collect(), "\n")
print("df_csv collect", df_csv.collect(), "\n")
print("df_json collect", df_json.collect(), "\n")

#select 
print("df_csv select col :", df_csv.select(df_csv['Name']).show(), "\n")
print("df_json col select :", df_json.select(df_json['name'], df_json['age'], df_json['sex']).show(), "\n")

#select alt
print("df_csv select col alt 1 :", df_csv.select(df_csv.Name).show(), "\n")
print("df_json col select alt 1 :", df_json.select(df_json.name, df_json.age, df_json.sex).show(), "\n")

#select alt 2
print("df_csv select col alt 2 :", df_csv.select('Name').show(), "\n")
print("df_json col select alt 2 :", df_json.select('name', 'age', 'sex').show(), "\n")


#select alt, dropDuplicates and sort
print("df_csv select col alt, dropDuplicates and sort :", df_csv.select('Name').dropDuplicates().sort('Name').show(), "\n")
print("df_json col select alt, dropDuplicates and sort:", df_json.select('name', 'sex').dropDuplicates().sort('name').show(), "\n")

#select 
print("df_json filter by age\n", df_json.filter(df_json['age'] > 20).select(df_json['name'], df_json['age'], df_json['sex']).show(), "\n")
print("df_json filter by name\n", df_json.filter(df_json['name'].isin(["Andy", "Fabiana", "Justin"])) \
        .select(df_json['name'], df_json['age'], df_json['sex'])        \
        .show(), "\n")
print("df_json filter by name and age \n", df_json.filter(df_json['name'].isin(["Andy", "Fabiana", "Justin"])) \
        .filter(df_json['age'] > 20)    \
        .select(df_json['name'], df_json['age'], df_json['sex'])        \
        .show(), "\n")

#select alt 1  using 'filter'
print("df_json filter ALT 1 by name and age \n", df_json.filter(df_json.name.isin(["Andy", "Fabiana", "Justin"])) \
        .filter(df_json.age > 20)    \
        .select(df_json.name, df_json.age, df_json.sex)        \
        .show(), "\n")

#select alt 2 using 'where' instead of 'filter'
print("df_json where ALT 2 by name and age \n", df_json.filter(df_json.name.isin(["Andy", "Fabiana", "Justin"])) \
        .where(df_json.age > 20)    \
        .select('name', 'age', 'sex')        \
        .show(), "\n")

#select alt 2 with collect ()
print("df_json where collect ALT 2 by name and age \n", df_json.filter(df_json.name.isin(["Andy", "Fabiana", "Justin"])) \
        .where(df_json.age > 20)    \
        .select('name', 'age', 'sex')        \
        .collect(), "\n")

#groupBy, orderBy various ways
df_1 = df_json.groupBy('name') \
        .count()
print("df_json groupBy 1 show \n", df_1.orderBy('name').show(), "\n")

print("df_json groupBy orderBy 2 select show \n", df_json.groupBy(df_json.name) \
        .sum('salary').orderBy(df_json.name).show(), "\n")

print("df_json groupBy orderBy 3 select show \n", df_json.groupBy('name') \
        .max('salary').orderBy(df_json.name).show(), "\n")

print("df_json groupBy orderBy 4 select collect \n", df_json.groupBy('name') \
        .max('salary').orderBy('name').collect(), "\n")

print("df_json groupBy orderBy 5 show \n", df_json.groupBy().sum('salary').show(), "\n")

'''
print("df_json salary sum for Justin \n", df_json.filter(df_json['name'].isin(["Justin"])).select(max('salary')), "\n")
'''


'''
list_num = []
for i in range (25):
        list_num.append(random.randrange(100))
'''




