import pyspark
import random
import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark import SparkContext

def avg_list (rdd_list):
    return (rdd_list.sum()/rdd_list.count())

#spark = SparkSession \
#    .builder \
#    .appName("Python Spark SQL basic example") \
#    .config("spark.some.config.option", "some-value") \
#    .getOrCreate()

ss = pyspark.sql.SparkSession.builder.getOrCreate()
#sc = ss.sparkContext()

df_json = ss.read.json("/home/ubuntu/Downloads/spark-2.4.3-bin-hadoop2.7/examples/src/main/resources/people.json")
df_json = ss.read.json("people.json")
#df_csv = ss.read.csv('/home/ubuntu/Python3/iqos-batch-feed-analyze-results/policy.csv', header=True)
df_csv = ss.read.csv('biostats.csv', header=True)

if os.path.exists('csv_dir'):
        shutil.rmtree('csv_dir')
if os.path.exists('json_dir'):
        shutil.rmtree('json_dir')

df_csv.write.csv('csv_dir', header=True)
df_json.write.json('json_dir')


print("df_csv\n", df_csv.show(), "\n")
print("df_json\n", df_json.show(), "\n")
print("df_csv schema\n", df_csv.printSchema(), "\n")
print("df_json schema\n", df_json.printSchema(), "\n")
print("df_csv select\n", df_csv.select(df_csv['Name']).show(), "\n")
print("df_json select\n", df_json.select(df_json['name'], df_json['age'], df_json['sex']).show(), "\n")
print("df_json filter 1 select age\n", df_json.filter(df_json['age'] > 20).select(df_json['name'], df_json['age'], df_json['sex']).show(), "\n")
print("df_json filter 2 select name\n", df_json.filter(df_json['name'].isin(["Andy", "Fabiana", "Justin"])) \
        .select(df_json['name'], df_json['age'], df_json['sex'])        \
        .show(), "\n")

print("df_json filter 3 select name\n", df_json.filter(df_json['name'].isin(["Andy", "Fabiana", "Justin"])) \
        .filter(df_json['age'] > 20)    \
        .select(df_json['name'], df_json['age'], df_json['sex'])        \
        .show(), "\n")


print("df_json groupBy 1 select name\n", df_json.groupBy('name') \
        .count().show(), "\n")

print("df_json groupBy 2 select name\n", df_json.groupBy(df_json['name']) \
        .sum('salary').show(), "\n")

print("df_json groupBy 3 select name\n", df_json.groupBy('name') \
        .max('salary').show(), "\n")

print("df_json  \n", df_json.groupBy().sum('salary').show(), "\n")

'''
print("df_json salary sum for Justin \n", df_json.filter(df_json['name'].isin(["Justin"])).select(max('salary')), "\n")
'''


'''
list_num = []
for i in range (25):
        list_num.append(random.randrange(100))
'''





