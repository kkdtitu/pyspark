import pyspark
import random
import shutil
import os
from pyspark.sql import SparkSession
#from pyspark import SparkContext

def calc_pi(n):
        x, y = random.random(), random.random()
        return x*x+y*y < 1

sc = pyspark.SparkContext()


srcFile = "/home/ubuntu/Downloads/spark-2.4.3-bin-hadoop2.7/README.md"  
rdd_file = sc.textFile(srcFile, 4)   #specify number of partitions in the RDD
lc_file = rdd_file.count()
rdd_file_words = rdd_file.flatMap(lambda line: line.split(" "))
wc_file = rdd_file_words.count()
rdd_wc_bykey_file = rdd_file_words.map(lambda word: (word, 1)).reduceByKey(lambda x,y: x+y).sortByKey()

if os.path.exists('wc-bykey'):
        shutil.rmtree('wc-bykey')
rdd_wc_bykey_file.saveAsTextFile("wc-bykey")


num_samples = 1000000
rdd_rand = sc.parallelize(range(num_samples),4)
rdd_circ = rdd_rand.filter(calc_pi)
#print ("\n rdd_rand", rdd_rand.collect(), "\n")
#print ("\n rdd_circ", rdd_circ.collect(), "\n")
rdd_rand_count = rdd_rand.count()
rdd_circ_count = rdd_circ.count()
print ("\n rdd_rand", rdd_rand.count(), "\n")
print ("\n rdd_circ", rdd_circ.count(), "\n")
print ("\n Pi =", 4*(rdd_circ_count/rdd_rand_count),"\n")




