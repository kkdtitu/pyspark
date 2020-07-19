import pyspark
import random
#from pyspark import SparkContext

def avg_list (rdd_list):
    return (rdd_list.sum()/rdd_list.count())

logFile = "/home/ubuntu/Downloads/spark-2.4.3-bin-hadoop2.7/README.md"  
sc = pyspark.SparkContext()
#logData = sc.textFile(logFile).cache()
logData = sc.textFile(logFile)
numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()

list_char = ['a', 'b', 'c', 'd', 'e']
list_num = []
for i in range (25):
        list_num.append(random.randrange(100))

rdd_char = sc.parallelize(list_char)
data_char_count = rdd_char.count()

rdd_num = sc.parallelize(list_num)

rdd_num_even = rdd_num.filter(lambda n: n%2 == 0)
list_num_even = rdd_num_even.collect()

rdd_num_odd = rdd_num.filter(lambda n: n%2 !=0)
list_num_odd = rdd_num_odd.collect()

data_num_count = rdd_num.count()
data_num_sum = rdd_num.sum()
data_num_avg = avg_list (rdd_num) 

data_num_mean = rdd_num.mean()
data_num_min = rdd_num.min()
data_num_max = rdd_num.max()

print ("Lines with a", numAs, "\n")
print ("Lines with b", numBs, "\n")
print ("data_char count", data_char_count, "\n")
print ("list_num:", list_num, "\n")
print ("list_num_even", list_num_even,"\n")
print ("list_num_odd", list_num_odd,"\n")
print ("data_num count, sum, average ", data_num_count, data_num_sum, data_num_avg, "\n")
print ("data_num mean, min, max ", data_num_mean, data_num_min, data_num_max, "\n")
