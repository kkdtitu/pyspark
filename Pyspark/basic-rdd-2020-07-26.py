import pyspark
import random
from pyspark.sql import SparkSession
#from pyspark import SparkContext

def avg_list (rdd_list):
    return (rdd_list.sum()/rdd_list.count())


logFile = "/Users/ronakronik/server/spark-2.4.3-bin-hadoop2.7/README.md"  
sc = pyspark.SparkContext()
#logData = sc.textFile(logFile).cache()
logData = sc.textFile(logFile)
numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()

list_char = ['a', 'b', 'c', 'd', 'e']
list_num = []
for i in range (25):
        list_num.append(random.randrange(100))

list_list1 = [('a',1),('a',5),('b',10),('b',150),('c',100),('c',-5),('c',-12)]
list_list2 = [('a',1),('b',1),('c',10),('d',10),('e',10)]
list_list3 = [("a",[3,7,8]),("b",[78,5,6]),("c",[-10,4,5])]

rdd_list_char = sc.parallelize(list_char)
count_list_char = rdd_list_char.count()

rdd_list_num = sc.parallelize(list_num)
even_list_num = rdd_list_num.filter(lambda n: n%2 == 0).collect()
odd_list_num = rdd_list_num.filter(lambda n: n%2 !=0).collect()
count_list_num = rdd_list_num.count()
sum_list_num = rdd_list_num.sum()
avg_list_num = avg_list (rdd_list_num) 
mean_list_num = rdd_list_num.mean()
min_list_num = rdd_list_num.min()
max_list_num = rdd_list_num.max()
stats_list_num = rdd_list_num.stats()

rdd_list_list1 = sc.parallelize(list_list1)
rdd_list_list2 = sc.parallelize(list_list2)
rdd_list_list3 = sc.parallelize(list_list3)
count_list_list1 = rdd_list_list1.count()
count_list_list2 = rdd_list_list2.count()
count_list_list3 = rdd_list_list3.count()
countByKey_list_list1 = rdd_list_list1.countByKey()
countByKey_list_list2 = rdd_list_list2.countByKey()
countByKey_list_list3 = rdd_list_list3.countByKey()
countByValue_list_list1 = rdd_list_list1.countByValue()
countByValue_list_list2 = rdd_list_list2.countByValue()
#countByValue_list_list3 = rdd_list_list3.countByValue()
collectAsMap_list_list1 = rdd_list_list1.collectAsMap()
collectAsMap_list_list2 = rdd_list_list2.collectAsMap()
collectAsMap_list_list3 = rdd_list_list3.collectAsMap()


print ("Lines with a", numAs, "\n")
print ("Lines with b", numBs, "\n")
print ("count_list_char", count_list_char, "\n")
print ("list_num", list_num,"\n")
print ("even_list_num", even_list_num,"\n")
print ("odd_list_num", odd_list_num,"\n")
print ("list_num count, sum, average ", count_list_num, sum_list_num, avg_list_num, "\n")
print ("list_num mean, min, max ", mean_list_num, min_list_num, max_list_num, "\n")
print ("stats list num", stats_list_num, "\n")

print ("count lists ", count_list_list1, count_list_list2, count_list_list3, "\n")
print ("count by key lists", countByKey_list_list1, countByKey_list_list2, countByKey_list_list3,"\n")
print ("count by value lists", countByValue_list_list1, countByValue_list_list2, "\n")
print ("collet as Map lists",collectAsMap_list_list1, collectAsMap_list_list2, collectAsMap_list_list3, "\n")