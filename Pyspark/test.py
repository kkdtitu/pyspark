from pyspark import SparkContext
logFile = "/Users/ronakronik/server/spark-2.4.3-bin-hadoop2.7/README.md"  
sc = SparkContext("local", "anything random")
logData = sc.textFile(logFile).cache()
numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()
print ("Lines with a", numAs, "\n")
print ("Lines with b", numBs, "\n")
