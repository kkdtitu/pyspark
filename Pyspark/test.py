from pyspark import SparkContext
logFile = "/home/ubuntu/Downloads/spark-2.4.3-bin-hadoop2.7/README.md"  
sc = SparkContext("local", "anything random")
logData = sc.textFile(logFile).cache()
numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()
print ("Lines with a", numAs, "\n")
print ("Lines with b", numBs, "\n")
