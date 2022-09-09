from pyspark import SparkConf, SparkContext #import from sprak
import collections

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

lines = sc.textFile("/Users/elisazhang/Desktop/sparkcourse/fakefriends.csv")
rdd = lines.map(parseLine)
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
#results = averagesByAge.collect() #Return a list that contains all of the elements in this RDD.
results = averagesByAge.sortByKey().collect()
for result in results:
    print(result)
