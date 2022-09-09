from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

lines = sc.textFile("/Users/elisazhang/Desktop/sparkcourse/ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2]) #get ratings and put it into a new rdd called ratings
result = ratings.countByValue()#countbyvalue: create a histogram

sortedResults = collections.OrderedDict(sorted(result.items())) #create a order dictionary
for key, value in sortedResults.items(): #iterate the whole result
    print("%s %i" % (key, value))
