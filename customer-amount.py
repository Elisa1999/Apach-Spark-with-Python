from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("CustomerAmount")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(",")
    customerID = int(fields[0])
    amount = float(fields[2])
    return (customerID, amount)

lines = sc.textFile('/Users/elisazhang/Desktop/sparkcourse/customer-orders.csv')
rdd = lines.map(parseLine)
totalAmount = rdd.reduceByKey(lambda x, y: x+y)
sortedTotalAmount = totalAmount.map(lambda x: (x[1],x[0])).sortByKey()

results = sortedTotalAmount.collect()

for result in results:
    print(result[1],"{:.2f}".format(result[0]))
