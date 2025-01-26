from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    customerId = int(fields[0])
    amount = float(fields[2])
    return (customerId, amount)  #return a tuple which will make a key-value RDD

lines = sc.textFile("file:///Users/adiyen/poc/udemy-courses/sparkCourse/customer-orders.csv")
results = lines.map(parseLine) \
          .reduceByKey(lambda x,y : x + y) \
          .map(lambda x: (x[1],x[0])) \
          .sortByKey() \
          .collect()

for result in results:
    print(str(result[1]) + "\t{:.2f}".format(result[0]))




