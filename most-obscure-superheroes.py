from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostObscureSuperheroes").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("file:///Users/adiyen/poc/udemy-courses/sparkCourse/marvel-names.txt")
lines = spark.read.text("file:///Users/adiyen/poc/udemy-courses/sparkCourse/marvel-graph.txt")

#we trim whitespace from each line as this could throw the counts off by one.
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))

minConnectionCount = connections.agg(func.min("connections")).first()[0]

#select only rows whose connections equals minconnection count
minConnections = connections.filter(func.col("connections") == minConnectionCount)

#Join minConnections dataframe and names dataframe on id column
minConnectionsWithNames = minConnections.join(names, "id")

print("The following characters have only " + str(minConnectionCount) + " connection(s):")

minConnectionsWithNames.select("name").show()