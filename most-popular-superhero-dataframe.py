from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

#file is space delimited first column is id; second column is name
names = spark.read.schema(schema).option("sep", " ").csv("file:///Users/adiyen/poc/udemy-courses/sparkCourse/marvel-names.txt")

#since we dont give schema, each line will be a row with just one column called "value"
#A sample line from file
#5988 748 1722 3752 4655 5743 1872 3413 5527 6368 6085 4319 4728 1636 2397 3364 4001 1614 1819 1585 732 2660 3952 2507 3891 2070 2239 2602 612 1352 5447 4548 1596 5488 1605 5517 11 479 2554 2043 17 865 4292 6312 473 534 1479 6375 4456 
#First column is the hero id and subsequent columns are connection ids
#our task is to find the super hero who has maximum no of connections
lines = spark.read.text("file:///Users/adiyen/poc/udemy-courses/sparkCourse/marvel-graph.txt")

# we trim each line of whitespace since that could throw off the counts.
#split the value column and add columns id and connections to lines dataframe
#id column will be first element of split array
#connections - we are just interested in how many ids are there. so get size of array
#and subtract 1 (to exclude the id column) to get no. of connections
#same id could repeat in multiple lines in the text file. To consider that
#we need to groupBy id and sum the connections
#Note that dataframes are immutable. It only creates new dataFrame.So you need to
#assign it to another variable
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))
    
#order by connections desc and get the first record (pyspark.sql.types.Row)
mostPopular = connections.sort(func.col("connections").desc()).first()

#lookup name using id
mostPopularName = names.filter(func.col("id") == mostPopular[0]).select("name").first()

print(mostPopularName[0] + " is the most popular superhero with " + str(mostPopular[1]) + " co-appearances.")

