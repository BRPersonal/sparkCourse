from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType

spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

# Create schema when reading u.data
schema = StructType([ \
                     StructField("userID", IntegerType(), True), \
                     StructField("movieID", IntegerType(), True), \
                     StructField("rating", IntegerType(), True), \
                     StructField("timestamp", LongType(), True)])

# Load up movie data as dataframe. It is tab delimited text file
moviesDF = spark.read.option("sep", "\t").schema(schema).csv("file:///Users/adiyen/poc/udemy-courses/sparkCourse/ml-100k/u.data")

# Some SQL-style magic to sort all movies by popularity in one line!
#We want to find movies that has appeared max times in rating. Not necessary it has received
#5 star rating. So all we need is just count the occurrence of each movie and order by count descending
#to show the top 10 popular movies
topMovieIDs = moviesDF.groupBy("movieID").count().orderBy(func.desc("count"))

# Grab the top 10
topMovieIDs.show(10)

# Stop the session
spark.stop()
