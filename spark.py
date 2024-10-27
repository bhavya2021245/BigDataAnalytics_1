from pyspark.sql import SparkSession
from pyspark.sql.functions import col, size, avg, countDistinct, array_contains

# Step 1: Create Spark Session with MongoDB Connector
spark = SparkSession.builder \
    .appName("MongoDBSparkQueries") \
    .config("spark.mongodb.input.uri", "mongodb://localhost:27017/university_mongo") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/university_mongo") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0") \
    .getOrCreate()
