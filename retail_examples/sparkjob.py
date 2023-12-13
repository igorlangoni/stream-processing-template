
import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


if __name__ == "__main__":

    # Checking validity of Spark submission command
    if len(sys.argv) != 4:
        print("Wrong number of args.", file=sys.stderr)
        sys.exit(-1)

    # Initializing Spark session
    spark = SparkSession\
        .builder\
        .appName("MySparkSession")\
        .getOrCreate()

   # Setting parameters for the Spark session to read from Kafka
    bootstrapServers = sys.argv[1]
    subscribeType = sys.argv[2]
    topics = sys.argv[3]

    # Streaming data from Kafka topic as a dataframe
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "b-1.monstercluster1.6xql65.c3.kafka.eu-west-2.amazonaws.com:9092") \
        .option("subscribe", "retail_transactions") \
        .load()

    # Expression that reads in raw data from dataframe as a string
    # and names the column "json"
    transactions = (df.selectExpr("CAST(value AS STRING)")
                    .withColumn("data", from_json(col("value"), schema))
                    .select("data.*"))

    # Writing dataframe to console in append mode
    query = transactions.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    # Terminates the stream on abort
    query.awaitTermination()