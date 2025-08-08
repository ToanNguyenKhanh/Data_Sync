from pyspark.sql.functions import *
from pyspark.sql.types import *

from pyspark.sql import SparkSession

def main():

    jars = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    ]

    spark = SparkSession.builder \
        .appName("ProductProcessing") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.3,org.mongodb.spark:mongo-spark-connector_2.12:10.5.0") \
        .getOrCreate()

    # spark.sparkContext.setLogLevel("INFO")

    schemaKafka = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("login", StringType(), True),
        StructField("gravatar_id", StringType(), True),
        StructField("avatar_url", StringType(), True),
        StructField("url", StringType(), True),
        StructField("state", StringType(), True),
        StructField("log_timestamp", StringType(), True)
    ])

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "datdepzai") \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse JSON
    df_property = kafka_df.select(col("value").cast("string"))
        # .select(from_json(col("value"), schema).alias("data")) \
        # .select("data.*")

    df_property = df_property.withColumn("value", regexp_replace(col("value"), "\\\\", "")) \
        .withColumn("value", regexp_replace(col("value"), "^\"|\"$", ""))

    df_property = df_property.select(from_json(col("value"), schemaKafka).alias("data")) \
        .select("data.*")
    #

    mongo_stream =  df_property.writeStream \
        .format("mongodb") \
        .option("checkpointLocation", "/home/prime/PycharmProjects/DE-ETL-102/checkpoint/pyspark") \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .option('spark.mongodb.connection.uri', 'mongodb://datdepzai:datdepzaivailon@localhost:27017') \
        .option('spark.mongodb.database', 'github_data') \
        .option('spark.mongodb.collection', 'users') \
        .trigger(continuous="0.1 seconds") \
        .outputMode("append") \
        .start()

    mongo_stream.awaitTermination()

    # df_property.writeStream \
    #    .format("console") \
    #    .queryName("ConsoleOutput") \
    #    .option("forceDeleteTempCheckpointLocation", "true") \
    #    .outputMode("append") \
    #    .start() \
    #    .awaitTermination()





if __name__ == '__main__':
    main()