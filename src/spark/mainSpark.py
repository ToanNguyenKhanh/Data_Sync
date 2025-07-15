from config.database_config import get_database_config
from config.spark_config import SparkConnect
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, lit
from src.spark.spark_write_data import SparkWriteDatabase
from config.spark_config import get_spark_config
def main():
    db_config = get_database_config()

    jars = [
        "mysql:mysql-connector-java:8.0.33",
        "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1",
    ]

    spark_connect = SparkConnect(
        app_name="ETL",
        master_url="local[*]",
        executor_memory='2g',
        executor_cores=1,
        driver_memory='2g',
        num_executors=2,
        jars_packages=jars,
        # spark_config=None,
        log_level="INFO",
    )

    schema = StructType([
        StructField("actor", StructType([
            StructField("id", IntegerType(), False),
            StructField("login", StringType(), True),
            StructField("gravatar_id", StringType(), True),
            StructField("avatar_url", StringType(), True),
            StructField("url", StringType(), True),
        ]), True),

        StructField("repo", StructType([
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), True),
            StructField("url", StringType(), True),
        ]), True),
    ])

    df = spark_connect.spark.read.schema(schema).json("/home/toan/PycharmProjects/GITHUB_ETL/Data/2015-03-01-17.json")

    df_write_table_Users = df.withColumn("spark_temp", lit("sparkwrite")).select(
        col("actor.id").alias("user_id"),
        col("actor.login").alias("login"),
        col("actor.gravatar_id").alias("gravatar_id"),
        col("actor.avatar_url").alias("avatar_url"),
        col("actor.url").alias("url"),
        col("spark_temp").alias("spark_temp"),
    )

    spark_configs = get_spark_config()
    df_write = SparkWriteDatabase(spark_connect.spark, spark_configs)
    df_write.spark_write_all_database(df_write_table_Users, mode="append")
    #
    df_validate = SparkWriteDatabase(spark_connect.spark, spark_configs)
    df_validate.validate_spark(df_write_table_Users)

    spark_connect.stop()





if __name__ == "__main__":
    main()