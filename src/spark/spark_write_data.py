from click import option
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

from config.database_config import get_database_config
from database.mysql_connect import MysqlConnect
from mysql.connector import Error
from typing import Dict

class SparkWriteDatabase:
    def __init__(self, spark: SparkSession, db_config):
        self.spark = spark
        self.db_config = db_config

    def spark_write_mysql(self,
                          df_write: DataFrame,
                          table_name: str,
                          jdbc_url: str,
                          config: Dict,
                          mode: str = "append"):
        try:
            mysql_config = get_database_config()["mysql"]
            mysql_client = MysqlConnect(mysql_config.mysql_user,
                                        mysql_config.mysql_password,
                                        mysql_config.mysql_host,
                                        mysql_config.mysql_db)
            with mysql_client:
                connection = mysql_client.connection
                cursor = mysql_client.cursor
                database = "github_data"
                connection.database = database
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN spark_temp VARCHAR(255)")
                connection.commit()
                connection.close()
        except Error as e:
            raise Exception(f"Error while connecting to MySQL: {e}")


        df_write.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", table_name) \
            .option("user", config["user"]) \
            .option("password", config["password"]) \
            .mode(mode) \
            .save()

        print(f"{'-' * 10} Spark write data to Mysql table {table_name} {'-' * 10} ")



    def spark_write_mongodb(self,
                            df: DataFrame,
                            mongo_uri: str,
                            database: str,
                            collection: str,
                            mode: str = "append",
                            ):
        try:
            df.write \
                .format("mongo") \
                .option("uri", mongo_uri) \
                .option("database", database) \
                .option("collection", collection) \
                .mode(mode) \
                .save()

            print(f"{'-' * 10} Spark write data to Mongodb database {database}.{collection} {'-' * 10} ")
        except Exception as e:
            print(f"{'-' * 10}  Failed to write to Mongodb database {database}.{collection}: {e} {'-' * 10} ")
            raise

    def validate_park_mongodb(self,
                                df_write: DataFrame,
                                mongo_uri: str,
                                database: str,
                                collection: str,
    ):
        query ={"spark_temp": "sparkwrite"}
        df_read = self.spark.read \
                    .format("mongo") \
                    .option("uri", mongo_uri) \
                    .option("database", database) \
                    .option("collection", collection) \
                    .option("pipeline", str([{"$match": query}])) \
                    .load()
        df_read = df_read.select(
                col("user_id"),
                col("login"),
                col("gravatar_id"),
                col("avatar_url"),
                col("url"),
                col("spark_temp"),
            )
        def subtract_dataframe(df_spark_write: DataFrame, df_spark_read: DataFrame, mode: str = "append"):
            result = df_spark_write.exceptAll(df_spark_read)
            if not result.isEmpty():
                result.write \
                    .format("mongo") \
                    .option("uri", mongo_uri) \
                    .option("database", database) \
                    .option("collection", collection) \
                    .mode(mode) \
                    .save()

        if df_write.count() == df_read.count():
            print(f"{'-' * 10} Validation spark_mongodb successful {df_read.count()} records {'-' * 10} ")
            subtract_dataframe(df_write, df_read)
        else:
            subtract_dataframe(df_write, df_read)
            print(f"{'-' * 10} Insert missing records {'-' * 10}")


        from database.mongo_connect import MongoConnect
        from config.database_config import get_database_config

        mongo_client = MongoConnect(mongo_uri, database)
        with mongo_client:
            mongo_client.db.Users.update_many({}, {"$unset": {"spark_temp": ""}})
            print(f"{'-' * 10} Delete temp {'-' * 10}")

    def validate_spark_mysql(self,
                             df_write: DataFrame,
                             jdbc_url: str,
                             table_name: str,
                             config: Dict,):
        df_read = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", f"(SELECT * FROM {table_name} WHERE spark_temp='sparkwrite') AS subquery") \
            .option("user", config["user"]) \
            .option("password", config["password"]) \
            .load()
        # df_read.show(
        def subtract_dataframe(df_spark_write: DataFrame, df_spark_read: DataFrame, mode: str = "append"):
            result = df_spark_write.exceptAll(df_spark_read)
            if not result.isEmpty():
                result.write \
                    .format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("driver", "com.mysql.cj.jdbc.Driver") \
                    .option("dbtable", table_name) \
                    .option("user", config["user"]) \
                    .option("password", config["password"]) \
                    .mode(mode) \
                    .save()


        if df_write.count() == df_read.count():
            print(f"{'-' * 10} Validation spark_mysql successful {df_read.count()} records {'-' * 10} ")
            subtract_dataframe(df_write, df_read)
        else:
            subtract_dataframe(df_write, df_read)
            print(f"{'-' * 10} Insert missing records {'-' * 10}")

        try:
            mysql_config = get_database_config()["mysql"]
            mysql_client = MysqlConnect(mysql_config.mysql_user,
                                        mysql_config.mysql_password,
                                        mysql_config.mysql_host,
                                        mysql_config.mysql_db)
            with mysql_client:
                connection = mysql_client.connection
                cursor = mysql_client.cursor
                database = "github_data"
                connection.database = database
                cursor.execute(f"ALTER TABLE {table_name} DROP COLUMN spark_temp")
                connection.commit()
                print(f"{'-' * 10} Drop column spark_temp {'-' * 10}")
        except Error as e:
            raise Exception(f"{'-' * 10}Error while connecting to MySQL: {e}{'-' * 10}")


    def spark_write_all_database(self, df: DataFrame, mode: str = "append"):
        self.spark_write_mysql(
            df,
            self.db_config["mysql"]["table"],
            self.db_config["mysql"]["jdbc_url"],
            self.db_config["mysql"]["config"],
            mode
        )
        self.spark_write_mongodb(
            df,
            self.db_config["mongodb"]["uri"],
            self.db_config["mongodb"]["database"],
            self.db_config["mongodb"]["collection"],
            mode
        )

    def validate_spark(self, df: DataFrame):
        self.validate_spark_mysql(
            df,
            self.db_config["mysql"]["jdbc_url"],
            self.db_config["mysql"]["table"],
            self.db_config["mysql"]["config"],
        )
        self.validate_park_mongodb(
            df,
            self.db_config["mongodb"]["uri"],
            self.db_config["mongodb"]["database"],
            self.db_config["mongodb"]["collection"],
        )