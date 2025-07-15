import pyspark
from pyspark.sql import SparkSession
from typing import Optional, Dict, List
import os
from config.database_config import get_database_config

class SparkConnect:
    def __init__(self,
                 app_name: str,
                 master_url: str = "Local[*]",
                 executor_memory: Optional[str] = "4g",
                 executor_cores: Optional[int] = 2,
                 driver_memory: Optional[str] = "4g",
                 num_executors: Optional[int] = 3,
                 jars_packages: Optional[List[str]] = None,
                 spark_config: Optional[Dict[str, str]] = None,
                 log_level: str = "WARN", ):
        self.app_name = app_name
        self.master_url = master_url
        self.executor_memory = executor_memory
        self.executor_cores = executor_cores
        self.driver_memory = driver_memory
        self.num_executors = num_executors
        self.jars_packages = jars_packages
        self.spark_config = spark_config
        self.log_level = log_level

        self.spark = self.create_spark_session()

    def create_spark_session(self):
        builder = SparkSession.builder \
            .appName(self.app_name) \
            .master(self.master_url)

        if self.executor_memory:
            builder.config("spark.executor.memory", self.executor_memory)
        if self.executor_cores:
            builder.config("spark.executor.cores", str(self.executor_cores))
        if self.driver_memory:
            builder.config("spark.driver.memory", self.driver_memory)
        if self.num_executors:
            builder.config("spark.executor.instances", str(self.num_executors))

        # if self.jars:
        #     jars_path = ",".join([os.path.abspath(jar) for jar in self.jars])
        #     builder.config("spark.jars", jars_path)

        if self.jars_packages:
            jar_packages_url = ",".join([jar_package for jar_package in self.jars_packages])
            builder.config("spark.jars.packages", jar_packages_url)

        if self.spark_config:
            for key, value in self.spark_config.items():
                builder.config(key, value)

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel(self.log_level)
        return spark

    def stop(self):
        if self.spark:
            self.spark.stop()
        print(f"{'-' * 10} Stopping spark session with app name {self.app_name} {'-' * 10} ")

def get_spark_config() -> Dict:
    db_configs = get_database_config()

    return {
        "mysql": {
            "table": db_configs["mysql"].table,
            # "jdbc": db_configs["mysql"].jdbc,
            "jdbc_url": "jdbc:mysql://{}:{}/{}".format(db_configs["mysql"].mysql_host,db_configs["mysql"].mysql_port,db_configs["mysql"].mysql_db),
            # "jdbc_url" : "jdbc:mysql://172.17.0.2:3306/github_data",
            "config": {
                "host": db_configs["mysql"].mysql_host,
                "port": db_configs["mysql"].mysql_port,
                "user": db_configs["mysql"].mysql_user,
                "password": db_configs["mysql"].mysql_password,
                "database": db_configs["mysql"].mysql_db,
            }
        },
        "mongodb": {
            "uri": db_configs["mongo"].mongo_uri,
            "database": db_configs["mongo"].mongo_db,
            "collection": db_configs["mongo"].collection,
        },
        "redis" : {

        }
    }

if __name__ == '__main__':
    res = get_spark_config()
    print(res)