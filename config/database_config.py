from dataclasses import dataclass
import os
from dotenv import load_dotenv
from typing import Optional

class DatabaseConfig:
    def validate(self):
        for key, value in self.__dict__.items():
            if value is None:
                raise Exception(f"{'-'*10} Missing config for {key} {'-'*10}")


@dataclass
class MysqlConfig(DatabaseConfig):
    mysql_user: str
    mysql_password: str
    mysql_host: str
    mysql_port: int
    mysql_db: str
    jar_path: Optional[str] = None
    table: str = "Users"

@dataclass
class MongodbConfig(DatabaseConfig):
    mongo_uri: str
    mongo_db: str
    jar_path: Optional[str] = None
    collection: str = "Users"

@dataclass
class RedisConfig(DatabaseConfig):
    redis_host: str
    redis_port: int
    redis_user: str
    redis_password: str
    redis_db: str
    jar_path: Optional[str] = None
    key_column: str = "id"

def get_database_config():
    load_dotenv()
    config = {
        "mysql": MysqlConfig(
            mysql_user= os.getenv("MYSQL_USER"),
            mysql_password= os.getenv("MYSQL_PASSWORD"),
            mysql_host= os.getenv("MYSQL_HOST"),
            mysql_port= int(os.getenv("MYSQL_PORT")),
            mysql_db= os.getenv("MYSQL_DB"),
            jar_path= os.getenv("MYSQL_JAR_PATH"),
        ),
        "mongo": MongodbConfig(
            mongo_uri= os.getenv("MONGO_URI"),
            mongo_db= os.getenv("MONGO_DB"),
            jar_path= os.getenv("MONGO_JAR_PATH"),
        ),
        "redis": RedisConfig(
            redis_host=os.getenv("REDIS_HOST"),
            redis_port=int(os.getenv("REDIS_PORT")),
            redis_user=os.getenv("REDIS_USER"),
            redis_password=os.getenv("REDIS_PASSWORD"),
            redis_db=os.getenv("REDIS_DB"),
            jar_path= os.getenv("REDIS_JAR_PATH"),
        )

    }


    for db, settings in config.items():
        settings.validate()

    return config

if __name__ == '__main__':
    config = get_database_config()
    print(config)