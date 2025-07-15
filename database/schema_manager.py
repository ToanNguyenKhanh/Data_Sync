from dask.array import tril_indices
from jupyter_lsp.specs import sql
from mysql.connector import Error
from pymongo.errors import ConnectionFailure
import os

from database.mysql_connect import MysqlConnect

SQL_FILE_PATH = "../sql/schema.sql"

def create_mysql_schema(connection, cursor, db):
    db_name = db
    print(f"{'-' * 10} Creating MySQL database schema {'-' * 10} ")
    try:
        cursor.execute(f"DROP DATABASE IF EXISTS {db_name};")
        connection.commit()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        connection.commit()
        connection.database = db_name

        sql_cmd = []
        with open(SQL_FILE_PATH, "r") as sql_file:
            sqls = sql_file.read()
            for sql in sqls.split(";"):
                cmd = sql.strip()
                if cmd:
                    sql_cmd.append(cmd)

        for cmd in sql_cmd:
            cursor.execute(cmd)

        print(f"{'-' * 10} Created schema {'-' * 10} ")
        connection.commit()

    except Error as e:
        connection.rollback()
        raise Exception(f"{'-' * 10} Error while creating MySQL schema: {e} {'-' * 10}") from e

def validate_mysql_schema(connection, cursor):
    cursor.execute("SHOW TABLES;")
    tables = cursor.fetchall()

    tables_list = []
    for row in tables:
        tables_list.append(row[0])

    if "Users" not in tables_list or "Repositories" not in tables_list:
        raise Exception(f"{'-' * 10} Tables not found for MySQL schema: {tables_list}")

    cursor.execute("SELECT * FROM Users WHERE user_id = 1;")
    user = cursor.fetchone()
    if user is None:
        raise Exception(f"{'-' * 10} user not found for MySQL schema: {tables_list}")
    print(f"{'-' * 10} Validated MySQL schema {'-' * 10}")

def create_mysql_trigger(connection, cursor, db):
        trigger_file_path = os.path.abspath("../sql/trigger/mysql.trigger")
        db_name = db
        print(f"{'-' * 10} Creating MySQL trigger {'-' * 10} ")
        try:
            sqls = []
            with open(trigger_file_path, "r") as sql_file:
                trigger_sqls= sql_file.read()
                delimiter = "DELIMITER //"

                for sql_cmd in trigger_sqls.split(";"):
                    cmd = sql.strip()
                    if cmd:
                        sqls.append(cmd)
            for cmd in sqls:
                cursor.execute(cmd)
            connection.commit()
        except Error as e:
            raise Exception("")


def create_mongo_schema(db):
    db.drop_collection("Users")
    user_schema = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["user_id", "login"],
            "properties": {
                "user_id": {
                    "bsonType": "int",
                },
                "login": {
                    "bsonType": ["string"]
                },
                "gravatar_id": {
                    "bsonType": ["string", "null"]
                },
                "avatar_url": {
                    "bsonType": ["string", "null"]
                },
                "url": {
                    "bsonType": ["string", "null"]
                }
            },
        }
    }
    try:
        db.create_collection("Users", validator=user_schema)
    except ConnectionFailure as e:
        raise Exception(f"{'-' * 10} Connection Failure: {e} {'-' * 10}")
    db.Users.create_index([("user_id", 1)], unique=False)

def validate_mongo_schema(db):
    collection_name = db.list_collection_names()
    if "Users" not in collection_name:
        raise Exception(f"{'-' * 10} Collection not found for MySQL schema: {collection_name}")

    user = db.Users.find_one({"user_id": 1})
    if user is None:
        raise Exception(f"{'-' * 10} User not found for MySQL schema: {collection_name}")
    print(f"{'-' * 10} Validated MongoDB schema {'-' * 10}")

def create_redis_schema(client):
    try:
        client.flushdb()
        client.set("user:1:login", "jsonmurphy")
        client.set("user:1:gravatar_id", "")
        client.set("user:1:avatar_url", "https://avatars.githubusercontent.com/u/1843574?")
        client.set("user:1:url", "https://api.github.com/users/jsonmurphy")
        client.sadd("user_id", "user:1")
        print(f"{'-'*10} Add data to Redis {'-'*10}")
    except Exception as e:
        raise Exception(f"{'-'*10} Failed to add data to Redis: {e} {'-'*10}") from e

def validate_redis_schema(client):
    if not client.get("user:1:login") == "jsonmurphy":
        raise Exception(f"{'-'*10} Missing data in Redis {'-'*10}")

    if not client.sismember("user_id", "user:1"):
        raise Exception(f"{'-' * 10} User not in Redis {'-'*10}")

    print(f"{'-' * 10} Redis schema validated {'-'*10}")






