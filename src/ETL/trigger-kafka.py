import configparser
import database
from database.mysql_connect import MysqlConnect
from config.database_config import get_database_config
from kafka import KafkaProducer
import json
import time

import sys
import os

# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
import sys
print(sys.path)

def get_data_trigger(mysql_client, last_timestamp):
    try:

        connection = mysql_client.connection
        connection.database = mysql_client.config['db']
        cursor = mysql_client.cursor
        query = "SELECT user_id, login, gravatar_id, avatar_url, url, state, DATE_FORMAT (log_timestamp, '%Y-%m-%d %H:%i:%s') AS log_timestamp FROM user_log_after"

        if last_timestamp:
            query += " WHERE DATE_FORMAT(log_timestamp, '%Y-%m-%d %H:%i:%s') = DATE_FORMAT(NOW(), '%Y-%m-%d %H:%i:%s')"
            cursor.execute(query)
        else:
            cursor.execute(query)

        rows = cursor.fetchall()
        connection.commit()

        schema = ["user_id", "login", "gravatar_id", "avatar_url", "url", "state", "log_timestamp"]
        data = [dict(zip(schema, row)) for row in rows]

        if data:
            new_timestamp = max((row["log_timestamp"] for row in data), default=last_timestamp)
        else:
            new_timestamp = last_timestamp
        return data, new_timestamp
    except Exception as e:
        print(e)
        return [], last_timestamp

def main():
    config = get_database_config()
    last_timestamp = None
    while True:
        mysql_config = get_database_config()["mysql"]
        mysql_client = MysqlConnect(mysql_config.mysql_user,
                                    mysql_config.mysql_password,
                                    mysql_config.mysql_host,
                                    mysql_config.mysql_db)
        with mysql_client:
            producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
            while True:
                data, new_timestamp = get_data_trigger(mysql_client, last_timestamp)
                last_timestamp = new_timestamp

                for record in data:
                    time.sleep(1)
                    producer.send("user_log_after", record)
                    producer.flush()
                    print(record)

if __name__ == '__main__':
    main()