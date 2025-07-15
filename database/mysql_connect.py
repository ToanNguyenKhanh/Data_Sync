from config.database_config import get_database_config
from mysql.connector import Error
import mysql

class MysqlConnect:
    def __init__(self, user, password, host, db):
        self.config = {'user': user, 'password': password, 'host': host, 'db': db}
        self.cursor = None
        self.connection = None

    def connect(self):
        try:
            print(f"{'-' * 10} Connecting MySql {'-' * 10} ")
            self.connection = mysql.connector.connect(**self.config)
            self.cursor = self.connection.cursor()
            print(f"{'-'*10} Connected to MySQL {'-'*10}")
            return self.cursor
        except Error as e:
            raise Exception(f"{'-'*10} Failed to connect to MySQL: {e} {'-'*10}") from e

    def close(self):
        if self.connection:
            self.connection.close()
        print(f"{'-' * 10} Mysql close success {'-' * 10} ")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


if __name__ == '__main__':
    mysql_config = get_database_config()['mysql']
    sql_client = MysqlConnect(mysql_config.mysql_user,
                              mysql_config.mysql_password,
                              mysql_config.mysql_host,
                              mysql_config.mysql_db)

    with sql_client:
        print(f"{'-' * 10} Connected to MySQL: {mysql_config.mysql_db} {'-' * 10}")
        connection = sql_client.connection
        cursor = sql_client.cursor

