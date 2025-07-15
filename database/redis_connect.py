import redis
from redis.exceptions import ConnectionError
from config.database_config import get_database_config

class RedisConnect:
    def __init__(self, user, password, host, port, db):
        self.config = {
            "host": host,
            "port": port,
            "password": password,
            "username": user,
            "db": db,
        }
        self.client = None

    def connect(self):
        try:
            print("🔌 Connecting Redis...")
            self.client = redis.Redis(
                **self.config,
                decode_responses=True
            )
            self.client.ping()
            print(f"{'-'*10} Connected to Redis {'-'*10}")

        except ConnectionError as e:
            raise Exception(f"{'-'*10} Failed to connect to Redis: {e} {'-'*10}") from e

    def close(self):
        if self.client:
            self.client.close()
        print(f"{'-'*10} Closed Redis {'-'*10}")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


