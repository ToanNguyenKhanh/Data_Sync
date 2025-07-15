from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

class MongoConnect:
    def __init__(self, uri: str, db_name: str):
        self.uri = uri
        self.db_name = db_name
        self.client = None
        self.db = None

    def connect(self):
        print(f"{'-'*10} Connecting MongoDB {'-'*10}")
        try:
            self.client = MongoClient(self.uri)
            self.client.server_info()
            self.db = self.client[self.db_name]
            return self.db

        except ConnectionFailure as e:
            raise Exception(f"{'-'*10} Connection Failure: {e} {'-'*10}")

    def close(self):
        if self.client:
            self.client.close()
        print(f"{'-' * 10} Close Mongodb {'-' * 10}")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

