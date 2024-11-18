import pymongo

class MongoDBSingleton:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MongoDBSingleton, cls).__new__(cls)
            cls._instance.client = pymongo.MongoClient("mongodb://localhost:27017/")
            cls._instance.db = cls._instance.client["Contacts_DB"]

            cls.clear_database(self= cls._instance)

            print("Connected to the database")
        return cls._instance

    def get_db(self):
        return self.db

    def clear_database(self):
        if self._instance.db.contacts.find_one() is not None:
            print("Clearing existing contacts...")
            self._instance.db.contacts.delete_many({})
