import csv
import time
from pymongo import MongoClient
from datetime import datetime


class MongoDBExecutor:
    def __init__(self, dbname, host, port):
        self.dbname = dbname
        self.host = host
        self.port = port
        self.client = None
        self.db = None

    def connect(self):
        """Establish connection to MongoDB"""
        connection_string = f"mongodb://{self.host}:{self.port}"
        self.client = MongoClient(connection_string)
        self.db = self.client[self.dbname]

    def close(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()

    def load_csv(self, row_num):
        """Load CSV files into MongoDB collections"""
        try:
            self.connect()

            def load_csv_to_collection(file_path, collection_name, transform_func=None):
                documents = []
                with open(file_path, 'r') as file:
                    reader = csv.DictReader(file)
                    for row in reader:
                        if transform_func:
                            doc = transform_func(row)
                        else:
                            doc = row
                        documents.append(doc)

                        # Batch insert every 500 documents
                        if len(documents) >= 500:
                            self.db[collection_name].insert_many(documents)
                            documents = []

                    # Insert remaining documents
                    if documents:
                        self.db[collection_name].insert_many(documents)

            # Transform functions for each collection
            def transform_contact(row):
                return {
                    "_id": row['id'],
                    "first_name": row['first_name'],
                    "last_name": row['last_name'],
                    "email": row['email'],
                    "phone_number": row['phone_number'],
                    "address": row['address'],
                    "created_at": datetime.fromisoformat(row['created_at']),
                    "groups": [],  # Will be populated when processing contact_groups
                    "calls": []  # Will be populated when processing calls
                }

            def transform_group(row):
                return {
                    "_id": row['id'],
                    "name": row['name'],
                    "created_at": datetime.fromisoformat(row['created_at']),
                    "members": []  # Will be populated when processing contact_groups
                }

            def transform_call(row):
                return {
                    "_id": row['id'],
                    "duration": int(row['duration']),
                    "call_date": datetime.fromisoformat(row['call_date'])
                }

            # Load main collections
            load_csv_to_collection(
                f'../../../common/data/contacts{row_num}.csv',
                'contacts',
                transform_contact
            )

            load_csv_to_collection(
                f'../../../common/data/groups{row_num}.csv',
                'groups',
                transform_group
            )

            load_csv_to_collection(
                f'../../../common/data/calls{row_num}.csv',
                'calls',
                transform_call
            )

            # Process relationships
            with open(f'../../../common/data/contact_groups{row_num}.csv', 'r') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    # Update contact's groups
                    self.db.contacts.update_one(
                        {"_id": row['contact_id']},
                        {"$push": {"groups": row['group_id']}}
                    )
                    # Update group's members
                    self.db.groups.update_one(
                        {"_id": row['group_id']},
                        {"$push": {"members": row['contact_id']}}
                    )

        except Exception as error:
            print(f"Error: {error}")
        finally:
            self.close()

    def create_indexes(self):
        """Create MongoDB indexes"""
        try:
            self.connect()

            # Contacts indexes
            self.db.contacts.create_index("email", unique=True)
            self.db.contacts.create_index("phone_number")
            self.db.contacts.create_index("groups")

            # Groups indexes
            self.db.groups.create_index("name")
            self.db.groups.create_index("members")

            # Calls indexes
            self.db.calls.create_index("call_date")

        except Exception as error:
            print(f"Error: {error}")
        finally:
            self.close()

    def drop_indexes(self):
        """Drop all indexes except _id"""
        try:
            self.connect()

            self.db.contacts.drop_indexes()
            self.db.groups.drop_indexes()
            self.db.calls.drop_indexes()

        except Exception as error:
            print(f"Error: {error}")
        finally:
            self.close()

    def execute_query_with_timing(self, collection_name, query, operation='find'):
        """Execute MongoDB query and measure execution time"""
        try:
            self.connect()

            start_time = time.time()

            collection = self.db[collection_name]
            if operation == 'find':
                result = collection.find(query)
                # Materialize the cursor to get actual execution time
                list(result)
            elif operation == 'aggregate':
                result = collection.aggregate(query)
                # Materialize the cursor to get actual execution time
                list(result)
            elif operation == 'update':
                result = collection.update_many(query[0], query[1])
            elif operation == 'delete':
                result = collection.delete_many(query)

            end_time = time.time()

            return end_time - start_time

        except Exception as error:
            print(f"Error: {error}")
        finally:
            self.close()

    def create_collections(self):
        """Create collections with validation schemas"""
        try:
            self.connect()

            # Contacts collection
            self.db.create_collection("contacts", validator={
                "$jsonSchema": {
                    "bsonType": "object",
                    "required": ["_id", "first_name", "last_name", "email"],
                    "properties": {
                        "email": {
                            "bsonType": "string",
                            "pattern": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
                        },
                        "phone_number": {
                            "bsonType": "string"
                        },
                        "groups": {
                            "bsonType": "array",
                            "items": {
                                "bsonType": "string"
                            }
                        }
                    }
                }
            })

            # Groups collection
            self.db.create_collection("groups", validator={
                "$jsonSchema": {
                    "bsonType": "object",
                    "required": ["_id", "name"],
                    "properties": {
                        "name": {
                            "bsonType": "string"
                        },
                        "members": {
                            "bsonType": "array",
                            "items": {
                                "bsonType": "string"
                            }
                        }
                    }
                }
            })

            # Calls collection
            self.db.create_collection("calls", validator={
                "$jsonSchema": {
                    "bsonType": "object",
                    "required": ["_id", "duration", "call_date"],
                    "properties": {
                        "duration": {
                            "bsonType": "int"
                        },
                        "call_date": {
                            "bsonType": "date"
                        }
                    }
                }
            })

        except Exception as error:
            print(f"Error: {error}")
        finally:
            self.close()