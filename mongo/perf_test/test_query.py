from datetime import datetime
from faker import Faker
from mongo.database.mongodb_executor import MongoDBExecutor


def test_insert_random_guest(database_service: MongoDBExecutor):
    fake = Faker()
    document = {
        "first_name": fake.first_name() + " insert",
        "last_name": fake.last_name(),
        "email": fake.email(),
        "phone_number": fake.phone_number(),
        "address": fake.address(),
        "created_at": datetime.now(),
        "groups": [],
        "calls": []
    }
    return database_service.execute_query_with_timing("contacts",
        lambda coll: coll.insert_one(document))

def test_update_random_guest(database_service: MongoDBExecutor):
    fake = Faker()
    update_data = {
        "first_name": fake.first_name() + " updated",
        "last_name": fake.last_name(),
        "email": fake.email(),
        "phone_number": fake.phone_number(),
        "address": fake.address(),
        "created_at": datetime.now()
    }
    return database_service.execute_query_with_timing("contacts",
        lambda coll: coll.update_one(
            {"id": "20"}, # Empty filter to update first matching document
            {"$set": update_data}
        ))

def test_delete_random_guest(database_service: MongoDBExecutor):
    return database_service.execute_query_with_timing("contacts",
        lambda coll: coll.delete_one({ "_id": "50" }))  # Empty filter to delete first matching document

def test_select_calls_by_date(database_service: MongoDBExecutor):
    return database_service.execute_query_with_timing("contacts",
        lambda coll: coll.find({
            "calls.date": datetime(2025, 1, 3)
        }))


def test_select_calls_by_participants(database_service: MongoDBExecutor):
    return database_service.execute_query_with_timing("contacts",
        lambda coll: coll.find({
            "calls.participants": {"$size": {"$gt": 5}}
        }))


def test_select_groups_with_john(database_service: MongoDBExecutor):
    return database_service.execute_query_with_timing("contacts",
        lambda coll: coll.find({
            "first_name": "John"
        }))


def test_select_phone_plus_one(database_service: MongoDBExecutor):
    return database_service.execute_query_with_timing("contacts",
        lambda coll: coll.find({
            "phone_number": {"$regex": "^\\+1"}
        }))


def test_select_email_org(database_service: MongoDBExecutor):
    return database_service.execute_query_with_timing("contacts",
        lambda coll: coll.find({
            "email": {"$regex": "org$"}
        }))


def test_select_phone_plus_one_limit(database_service: MongoDBExecutor):
    return database_service.execute_query_with_timing("contacts",
        lambda coll: coll.find({
            "phone_number": {"$regex": "^\\+1"}
        }).limit(5))


def test_select_email_org_limit(database_service: MongoDBExecutor):
    return database_service.execute_query_with_timing("contacts",
        lambda coll: coll.find({
            "email": {"$regex": "org$"}
        }).limit(5))

def measure_mongo_times(database_service: MongoDBExecutor):
    def perform_measurements(measurements, row_num):
        # CRUD operations
        measurements['insert'].append(
            (test_insert_random_guest(database_service), row_num)
        )
        measurements['update'].append(
            (test_update_random_guest(database_service), row_num)
        )
        measurements['delete'].append(
            (test_delete_random_guest(database_service), row_num)
        )

        # Complex queries
        measurements['select_calls_by_date'].append(
            (test_select_calls_by_date(database_service), row_num)
        )
        measurements['select_calls_by_participants'].append(
            (test_select_calls_by_participants(database_service), row_num)
        )
        measurements['select_groups_with_john'].append(
            (test_select_groups_with_john(database_service), row_num)
        )
        measurements['select_phone_plus_one'].append(
            (test_select_phone_plus_one(database_service), row_num)
        )
        measurements['select_email_org'].append(
            (test_select_email_org(database_service), row_num)
        )
        measurements['select_phone_plus_one_limit'].append(
            (test_select_phone_plus_one_limit(database_service), row_num)
        )
        measurements['select_email_org_limit'].append(
            (test_select_email_org_limit(database_service), row_num)
        )


    measurement_keys = [
        'insert', 'update', 'delete',
        'select_calls_by_date', 'select_calls_by_participants',
        'select_groups_with_john', 'select_phone_plus_one',
        'select_email_org', 'select_phone_plus_one_limit',
        'select_email_org_limit'
    ]

    measurements = {key: [] for key in measurement_keys}
    measurements_w_idx = {key: [] for key in measurement_keys}

    row_nums = [1000]

    for row_num in row_nums:
        database_service.load_csv(row_num)

        perform_measurements(measurements, row_num)

        database_service.create_indexes()
        perform_measurements(measurements_w_idx, row_num)
        database_service.drop_indexes()

    return measurements, measurements_w_idx