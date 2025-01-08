from datetime import datetime
from faker import Faker
from mongo.database.mongodb_executor import MongoDBExecutor


def test_insert(database_service: MongoDBExecutor):
    fake = Faker()
    document = {
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email": fake.email(),
        "phone_number": fake.phone_number(),
        "address": fake.address(),
        "created_at": datetime.now(),
        "groups": [],
        "calls": []
    }
    return database_service.execute_query_with_timing("contacts", document,'insert')


def test_update(database_service: MongoDBExecutor):
    fake = Faker()
    update_data = {
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email": fake.email(),
        "phone_number": fake.phone_number(),
        "address": fake.address(),
        "created_at": datetime.now()
    }
    query = [
        {"_id": "1"},
        {"$set": update_data}
    ]
    return database_service.execute_query_with_timing(
        "contacts",
        query,
        'update'
    )


def test_delete(database_service: MongoDBExecutor, id: str):
    query = {"_id": str(id)}
    return database_service.execute_query_with_timing(
        "contacts",
        query,
        'delete'
    )


def measure_mongo_times(database_service: MongoDBExecutor):
    def perform_measurements(measurements, row_num):
        # Insert test
        measurements['insert'].append(
            (test_insert(database_service), row_num)
        )

        # Update test
        measurements['update'].append(
            (test_update(database_service), row_num)
        )

        # Select all test
        measurements['select_all'].append(
            (database_service.execute_query_with_timing(
                "contacts",
                {},
                'find'
            ), row_num)
        )

        # Select by id test
        measurements['select_by_id'].append(
            (database_service.execute_query_with_timing(
                "contacts",
                {"_id": "1"},
                'find'
            ), row_num)
        )

        # Select by first name test
        measurements['select_by_first_name'].append(
            (database_service.execute_query_with_timing(
                "contacts",
                {"first_name": "John"},
                'find'
            ), row_num)
        )

        # Select by last name test
        measurements['select_by_last_name'].append(
            (database_service.execute_query_with_timing(
                "contacts",
                {"last_name": "Doe"},
                'find'
            ), row_num)
        )

        # Delete test
        measurements['delete'].append(
            (test_delete(database_service, str(row_num - 1)), row_num)
        )

    # Initialize measurement dictionaries
    measurements = {key: [] for key in [
        'insert', 'update', 'select_all', 'select_by_id',
        'select_by_first_name', 'select_by_last_name', 'delete'
    ]}

    measurements_w_idx = {key: [] for key in [
        'insert', 'update', 'select_all', 'select_by_id',
        'select_by_first_name', 'select_by_last_name', 'delete'
    ]}

    row_nums = [1000, 10000, 100000, 1000000]

    for row_num in row_nums:
        database_service.load_csv(row_num)

        perform_measurements(measurements, row_num)

        database_service.create_indexes()
        perform_measurements(measurements_w_idx, row_num)
        database_service.drop_indexes()

    return measurements, measurements_w_idx