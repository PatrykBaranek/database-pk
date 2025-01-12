from datetime import datetime
from faker import Faker
from postgres.database.postgres_executor import PostgresExecutor


def test_insert(database_service: PostgresExecutor, id: int):
    fake = Faker()
    first_name = fake.first_name()
    last_name = fake.last_name()
    email = fake.email()
    phone_number = fake.phone_number()
    address = fake.address()
    created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return database_service.execute_query_with_timing(
        f"INSERT INTO contacts (id, first_name, last_name, email, phone_number, address, created_at) VALUES ('{id}', '{first_name}', '{last_name}', '{email}', '{phone_number}', '{address}', '{created_at}')")

def test_update(database_service: PostgresExecutor):
    fake = Faker()
    first_name = fake.first_name()
    last_name = fake.last_name()
    email = fake.email()
    phone_number = fake.phone_number()
    address = fake.address()
    created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return database_service.execute_query_with_timing(
        f"UPDATE contacts SET first_name = '{first_name}', last_name = '{last_name}', email = '{email}', phone_number = '{phone_number}', address = '{address}', created_at = '{created_at}' WHERE id = 1")

def test_delete(database_service: PostgresExecutor, id: int):
    return database_service.execute_query_with_timing(f"DELETE FROM contacts WHERE id = {id}")


def measure_postgres_times(database_executor: PostgresExecutor):
    def perform_measurements(measurements, row_num):
        measurements['insert'].append((test_insert(database_executor, row_num + 1), row_num))
        measurements['update'].append((test_update(database_executor), row_num))
        measurements['select_all'].append(
            (database_executor.execute_query_with_timing("SELECT * FROM contacts"), row_num))
        measurements['select_by_id'].append(
            (database_executor.execute_query_with_timing("SELECT * FROM contacts WHERE id = 1"), row_num))
        measurements['select_by_first_name'].append(
            (database_executor.execute_query_with_timing("SELECT * FROM contacts WHERE first_name = 'John'"), row_num))
        measurements['select_by_last_name'].append(
            (database_executor.execute_query_with_timing("SELECT * FROM contacts WHERE last_name = 'Doe'"), row_num))
        measurements['delete'].append((test_delete(database_executor, row_num + 1), row_num))

    measurements = {key: [] for key in
                    ['insert', 'update', 'select_all', 'select_by_id', 'select_by_first_name', 'select_by_last_name',
                     'delete']}
    measurements_w_idx = {key: [] for key in ['insert', 'update', 'select_all', 'select_by_id', 'select_by_first_name',
                                              'select_by_last_name', 'delete']}
    row_nums = [1000]

    for row_num in row_nums:
        database_executor.create_tables()
        database_executor.create_tables()
        database_executor.load_csv(row_num)
        perform_measurements(measurements, row_num)

        database_executor.create_indexes()
        perform_measurements(measurements_w_idx, row_num)
        database_executor.drop_indexes()

    return measurements, measurements_w_idx

