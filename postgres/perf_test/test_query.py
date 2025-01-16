from datetime import datetime
from faker import Faker

from common.constants import NUMBERS_OF_ROWS
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
        # CRUD operations
        measurements['insert'].append(
            (test_insert(database_executor, row_num + 1), row_num)
        )
        measurements['update'].append(
            (test_update(database_executor), row_num)
        )
        measurements['delete'].append(
            (test_delete(database_executor, row_num + 1), row_num)
        )
        measurements['select_calls_by_date'].append(
            (database_executor.execute_query_with_timing("SELECT * FROM calls WHERE call_date = '2025-01-03';"), row_num)
        )
        measurements['select_calls_by_participants'].append(
            (database_executor.execute_query_with_timing("SELECT c.* FROM calls c JOIN Contacts_Calls cc ON c.id = cc.call_id GROUP BY c.id HAVING COUNT(cc.contact_id) > 5;"), row_num)
        )
        measurements['select_groups_with_john'].append(
            (database_executor.execute_query_with_timing("SELECT * FROM contacts WHERE first_name = 'John'"), row_num)
        )
        measurements['select_phone_plus_one'].append(
            (database_executor.execute_query_with_timing("SELECT * FROM contacts WHERE phone_number ~ '^\\+1'"), row_num)
        )
        measurements['select_email_org'].append(
            (database_executor.execute_query_with_timing("SELECT * FROM contacts WHERE email ~ 'org$'"), row_num)
        )
        measurements['select_phone_plus_one_limit'].append(
            (database_executor.execute_query_with_timing("SELECT * FROM contacts WHERE phone_number ~ '^\\+1' LIMIT 5"), row_num)
        )
        measurements['select_email_org_limit'].append(
            (database_executor.execute_query_with_timing("SELECT * FROM contacts WHERE email ~ 'org$' LIMIT 5"), row_num)
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

    row_nums = NUMBERS_OF_ROWS

    for row_num in row_nums:
        database_executor.drop_all()
        database_executor.create_tables()
        database_executor.load_csv(row_num)

        perform_measurements(measurements, row_num)

        database_executor.create_indexes()
        perform_measurements(measurements_w_idx, row_num)
        database_executor.drop_indexes()

    return measurements, measurements_w_idx
