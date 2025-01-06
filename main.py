from faker import Faker
from datetime import datetime

from common.plotting.plotting import plot_result
from db import MongoDBSingleton
from measurements import measure_add_contacts, measure_read_contacts, measure_update_contacts, \
    measure_delete_all_contacts
from postgres.database.functions.DatabaseService import DatabaseService
from postgres.perf_test.test_query import measure_postgres_times

database = MongoDBSingleton()
db = database.get_db()

def add_contacts(num_records):
    contacts = []
    for i in range(num_records):
        faker = Faker()
        contact = {
            'first_name': f"FirstName{i}",
            'last_name': faker.last_name(),
            'email': faker.email(),
            'phone_number': faker.phone_number(),
            'address': faker.address(),
            'created_at': datetime.now()
        }
        contacts.append(contact)
    db.contacts.insert_many(contacts)

def test_crud_operations():
    for record_size in [1000000]:
        print(f"\nTesting CRUD operations with {record_size} records:")

        measure_add_contacts(record_size)
        measure_read_contacts()
        measure_update_contacts(record_size)
        measure_delete_all_contacts()

#['insert', 'update', 'select_all', 'select_by_id', 'select_by_first_name', 'select_by_last_name', 'delete']
def main():
    postgres_service = DatabaseService('postgres', 'postgres', 'postgres', 'localhost', '5432')
    postgres_service.create_tables()
    postgres, postgres_w_idx = measure_postgres_times(postgres_service)
    mongo = [] # TODO: Measure MongoDB times

    plot_result(postgres['insert'], mongo, 'Postgres vs MongoDB insert')
    plot_result(postgres['update'], mongo, 'Postgres vs MongoDB update')
    plot_result(postgres['select_all'], mongo, 'Postgres vs MongoDB select all')
    plot_result(postgres['select_by_id'], mongo, 'Postgres vs MongoDB select by id')
    plot_result(postgres['select_by_first_name'], mongo, 'Postgres vs MongoDB select by first name')
    plot_result(postgres['select_by_last_name'], mongo, 'Postgres vs MongoDB select by last name')
    plot_result(postgres['delete'], mongo, 'Postgres vs MongoDB delete')

    plot_result(postgres_w_idx['insert'], mongo, 'Postgres vs MongoDB with indexes insert')
    plot_result(postgres_w_idx['update'], mongo, 'Postgres vs MongoDB with indexes update')
    plot_result(postgres_w_idx['select_all'], mongo, 'Postgres vs MongoDB with indexes select all')
    plot_result(postgres_w_idx['select_by_id'], mongo, 'Postgres vs MongoDB with indexes select by id')
    plot_result(postgres_w_idx['select_by_first_name'], mongo, 'Postgres vs MongoDB with indexes select by first name')
    plot_result(postgres_w_idx['select_by_last_name'], mongo, 'Postgres vs MongoDB with indexes select by last name')
    plot_result(postgres_w_idx['delete'], mongo, 'Postgres vs MongoDB with indexes delete')

if __name__ == "__main__":
    test_crud_operations()
