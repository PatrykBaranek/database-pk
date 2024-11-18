from faker import Faker
from datetime import datetime
from db import MongoDBSingleton
from measurements import measure_add_contacts, measure_read_contacts, measure_update_contacts, \
    measure_delete_all_contacts


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

if __name__ == "__main__":
    test_crud_operations()
