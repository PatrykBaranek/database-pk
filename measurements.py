from timeit import timeit
from db import MongoDBSingleton


database = MongoDBSingleton()
db = database.get_db()

def measure_add_contacts(num_records):
    setup_code = f"from __main__ import add_contacts; num_records={num_records}"
    execution_time = timeit("add_contacts(num_records)", setup=setup_code, number=1)
    print(f"Time to add {num_records} contacts: {execution_time:.2f} seconds")

def measure_read_contacts():
    setup_code = "from __main__ import db"
    execution_time = timeit("list(db.contacts.find())", setup=setup_code, number=1)
    print(f"Time to read contacts: {execution_time:.2f} seconds")


def measure_update_contacts(num_records):
    setup_code = f"from __main__ import db; num_records={num_records}"
    execution_time = timeit(
        "db.contacts.update_one({'first_name': f'FirstName{0}'}, {'$set': {'phone_number': '+48987654321'}})",
        setup=setup_code, number=num_records)
    print(f"Time to update {num_records} contacts: {execution_time:.2f} seconds")


def measure_delete_all_contacts():
    execution_time = timeit("db.contacts.delete_many({})", setup="from __main__ import db", number=1)
    print(f"Time to delete all contacts: {execution_time:.2f} seconds")


def measure_delete_single_one_contact(num_records):
    setup_code = f"from __main__ import db; num_records={num_records}"
