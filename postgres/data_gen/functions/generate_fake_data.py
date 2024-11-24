from faker import Faker
import random
from datetime import datetime

#TODO turn it to converting csv to sql
def generate_fake_data(num_contacts, num_groups, output_file):
    fake = Faker()
    with open(output_file, 'w') as f:
        for _ in range(num_contacts):
            first_name = fake.first_name()
            last_name = fake.last_name()
            email = fake.email()
            phone_number = fake.phone_number()
            address = fake.address()
            created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            f.write(f"INSERT INTO Contacts (first_name, last_name, email, phone_number, address, created_at) VALUES ('{first_name}', '{last_name}', '{email}', '{phone_number}', '{address}', '{created_at}') RETURNING id;\n")

        f.write("COMMIT;\n")

        contact_weights = [1] * num_contacts

        counter = 0
        for _ in range(num_groups):
            group_name = fake.word()
            f.write(f"INSERT INTO Groups (name, created_at) VALUES ('{group_name}', '{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}') ON CONFLICT (name) DO NOTHING;\n")

            num_contacts_in_group = random.randint(2, 50)
            contact_ids = random.choices(range(1, num_contacts + 1), weights=contact_weights, k=num_contacts_in_group)

            for contact_id in contact_ids:
                f.write(f"INSERT INTO Contact_Groups (contact_id, group_id) VALUES ({contact_id}, currval(pg_get_serial_sequence('Groups', 'id')));\n")
                contact_weights[contact_id - 1] *= 0.9

            counter += 1
            if counter >= 500:
                f.write("COMMIT;\n")
                counter = 0

        f.write("COMMIT;\n")

generate_fake_data(
    1000,
    100,
    'scripts/output1000.sql'
)
generate_fake_data(
    10000,
    1000,
    'scripts/output10000.sql'
)
generate_fake_data(
    100000,
    10000,
    'scripts/output100000.sql'
)
# generate_fake_data(
#     1000000,
#     100000,
#     'scripts/output1000000.sql'
# )