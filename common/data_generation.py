import csv
from faker import Faker
import random
from datetime import datetime
import secrets
import string

def generate_fake_data(num_contacts, num_groups, num_calls):
    fake = Faker()

    with open('data/contacts.csv', 'w', newline='') as contact_file, \
         open('data/groups.csv', 'w', newline='') as group_file, \
         open('data/contact_groups.csv', 'w', newline='') as contact_group_file, \
         open('data/calls.csv', 'w', newline='') as call_file, \
         open('data/contact_calls.csv', 'w', newline='') as call_contacts_file:

        contacts_writer = csv.writer(contact_file)
        groups_writer = csv.writer(group_file)
        contact_groups_writer = csv.writer(contact_group_file)
        call_writer = csv.writer(call_file)
        call_contacts_writer = csv.writer(call_contacts_file)

        contacts_writer.writerow(['id', 'first_name', 'last_name', 'email', 'phone_number', 'address', 'created_at'])
        groups_writer.writerow(['id', 'name', 'created_at'])
        contact_groups_writer.writerow(['contact_id', 'group_id'])
        call_writer.writerow(['id', 'duration', 'call_date'])
        call_contacts_writer.writerow(['contact_id', 'call_id'])

        for contact_id in range(1, num_contacts + 1):
            first_name = fake.first_name()
            last_name = fake.last_name()
            email = fake.email()
            phone_number = fake.phone_number()
            address = fake.address()
            created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            contacts_writer.writerow([contact_id, first_name, last_name, email, phone_number, address, created_at])

        contact_weights = [1] * num_contacts

        for group_id in range(1, num_groups + 1):
            group_name = fake.word()
            random_string = ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(6))
            group_name_with_random_string = f"{group_name}_{random_string}"
            created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            groups_writer.writerow([group_id, group_name_with_random_string, created_at])

            num_contacts_in_group = random.randint(2, 50)
            contact_ids = random.choices(range(1, num_contacts + 1), weights=contact_weights, k=num_contacts_in_group)

            for contact_id in contact_ids:
                contact_groups_writer.writerow([contact_id, group_id])
                contact_weights[contact_id - 1] *= 0.9

        for call_id in range(1, num_calls + 1):
            duration = random.randint(1, 180)
            call_date = fake.date_time_this_year().strftime('%Y-%m-%d %H:%M:%S')

            call_writer.writerow([call_id, duration, call_date])

            num_contacts_in_call = random.randint(2, 20)
            contact_ids = random.choices(range(1, num_contacts + 1), k=num_contacts_in_call)

            for contact_id in contact_ids:
                call_contacts_writer.writerow([contact_id, call_id])

generate_fake_data(
    1000,
    1000,
    1000
)