import csv
import random

from faker import Faker


def regenerate_contact_groups_and_calls(num_contacts, num_groups, num_calls, file_num):
    fake = Faker()
    print(f'Regenerating contact_groups and contact_calls for file {file_num}')

    contact_group_set = set()
    call_contact_set = set()

    with open(f'data/contact_groups{file_num}.csv', 'w', newline='') as contact_group_file, \
            open(f'data/contact_calls{file_num}.csv', 'w', newline='') as call_contacts_file:

        contact_groups_writer = csv.writer(contact_group_file)
        call_contacts_writer = csv.writer(call_contacts_file)

        contact_groups_writer.writerow(['contact_id', 'group_id'])
        call_contacts_writer.writerow(['contact_id', 'call_id'])

        for call_id in range(1, num_calls + 1):
            num_contacts_in_call = random.randint(2, 6)
            contact_ids = random.choices(range(1, num_contacts + 1), k=num_contacts_in_call)

            for contact_id in contact_ids:
                if (contact_id, call_id) not in call_contact_set:
                    call_contacts_writer.writerow([contact_id, call_id])
                    call_contact_set.add((contact_id, call_id))




regenerate_contact_groups_and_calls(
    50000,
        50000,
        50000,
        100000
)

regenerate_contact_groups_and_calls(
    300000,
    300000,
    300000,
    1000000
)