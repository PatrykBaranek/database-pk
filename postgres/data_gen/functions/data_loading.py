import csv
import os

import psycopg2

def load_csv(dbname, user, password, host, port):
    try:
        connection = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        cursor = connection.cursor()

        def load_csv_to_db(file_path, insert_query):
            with open(file_path, 'r') as file:
                reader = csv.reader(file)
                next(reader)
                counter = 0
                for row in reader:
                    cursor.execute(insert_query, row)
                    counter += 1
                    if counter >= 500:
                        connection.commit()
                        counter = 0
                connection.commit()

        load_csv_to_db('data/contacts.csv', '''
            INSERT INTO contacts (id, first_name, last_name, email, phone_number, address, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        ''')

        load_csv_to_db('data/groups.csv', '''
            INSERT INTO groups (id, name, created_at)
            VALUES (%s, %s, %s)
        ''')

        load_csv_to_db('data/contact_groups.csv', '''
            INSERT INTO contact_groups (contact_id, group_id)
            VALUES (%s, %s)
        ''')

        load_csv_to_db('data/calls.csv', '''
            INSERT INTO calls (id, contact_id, duration, call_date)
            VALUES (%s, %s, %s, %s)
        ''')

        cursor.close()
        connection.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")


def execute_sql(cursor, file_path):
    with open(file_path, 'r') as file:
        sql = file.read()
        cursor.execute(sql)


def load_sql_file(dbname, user, password, host, port, sql_files):
    try:
        connection = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        cursor = connection.cursor()

        for sql_file in sql_files:
            if os.path.exists(sql_file):
                execute_sql(cursor, sql_file)
                connection.commit()
            else:
                print(f"File {sql_file} does not exist.")

        cursor.close()
        connection.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")