import csv
import os
import time

import psycopg2


class PostgresExecutor:

    def __init__(self, dbname, user, password, host, port):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port

    def load_csv(self, row_num):
        try:
            connection = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
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

            load_csv_to_db(f'../../../common/data/contacts{row_num}.csv', '''
                INSERT INTO contacts (id, first_name, last_name, email, phone_number, address, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            ''')

            load_csv_to_db(f'../../../common/data/groups{row_num}.csv', '''
                INSERT INTO groups (id, name, created_at)
                VALUES (%s, %s, %s)
            ''')

            load_csv_to_db(f'../../../common/data/contact_groups{row_num}.csv', '''
                INSERT INTO contact_groups (contact_id, group_id)
                VALUES (%s, %s)
            ''')

            load_csv_to_db(f'../../../common/data/calls{row_num}.csv', '''
                INSERT INTO calls (id, contact_id, duration, call_date)
                VALUES (%s, %s, %s, %s)
            ''')

            cursor.close()
            connection.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Error: {error}")

    @staticmethod
    def execute_file_sql(cursor, file_path):
        with open(file_path, 'r') as file:
            sql = file.read()
            cursor.execute(sql)

    def load_sql_file(self, sql_files):
        try:
            connection = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            cursor = connection.cursor()

            for sql_file in sql_files:
                if os.path.exists(sql_file):
                    self.execute_file_sql(cursor, sql_file)
                    connection.commit()
                else:
                    print(f"File {sql_file} does not exist.")

            cursor.close()
            connection.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Error: {error}")

    def drop_indexes(self):
        self.load_sql_file(['../../scripts/drop_indexes.sql'])  # check this path

    def create_indexes(self):
        self.load_sql_file(['../../scripts/indexes.sql'])

    def create_tables(self):
        self.load_sql_file(['../../scripts/create.sql'])

    def execute_query_with_timing(self, query):
        try:
            connection = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            cursor = connection.cursor()

            start_time = time.time()
            cursor.execute(query)
            connection.commit()
            end_time = time.time()

            cursor.close()
            connection.close()
            return end_time - start_time
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Error: {error}")
