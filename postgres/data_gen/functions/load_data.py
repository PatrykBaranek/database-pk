import psycopg2
import os


def load_sql_file(cursor, file_path):
    with open(file_path, 'r') as file:
        sql = file.read()
        cursor.execute(sql)


def load_data_to_db(dbname, user, password, host, port, sql_files):
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
                load_sql_file(cursor, sql_file)
                connection.commit()
            else:
                print(f"File {sql_file} does not exist.")

        cursor.close()
        connection.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")