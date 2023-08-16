import psycopg2
import os

# its common to load a json or csv file onto a Postgres table
# below code creates connection to a db, creates 3 tables and loads data from 3 .csv files onto corresponding tables.
# DDL query (creating a table) and a DML query (inserting data into table). both types are saved .sql extensions
# DDL queries typically do not return result sets like SELECT queries, only return if the execution was successful or not.
# used DB Navigator to test the connection via SQL console. to actually make the connection, I used code.
# recommended way to bulk load CSV files into a Postgres table: use copy_from() from psycopg2 to load csv file instead of looping each row and INSERT values
# Committing is essential to finalize the transaction and make the changes permanent in the database.

def db_connect():
    connection = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="postgres"
    )
    return connection
def run_sql_file(file_path, conn):
    try:
        with open(file_path, 'r') as f:
            queries = f.read().split(';')
            cursor = conn.cursor()
            for query in queries:
                cursor.execute(query)
        conn.commit()
        print("Queries executed successfully!")

    except (Exception, psycopg2.Error) as error:
        print("Error executing query:", error)

def ingest_csv_into_table(csv_file, table_name, conn):
    try:
        print(f'ingesting {csv_file} into {table_name} table...')

        with open(csv_file, 'r') as csvfile:
            cur = conn.cursor()
            # skip the header row
            next(csvfile)
            print(f'copying the data from {csv_file}')
            cur.copy_from(csvfile, table_name, sep=',')
        conn.commit()
        print(f'committed the changes to {table_name}...')
    except (Exception, psycopg2.Error) as error:
        print("Error:", error)
        conn.rollback()

if __name__ == "__main__":
    sql_file_path = 'queries.sql'
    conn = db_connect()
    run_sql_file(sql_file_path, conn)
    ingest_csv_into_table('data/accounts.csv', 'accounts', conn)
    ingest_csv_into_table('data/products.csv', 'products', conn)
    ingest_csv_into_table('data/transactions.csv', 'transactions', conn)
    conn.close()
