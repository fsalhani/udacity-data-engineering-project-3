import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Executes copy commands to load song and log data into staging tables.
    Queries are initialized in sql_queries.py.
    :param cur: psycopg cursor
    :param conn: psycopg connection
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Executes queries to insert into analytical tables.
    Queries are initialized in sql_queries.py.
    :param cur: psycopg cursor
    :param conn: psycopg connection
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Executes copy commands to setup staging tables.
    Executes inserts to analytical tables.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
