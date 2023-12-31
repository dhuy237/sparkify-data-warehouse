import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Execute every drop table queries.
    
    Keyword arguments:
    cur -- PostgreSQL cursor
    conn -- PostgreSQL connection to the cluster
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Execute every create table queries.

    Keyword arguments:
    cur -- PostgreSQL cursor
    conn -- PostgreSQL connection to the cluster
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Main program

    Connect to the AWS Redshift then execute the queries.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()