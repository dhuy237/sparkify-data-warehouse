import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Execute loading raw data to staging table on Redshift.
    
    Keyword arguments:
    cur -- PostgreSQL cursor
    conn -- PostgreSQL connection to the cluster
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Execute inserting data from staging tables to Dim-Fact table.
    
    Keyword arguments:
    cur -- PostgreSQL cursor
    conn -- PostgreSQL connection to the cluster
    """
    for query in insert_table_queries:
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
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()