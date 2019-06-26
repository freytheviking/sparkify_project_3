import configparser
import psycopg2
from utils import drop_tables, create_tables
from sql_queries import create_table_queries, drop_table_queries


def main():
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print("Connecting to Amazon Redshift...")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print("Successfully connected to Amazon Redshift")

    print("Dropping any existing tables...")
    drop_tables(cur, conn)
    
    print("Creating Tables...")
    create_tables(cur, conn)
    print("Done Creating Tables.")

    conn.close()
    print("Disconnected from Amazon Redshift.")


if __name__ == "__main__":
    main()