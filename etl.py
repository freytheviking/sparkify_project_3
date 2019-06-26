import configparser
import psycopg2
from utils import load_staging_tables, insert_tables
from sql_queries import copy_table_queries, insert_table_queries


def main():
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    print("Connecting to Amazon Redshift...")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print("Successfully connected to Amazon Redshift")
    
    print("Loading staging tables...")
    load_staging_tables(cur, conn)
    
    print("Inserting final tables...")
    insert_tables(cur, conn)
    
    print("ETL complete.")
    conn.close()
    print("Disconnected from Amazon Redshift.")


if __name__ == "__main__":
    main()