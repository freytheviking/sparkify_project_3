import configparser
from utils import *

def main():
    
    print("Reading Configuration...")
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print("Connecting to Amazon Redshift...")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print("Successfully connected to Amazon Redshift.")
    
    print("Creating Tables...")
    create_tables(cur, conn)
    
    print("Loading tables...")
    load_staging_tables(cur, conn)
    
    print("Inserting final tables...")
    insert_tables(cur, conn)

    conn.close()
    print("ETL complete.")


if __name__ == "__main__":
    main()
    