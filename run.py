import configparser
import psycopg2
from test_sql_queries import create_table_queries, drop_table_queries, copy_table_queries, insert_table_queries

# Functions 
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
        
def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        
def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        
def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
         
def main():
    
    print("Reading Configuration...")
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    print("Connecting to Amazon Redshift...")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print("Successfully connected to Amazon Redshift")
    
    print("Dropping tables before creating them...")
    drop_tables(cur, conn)
    
    print("Creating Tables...")
    create_tables(cur, conn)
    
    print("Loading tables...")
    load_staging_tables(cur, conn)
    
    print("Inserting final tables...")
    insert_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()
