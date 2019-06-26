import pandas as pd
from sql_queries import create_table_queries, copy_table_queries, drop_table_queries, insert_table_queries, delete_copy_tables

def drop_tables(cur, conn):
    """
    Drops all fact and dimensional tables.

    Args:
        cur: A psycopg2 cursor object
        conn: A psycopg2 connection object
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
	"""
	Creates staging, fact and dimensional tables.

	Args:
        cur: A psycopg2 cursor object
        conn: A psycopg2 connection object
	"""
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        
def load_staging_tables(cur, conn):
	"""
	COPY data from S3 to staging tables.

	Args:
        cur: A psycopg2 cursor object
        conn: A psycopg2 connection object
	"""
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
	"""
	Insert data to fact and dimensional tables from
	staging tables.

	Args:
        cur: A psycopg2 cursor object
        conn: A psycopg2 connection object
	"""
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

        
def delete_staging_tables(cur, conn):
	"""
	Delete staging tables

	Args:
        cur: A psycopg2 cursor object
        conn: A psycopg2 connection object
	"""
    for query in delete_copy_tables:
        cur.execute(query)
        conn.commit()


def execute_query(cur, query):
    """
   	Helper function that takes in a psycopg2 cursor object, a query string
   	and returns the result of the query in a pandas dataframe.

    Args:
        cur: A psycopg2 cursor object
        filepath (str): The filepath to the song metadata json file
    """
    
    cur.execute(query)
    data = cur.fetchall()
    colnames = list(data[0].keys())
    
    return pd.DataFrame([[row[col] for col in colnames] for row in data], columns=colnames)