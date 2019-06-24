import pandas as pd
import psycopg2
from sql_queries import *


# Functions 
def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
        
def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        
def insert_tables(cur, conn):
    for query in insert_table_queries:
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
