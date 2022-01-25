import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This function processes the COPY code for each of the staging tables within the copy_table_queries list
    
    Tables were previously built in create_tables.py
    Here, they are now loaded by collecting data from S3 locations specified in the config file.
    
    Simply pass the database connection details into the 'conn' paramter, creating a cursor for the 'cur' parameter to process.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This function processes the insert into for tables within the insert_table_queries list
    
    Tables were previously built in create_tables.py
    Here, those tables are now loaded by using queries using the events staging table, songs staging table or a comination fo both.
    
    Simply pass the database connection details into the 'conn' paramter, creating a cursor for the 'cur' parameter to process.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This function actually runs the execution of the above two functions sequentially.
    
    It will collect the required params from a config file, open a DB connection, then process
    1. load_staging_tables
    2. insert_tables
    before closing the connection. 
    
    No parameters are required to be passed to this function when called.
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