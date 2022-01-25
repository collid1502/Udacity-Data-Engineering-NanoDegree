import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    This function processes the drop table SQL queries, imported from the sql_queries.py file, one by one
    This will wipe any existing table that exists within the ETL process.
    
    Simply pass the database connection details into the 'conn' paramter, creating a cursor for the 'cur' parameter to process.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    This function processes the create table SQL queries, imported from the sql_queries.py file, one by one
    This will build all required staging & analytics tables, as set out in the create_table_queries list of queries.
    
    Simply pass the database connection details into the 'conn' paramter, creating a cursor for the 'cur' parameter to process.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This function actually runs the execution of the above two functions sequentially.
    
    It will collect the required params from a config file, open a DB connection, then process
    1. drop_tables
    2.create_tables
    before closing the connection. 
    
    No parameters are required to be passed to this function when called.
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