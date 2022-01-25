"""
This script provides some clean up by removing staging files from the redshift database once the analytics tables are built.
"""

import configparser
import psycopg2


## CONFIG
config = configparser.ConfigParser()
config.read('AWS.cfg') 

#------------------------------------------------------------------------------------------------------------------------------------------------

# DROP ANY EXISTING STAGING TABLES 
staging_accidents_drop = "DROP TABLE IF EXISTS accidents_staging" 
staging_vehicles_drop = "DROP TABLE IF EXISTS vehicles_staging" 
staging_lsoa_drop = "DROP TABLE IF EXISTS lsoa_staging" 
staging_test_centre_drop = "DROP TABLE IF EXISTS test_centre_staging" 

drop_table_queries = [staging_accidents_drop, staging_vehicles_drop, staging_lsoa_drop, staging_test_centre_drop]


def drop_stages(cur, conn):
    """
    This function will process each of the drop queries that are written above. It thus wipes any pre-existing tables should they exist.

    Database connection details will be passed into the `conn` parameter, then creating a `cur` parameter to execute commands with.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit() 



def main():
    """
    This function actually runs the execution of the above three functions sequentially.
    
    It will collect the required params from a config file, open a DB connection, then process
    1. drop tables
    before closing the connection. 
    
    No parameters are required to be passed to this function when called.
    """
    config = configparser.ConfigParser() 
    config.read('AWS.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}"
                            .format(config.get('CLUSTER','host'), config.get('DWH','dwh_db'), config.get('DWH','dwh_db_user'), 
                            config.get('DWH','dwh_db_password'), config.get('DWH','dwh_port'))
                            )
    cur = conn.cursor()

    drop_stages(cur, conn)

    conn.close()



if __name__ == "__main__":
    main()

# end of script 