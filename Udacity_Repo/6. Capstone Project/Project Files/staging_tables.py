"""
Script that will build the staging tables for the ETL of data into the 
UK Road Accidents database for Department of Transport.
"""
#------------------------------------------------------------------------------------------------------------------------------------------------

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

#------------------------------------------------------------------------------------------------------------------------------------------------

# CREATE STAGING TABLES 
staging_accidents_create = ("""
CREATE TABLE IF NOT EXISTS accidents_staging 
(
    Accident_Index VARCHAR,
    Road_Class VARCHAR,
    Road_Number VARCHAR,
    Accident_Severity VARCHAR,
    Date VARCHAR, 
    Latitude VARCHAR,
    Longitude VARCHAR,
    LSOA_of_Accident_Location VARCHAR,
    Number_of_Casualties VARCHAR,
    Number_of_Vehicles VARCHAR,
    Road_Surface_Conditions VARCHAR,
    Road_Type VARCHAR,
    Speed_Limit VARCHAR,
    Time VARCHAR,
    Weather_Conditions VARCHAR,
    Police_Attended VARCHAR
)
""")

staging_vehicles_create = ("""
CREATE TABLE IF NOT EXISTS vehicles_staging 
(
    Accident_Index VARCHAR,
    Age_Band_of_Driver VARCHAR,
    Age_of_Vehicle VARCHAR,
    Engine_Capacity_CC VARCHAR,
    make VARCHAR,
    model VARCHAR,
    Sex_of_Driver VARCHAR,
    Vehicle_Manoeuvre VARCHAR,
    X1st_Point_of_Impact VARCHAR,
    Vehicle_Type VARCHAR
)
""")

staging_lsoa_create = ("""
CREATE TABLE IF NOT EXISTS lsoa_staging 
(
    Country VARCHAR,
    Constituency VARCHAR,
    LSOA_Code VARCHAR,
    Police_Force VARCHAR, 
    Average_Income FLOAT
)
""")

staging_test_centre_create = ("""
CREATE TABLE IF NOT EXISTS test_centre_staging 
(
    Name VARCHAR,
    City VARCHAR,
    County VARCHAR,
    Postcode VARCHAR,
    Lat VARCHAR,
    Long VARCHAR,
    Closing_date VARCHAR
)
""") 

create_table_queries = [staging_accidents_create, staging_vehicles_create, staging_lsoa_create, staging_test_centre_create]

#------------------------------------------------------------------------------------------------------------------------------------------------

# COPY INTO STAGING TABLES FROM S3 BUCKET 

staging_accidents_copy = ("""
COPY accidents_staging FROM '{}'
credentials 'aws_iam_role={}' 
CSV
delimiter '~'
IGNOREHEADER 1 
compupdate off region '{}'; 
"""
).format(config.get('S3','accidents'), config.get('CLUSTER','role_arn'), config.get('BUCKET','region'))

staging_vehicles_copy = ("""
COPY vehicles_staging FROM '{}' 
credentials 'aws_iam_role={}' 
CSV
delimiter '~'
IGNOREHEADER 1 
compupdate off region '{}'; 
"""
).format(config.get('S3','vehicles'), config.get('CLUSTER','role_arn'), config.get('BUCKET','region'))

staging_lsoa_copy = ("""
COPY lsoa_staging FROM '{}'
credentials 'aws_iam_role={}' 
CSV
delimiter '~'
IGNOREHEADER 1 
compupdate off region '{}'; 
"""
).format(config.get('S3','lsoa'), config.get('CLUSTER','role_arn'), config.get('BUCKET','region'))

staging_test_centre_copy = ("""
COPY test_centre_staging FROM '{}' 
credentials 'aws_iam_role={}' 
JSON 'auto ignorecase' 
compupdate off region '{}'; 
"""
).format(config.get('S3','test_centre'), config.get('CLUSTER','role_arn'), config.get('BUCKET','region')) 

copy_table_queries = [staging_accidents_copy, staging_vehicles_copy, staging_lsoa_copy, staging_test_centre_copy]

#------------------------------------------------------------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------------------------------------------------------------

# The follwing function are created to drop, create and then load data for each of the staging tables above 

def drop_stages(cur, conn):
    """
    This function will process each of the drop queries that are written above. It thus wipes any pre-existing tables should they exist.

    Database connection details will be passed into the `conn` parameter, then creating a `cur` parameter to execute commands with.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit() 


def create_stages(cur, conn):
    """
    This function will process each of the create table queries that are written above. It builds empty tables should they not already exist.

    Database connection details will be passed into the `conn` parameter, then creating a `cur` parameter to execute commands with.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def copy_stages(cur, conn):
    """
    This function will process each of the copy table queries that are written above. It copies data from S3 buckets to the redshift table.

    Database connection details will be passed into the `conn` parameter, then creating a `cur` parameter to execute commands with.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This function actually runs the execution of the above three functions sequentially.
    
    It will collect the required params from a config file, open a DB connection, then process
    1. drop tables
    2. create tables
    3. copy tables
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
    create_stages(cur, conn)
    copy_stages(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()

# end of script 