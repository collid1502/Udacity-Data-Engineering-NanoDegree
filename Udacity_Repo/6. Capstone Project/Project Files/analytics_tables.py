"""
Script that will build the analytical tables, from the staging tables, for the ETL of data into the 
UK Road Accidents database for Department of Transport.
"""
#------------------------------------------------------------------------------------------------------------------------------------------------

import configparser
import psycopg2


## CONFIG
config = configparser.ConfigParser()
config.read('AWS.cfg') 

#------------------------------------------------------------------------------------------------------------------------------------------------

# DROP ANY EXISTING ANALYTICS TABLES 
accidents_drop = "DROP TABLE IF EXISTS Accidents" 
vehicles_drop = "DROP TABLE IF EXISTS Vehicles" 
locations_drop = "DROP TABLE IF EXISTS Locations" 
test_centre_drop = "DROP TABLE IF EXISTS Driving_Test_Centres" 
time_drop = "DROP TABLE IF EXISTS DateTime" 

drop_queries = [accidents_drop, vehicles_drop, locations_drop, test_centre_drop, time_drop] 

#------------------------------------------------------------------------------------------------------------------------------------------------
# Note redshift doesnt operate PRIMARY KEYs - so no actual need to specify, but we will show them in the Data Model 

# CREATE ANALYTICS TABLES 
accidents_create = ("""
CREATE TABLE IF NOT EXISTS Accidents
(
    Accident_Index VARCHAR NOT NULL,
    Vehicle_Number INT NOT NULL, 
    Vehicle_Key INT,
    DateTime_Key VARCHAR NOT NULL, 
    Latitude VARCHAR,
    Longitude VARCHAR,
    LSOA VARCHAR, 
    First_Road_Class VARCHAR, 
    First_Road_Number VARCHAR,
    Road_Type VARCHAR, 
    Road_Surface_Conditions VARCHAR, 
    Weather_Conditions VARCHAR,
    Police_Attended VARCHAR,
    Number_of_Casualties VARCHAR,
    Number_of_Vehicles VARCHAR,
    Accident_Severity VARCHAR,
    Age_Band_of_Driver VARCHAR,
    Sex_of_Driver VARCHAR,
    Age_of_Vehicle VARCHAR,
    First_Point_of_Impact VARCHAR
)
sortkey(Accident_Index, Vehicle_Number) 
""") 

vehicles_create = ("""
CREATE TABLE IF NOT EXISTS Vehicles
(
    Vehicle_Key BIGINT IDENTITY(0,1) sortkey,
    Vehicle_Type VARCHAR,
    Make VARCHAR,
    Model VARCHAR,
    Engine_Capacity VARCHAR
)
diststyle all
""")

locations_create = ("""
CREATE TABLE IF NOT EXISTS Locations
(
    LSOA VARCHAR NOT NULL sortkey distkey,
    Constituency VARCHAR,
    Country VARCHAR,
    Police_Force VARCHAR,
    Avg_Income DECIMAL(20,2) 
)
""")

test_centre_create = ("""
CREATE TABLE IF NOT EXISTS Driving_Test_Centres
(
    Latitude VARCHAR NOT NULL,
    Longitude VARCHAR NOT NULL,
    Name VARCHAR NOT NULL ,
    City VARCHAR,
    Postcode VARCHAR
)
diststyle all 
""") 

datetime_create = ("""
CREATE TABLE IF NOT EXISTS DateTime
(
    DateTime_Key BIGINT IDENTITY(0,1) sortkey,
    Date DATE,
    Time VARCHAR,
    Year VARCHAR,
    Month VARCHAR,
    Day VARCHAR,
    Weekday VARCHAR
)
diststyle all
""") 

create_queries = [accidents_create, vehicles_create, locations_create, test_centre_create, datetime_create]

#------------------------------------------------------------------------------------------------------------------------------------------------

# INSERT INTO ANALYTICS TABLES FROM STAGING 

insert_vehicles = ("""
INSERT INTO Vehicles 
(Vehicle_Type, Make, Model, Engine_Capacity)
SELECT DISTINCT 
    a.Vehicle_Type, 
    a.Make,
    a.Model,
    a.Engine_Capacity_CC
FROM 
    vehicles_staging as a 
INNER JOIN
    accidents_staging as b 
ON
a.Accident_Index = b.Accident_Index 
""")

insert_locations = ("""
INSERT INTO Locations
(LSOA, Constituency, Country, Police_Force, Avg_Income) 
SELECT DISTINCT 
    LSOA_Code, 
    Constituency, 
    Country, 
    Police_Force, 
    Average_Income
FROM lsoa_staging
""") 

insert_test_centres = ("""
INSERT INTO Driving_Test_Centres
(Latitude, Longitude, Name, City, Postcode) 
SELECT DISTINCT 
    Lat, 
    Long, 
    Name, 
    City, 
    Postcode
FROM test_centre_staging
""")

insert_datetime = ("""
INSERT INTO DateTime
(Date, Time, Year, Month, Day, Weekday)
SELECT
    a.date_,
    a.Time,
    EXTRACT(YEAR from a.date_),
    EXTRACT(MONTH from a.date_),
    EXTRACT(DAY from a.date_),
    EXTRACT(WEEKDAY from a.date_) 
FROM
    (
        SELECT DISTINCT 
            TO_DATE(Date, 'YYYY-MM-DD') as date_,
            Time
        FROM accidents_staging 
    ) as a 
""") 

insert_accidents = ("""
INSERT INTO Accidents
(Accident_Index, Vehicle_Number, Vehicle_Key, DateTime_Key, Latitude, Longitude, LSOA, First_Road_Class, First_Road_Number, Road_Type, Road_Surface_Conditions,
Weather_Conditions, Police_Attended, Number_of_Casualties, Number_of_Vehicles, Accident_Severity, Age_Band_of_Driver, Sex_of_Driver, Age_of_Vehicle, First_Point_of_Impact)
SELECT
    a.Accident_Index,
    b.Vehicle_Number,
    b.Vehicle_Key,
    c.DateTime_Key,
    a.Latitude,
    a.Longitude,
    a.LSOA_of_Accident_Location, 
    a.Road_Class, 
    a.Road_Number, 
    a.Road_Type,
    a.Road_Surface_Conditions, 
    a.Weather_Conditions, 
    a.Police_Attended, 
    a.Number_of_Casualties,
    a.Number_of_Vehicles,
    a.Accident_Severity,
    b.Age_Band_of_Driver,
    b.Sex_of_Driver,
    b.Age_of_Vehicle,
    b.X1st_Point_of_Impact

FROM 
    accidents_staging as a 
INNER JOIN 
    (SELECT 
        t2.Vehicle_Key,
        t1.Accident_Index, t1.Age_Band_of_Driver, t1.Sex_of_Driver, t1.Age_of_Vehicle, t1.X1st_Point_of_Impact,
        ROW_NUMBER () OVER (PARTITION BY Accident_Index) as Vehicle_Number

            FROM 
                vehicles_staging as t1
            LEFT JOIN 
                vehicles as t2
            ON
            t1.make = t2.Make and t1.model = t2.Model and t1.Vehicle_Type = t2.Vehicle_Type and t1.Engine_Capacity_CC = t2.Engine_Capacity
    ) as b 
ON 
a.Accident_Index = b.Accident_Index 

LEFT JOIN 
    DateTime as c
ON
TO_DATE(a.Date, 'YYYY-MM-DD') = c.Date and a.Time = c.Time 
""") 

insert_queries = [insert_vehicles, insert_locations, insert_test_centres, insert_datetime, insert_accidents]

#------------------------------------------------------------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------------------------------------------------------------

# The follwing function are created to drop, create and then load data for each of the staging tables above 

def drop_tables(cur, conn):
    """
    This function will process each of the drop queries that are written above. It thus wipes any pre-existing tables should they exist.

    Database connection details will be passed into the `conn` parameter, then creating a `cur` parameter to execute commands with.
    """
    for query in drop_queries:
        cur.execute(query)
        conn.commit() 


def create_tables(cur, conn):
    """
    This function will process each of the create table queries that are written above. It builds empty tables should they not already exist.

    Database connection details will be passed into the `conn` parameter, then creating a `cur` parameter to execute commands with.
    """
    for query in create_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This function will process each of the insert into table queries that are written above. It takes data from staging tables to the redshift table referenced.

    Database connection details will be passed into the `conn` parameter, then creating a `cur` parameter to execute commands with.
    """
    for query in insert_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This function actually runs the execution of the above three functions sequentially.
    
    It will collect the required params from a config file, open a DB connection, then process
    1. drop tables
    2. create tables
    3. insert tables
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

    drop_tables(cur, conn)
    create_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()

# end of script 