import configparser


## CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg') 

#------------------------------------------------------------------------------------------------------------------------------------------------

# DROP ANY EXISTING TABLES TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS events_staging"
staging_songs_table_drop = "DROP TABLE IF EXISTS songs_staging"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

#------------------------------------------------------------------------------------------------------------------------------------------------

# CREATE NEW TABLES
## Staging tables
### Start by creating most columns for staging table as VARCHAR. This is for development. When we push to analytics tables, we can identify correct data types
### For obvious numbers/decimals, use INT/FLOAT based on the example data extracts in project Info page
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS events_staging
( 
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender VARCHAR,
    itemInSession INT,
    lastName VARCHAR,
    length FLOAT, 
    level VARCHAR,
    location VARCHAR,
    method VARCHAR, 
    page VARCHAR, 
    registration VARCHAR,
    sessionId VARCHAR,
    song VARCHAR,
    status VARCHAR,
    ts BIGINT,
    userAgent VARCHAR,
    userId VARCHAR
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS songs_staging
(
    num_songs INT,
    artist_id VARCHAR,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration FLOAT, 
    year VARCHAR
)
""") 

#------------------------------------------------------------------------------------------------------------------------------------------------

## FACT & DIMENSION tables 
### NOTE - "Primary Keys" & "Uniqueness" are not enforced by AWS Redshift, so the statements are not technically required for the below to run.
### However, they can be specified to add some information to the ETL, in case some later process in the application would enforce these constraints
### AWS Redshift DOES enforce the NOT NULL statement 
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays 
(
songplay_id INT IDENTITY(0,1) PRIMARY KEY, 
start_time TIMESTAMP NOT NULL distkey,
user_id VARCHAR(10) NOT NULL, 
level VARCHAR,
song_id VARCHAR,
artist_id VARCHAR, 
session_id VARCHAR, 
location VARCHAR,
user_agent VARCHAR
) 
sortkey(start_time, song_id) 
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
user_id VARCHAR NOT NULL PRIMARY KEY sortkey distkey, 
first_name VARCHAR, 
last_name VARCHAR, 
gender VARCHAR,
level VARCHAR
) 
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs 
(
song_id VARCHAR NOT NULL UNIQUE PRIMARY KEY sortkey distkey, 
title VARCHAR, 
artist_id VARCHAR, 
year VARCHAR, 
duration FLOAT
) 
""") 

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(
artist_id VARCHAR NOT NULL UNIQUE PRIMARY KEY distkey,
name VARCHAR, 
location VARCHAR, 
latitude FLOAT, 
longitude FLOAT
) 
sortkey(location, name) 
""") 

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
start_time TIMESTAMP NOT NULL UNIQUE PRIMARY KEY sortkey, 
hour VARCHAR, 
day VARCHAR, 
week VARCHAR, 
month VARCHAR,
year VARCHAR, 
weekday VARCHAR
) 
diststyle all 
""")

#------------------------------------------------------------------------------------------------------------------------------------------------

# INSERT DATA TO TABLES 
## STAGING TABLES - copy data from S3 buckets to created staging tables

# locate the paths to each of the log data & song data. Place into objects. 
# These will be called within the .format inside the COPY queries for load staging tables
# Also collect the ARN value from the IAM_ROLE
# Also collect the JSON path for the log data to aide COPY statement in Logs. Songs will use 'auto' .
# We do this for the Log Data because if JSON data objects don't correspond directly to column names, you can use a file to map the JSON elements to columns.

staging_events_copy = ("""
COPY events_staging FROM {} 
credentials 'aws_iam_role={}'
JSON {} 
compupdate off region 'us-west-2';
""").format(config.get('S3','LOG_DATA') , config.get('IAM_ROLE','ARN'), config.get('S3','LOG_JSONPATH')) 

staging_songs_copy = ("""
COPY songs_staging FROM {} 
credentials 'aws_iam_role={}'
JSON 'auto' 
compupdate off region 'us-west-2';
""").format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE','ARN')) 


#------------------------------------------------------------------------------------------------------------------------------------------------

## FINAL TABLES - load data from staging tables into created FACT & DIMENSION tables
songplay_table_insert = ("""
INSERT INTO songplays
(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
SELECT 
    TIMESTAMP 'epoch' + a.ts/1000 * INTERVAL '1 second', 
    a.userId, 
    a.level,
    b.song_id,
    b.artist_id, 
    a.sessionId,
    a.location,
    a.userAgent
FROM events_staging as a 
LEFT JOIN songs_staging as b 
ON a.song = b.title 
WHERE a.page = 'NextSong' 
""")

# For users, we need to be able to handle any "Updates". Ensure we have the latest record per User only, but filtering to max TS per user
user_table_insert = ("""
INSERT INTO users
(user_id, first_name, last_name, gender, level)
SELECT DISTINCT
    a.userId, 
    a.firstName, 
    a.lastName, 
    a.gender, 
    a.level
FROM events_staging as a 
WHERE a.userId IS NOT NULL 
AND a.ts = (SELECT MAX(b.ts) FROM events_staging as b WHERE b.userId = a.userID) 
ORDER BY a.userId DESC 
""")

song_table_insert = ("""
INSERT INTO songs 
(song_id, title, artist_id, year, duration)
SELECT DISTINCT 
    song_id,
    title,
    artist_id,
    year,
    duration
FROM songs_staging
""")

# ensure we take the most recent Artist postion. We can do that by finding the most recent year that exists per artist.
artist_table_insert = ("""
INSERT INTO artists
(artist_id, name, location, latitude, longitude) 
SELECT DISTINCT 
    a.artist_id,
    a.artist_name,
    a.artist_location,
    a.artist_latitude,
    a.artist_longitude
FROM songs_staging as a 
WHERE a.artist_id IS NOT NULL
AND a.year = (SELECT MAX (b.year) FROM songs_staging as b WHERE b.artist_id = a.artist_id) 
ORDER BY a.artist_id DESC
""")

time_table_insert = ("""
INSERT INTO time 
(start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT
a.start_time, 
EXTRACT(HOUR from a.start_time),
EXTRACT(DAY from a.start_time),
EXTRACT(WEEK from a.start_time),
EXTRACT(MONTH from a.start_time),
EXTRACT(YEAR from a.start_time),
EXTRACT(WEEKDAY from a.start_time)
    FROM
        (SELECT DISTINCT 
            TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' as start_time 
                FROM events_staging) as a
""") 


# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
