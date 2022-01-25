# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

# use SERIAl to create auto-sequence of integers as records hit table. songplay_id does not already exist, we are creating it.
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays 
(
songplay_id SERIAL PRIMARY KEY, 
start_time varchar NOT NULL,
user_id varchar NOT NULL, 
level varchar,
song_id varchar,
artist_id varchar, 
session_id varchar, 
location varchar,
user_agent varchar
) ;
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
user_id varchar NOT NULL, 
first_name varchar, 
last_name varchar, 
gender varchar,
level varchar,
PRIMARY KEY (user_id) 
) ;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs 
(
song_id varchar NOT NULL UNIQUE, 
title varchar, 
artist_id varchar, 
year int, 
duration numeric,
PRIMARY KEY (song_id) 
) ;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(
artist_id varchar NOT NULL UNIQUE,
name varchar, 
location varchar, 
latitude numeric, 
longitude numeric,
PRIMARY KEY (artist_id) 
) ;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
start_time varchar NOT NULL UNIQUE, 
hour varchar, 
day varchar, 
week varchar, 
month varchar,
year varchar, 
weekday varchar,
PRIMARY KEY (start_time) 
) ;
""")

# INSERT RECORDS
# songplay_id is auto increment by database, so do not need to insert anything for it
songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) \
VALUES (%s, %s, %s, %s, %s, %s, %s, %s); 
""")

# in case of multiple entries per User ID - for example, where a User swicthes level from 'free' 
# need to utilise a ON CONFLICT statement with DO UPDATE so that the customer info is updated with latest Level
# use the SET level =    statement to overwrite the 'level' value with any updates as records hit table 
user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level) \
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level ; 
""")

# in case of multtiple entries of the same song, again, just do nothing 
song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration) \
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (song_id) DO NOTHING ;
""")

# in case of multiple entries per Artist ID - for example, where a Artist moves location  
# need to utilise a ON CONFLICT statement with DO UPDATE so that the record is updated with latest info
# use the SET statement to overwrite  
artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude) \
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id) DO UPDATE SET location = EXCLUDED.location, latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude ;
""")


# in case of any conflicts in time - where two listeners are doing something at the exact same moment, just DO NOTHING 
time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday) \
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (start_time) DO NOTHING ;
""")

# FIND SONGS

song_select = ("""
SELECT 
    a.song_id, b.artist_id 
FROM 
    songs as a 
INNER JOIN 
    artists as b 
ON 
a.artist_id = b.artist_id
WHERE a.title = %s AND b.name = %s AND a.duration = %s 
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]