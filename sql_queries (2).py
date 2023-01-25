import configparser
# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_drop"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song_table"
artist_table_drop = "DROP TABLE IF EXISTS artist_table"
time_table_drop = "DROP TABLE IF EXISTS time_table"
# CREATE TABLES
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events_table (
    artist VARCHAR(MAX),
    auth VARCHAR(MAX),
    FirstName VARCHAR(MAX),
    gender VARCHAR(MAX),
    itemInSession INT,
    lastName VARCHAR(MAX),
    length NUMERIC, 
    level VARCHAR(MAX),
    location VARCHAR(MAX),
    method VARCHAR(MAX),
    page VARCHAR(MAX),
    registration float8,
    sessionId INT,
    song VARCHAR(MAX),
    status INT,
    ts BIGINT,
    userAgent VARCHAR(MAX),
    userId INT);
""")
staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs_table (
    num_songs INT,
    artist_id VARCHAR(MAX),
    artist_latitude FLOAT,
    artist_longitude FLOAT, 
    artist_location VARCHAR(MAX),
    artist_name VARCHAR(MAX), 
    song_id VARCHAR(MAX), 
    title VARCHAR(MAX),
    duration FLOAT,
    year INT);
""")
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay_table (
    songplay_id INT IDENTITY PRIMARY KEY,
    start_time timestamp NOT NULL,
    user_id int NOT NULL,
    level varchar,
    song_id varchar,
    artist_id varchar,
    session_id varchar,
    location varchar,
    user_agent varchar);
""")
user_table_create = ("""
CREATE TABLE IF NOT EXISTS user_table (
    user_id int PRIMARY KEY, 
    FirstName varchar,                             
    lastName varchar,                            
    gender varchar, 
    level varchar);
""")
song_table_create = ("""
CREATE TABLE IF NOT EXISTS song_table (
    song_id varchar PRIMARY KEY,
    title varchar NOT NULL,
    artist_id varchar,
    year int,
    duration float NOT NULL); 
""")
artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist_table (
    artist_id varchar PRIMARY KEY,
    name varchar NOT NULL, 
    location varchar, 
    latitude float, 
    longitude float);
""")
time_table_create = ("""
CREATE TABLE IF NOT EXISTS time_table (
    start_time timestamp PRIMARY KEY, 
    hour int,
    day int,
    week int,
    month int,
    year int,
    weekday varchar);
""")
# STAGING TABLES
LOG_DATA  = config.get("S3", "LOG_DATA")
LOG_PATH  = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
IAM_ROLE  = config.get("IAM_ROLE","ARN")
staging_events_copy = ("""
copy staging_events_table from '{}'
credentials 'aws_iam_role={}'
json '{}' compupdate on region 'us-west-2';
""").format(LOG_DATA, IAM_ROLE, LOG_PATH)
staging_songs_copy = ("""
copy staging_songs_table from '{}'
credentials 'aws_iam_role={}'
json 'auto';
""").format(SONG_DATA, IAM_ROLE)


# FINAL TABLES
#https://stackoverflow.com/questions/39815425/how-to-convert-epoch-to-datetime-redshift
songplay_table_insert =  ("""
INSERT INTO songplay_table (start_time, user_id, level, song_id,
artist_id, session_id, location, user_agent)
SELECT DISTINCT
timestamp 'epoch' +  (se.ts / 1000) * INTERVAL '1 second' as start_time,
se.userID,
se.level,
ss.song_id,
ss.artist_id,
se.sessionID,
se.location,
se.userAgent
FROM staging_events_table se
INNER JOIN staging_songs_table ss ON se.song = ss.title 
AND se.artist = ss.artist_name
WHERE page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO user_table (user_id, first_name, last_name, gender, level)
SELECT DISTINCT
userID,
FirstName,
lastName,
gender,
level
FROM staging_events_table
WHERE userID IS NOT NULL
AND page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO song_table (song_id, title, artist_id, year, duration)
SELECT DISTINCT
song_id,
title, 
artist_id, 
year, 
duration
FROM staging_songs_table
WHERE song_id IS NOT NULL;
""")


artist_table_insert = ("""
INSERT INTO artist_table (artist_id, name, location, latitude, longitude)
SELECT DISTINCT
artist_id,
artist_name,        
artist_location,    
artist_latitude,    
artist_longitude,   
FROM staging_songs_table
WHERE artist_id IS NOT NULL;
""")

#https://livesql.oracle.com/apex/livesql/file/content_GCEY1DN2CN5HZCUQFHVUYQD3G.html
time_table_insert = ("""
INSERT into time_table (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT
timestamp 'epoch' +  (se.ts / 1000) * INTERVAL '1 second' as start_time,
EXTRACT(DAY FROM start_time)     as day,       
EXTRACT(WEEK FROM start_time)    as week,
EXTRACT(MONTH FROM start_time)   as month, 
EXTRACT(YEAR FROM start_time)    as year, 
to_char(start_time, 'DAY')       as weekday
FROM staging_events_table;
""")
# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]