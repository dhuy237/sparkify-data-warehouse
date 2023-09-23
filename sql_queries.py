import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

DWH_ROLE_ARN = config.get("IAM_ROLE", "ARN")

DWH_LOG_DATA = config.get("S3","LOG_DATA")
DWH_LOG_JSONPATH = config.get("S3","LOG_JSONPATH")
DWH_SONG_DATA = config.get("S3","SONG_DATA")


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song_table"
artist_table_drop = "DROP TABLE IF EXISTS artist_table"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist varchar(256),
    auth varchar(256),
    firstName varchar(256),
    gender varchar(1),
    itemInSession int,
    lastName varchar(256),
    length float,
    level varchar(256),
    location varchar(256),
    method varchar(256),
    page varchar(256),
    registration float,
    sessionId int,
    song varchar(256),
    status int,
    ts bigint,
    userAgent varchar(256),
    userId varchar(256)
)
""")
   
staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs int,
    artist_id varchar(256),
    artist_latitude float,
    artist_longitude float,
    artist_location varchar(256),
    artist_name varchar(256),
    song_id varchar(256),
    title varchar(256),
    duration float,
    year int
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id int identity(0,1) PRIMARY KEY,
    start_time timestamp NOT NULL SORTKEY,
    user_id varchar(256) NOT NULL,
    level varchar(256),
    song_id varchar(256),
    artist_id varchar(256),
    session_id int,
    location varchar(256),
    user_agent varchar(256)
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id varchar(256) NOT NULL PRIMARY KEY,
    first_name varchar(256),
    last_name varchar(256),
    gender varchar(1),
    level varchar(256)
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id varchar(256) NOT NULL PRIMARY KEY,
    title varchar(256),
    artist_id varchar(256),
    year int,
    duration float
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar(256) NOT NULL PRIMARY KEY,
    name varchar(256),
    location varchar(256),
    latitude float,
    longitude float
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time timestamp NOT NULL SORTKEY PRIMARY KEY,
    hour int,
    day int,
    week int,
    month int,
    year int,
    weekday int
)
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from {} 
iam_role {}
format as json {}
region 'us-west-2'
""").format(DWH_LOG_DATA, DWH_ROLE_ARN, DWH_LOG_JSONPATH)

staging_songs_copy = ("""
copy staging_songs from {}
iam_role {}
format as json 'auto'
region 'us-west-2'
""").format(DWH_SONG_DATA, DWH_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
)
SELECT 
    TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 second' AS start_time,
    e.userId AS user_id,
    e.level,
    s.song_id,
    s.artist_id,
    e.sessionId AS session_id,
    e.location,
    e.userAgent AS user_agent
FROM staging_events e
LEFT JOIN staging_songs s ON e.song = s.title AND e.artist = s.artist_name
WHERE e.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users (
    user_id,
    first_name,
    last_name,
    gender,
    level
)
SELECT DISTINCT
    s.userId AS user_id,
    s.firstName AS first_name,
    s.lastName AS last_name,
    s.gender,
    s.level
FROM staging_events s
""")

song_table_insert = ("""
INSERT INTO songs (
    song_id,
    title,
    artist_id,
    year,
    duration
)
SELECT DISTINCT
    s.song_id,
    s.title,
    s.artist_id,
    s.year,
    s.duration
FROM staging_songs s
""")

artist_table_insert = ("""
INSERT INTO artists (
    artist_id,
    name,
    location,
    latitude,
    longitude
)
SELECT DISTINCT
    s.artist_id,
    s.artist_name AS name,
    s.artist_location AS location,
    s.artist_latitude AS latitude,
    s.artist_longitude AS longitude
FROM staging_songs s
""")

time_table_insert = ("""
INSERT INTO time (
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday
)
WITH temp_time AS (SELECT TIMESTAMP 'epoch' + (ts/1000 * INTERVAL '1 second') AS ts FROM staging_events)
SELECT DISTINCT
    ts AS start_time,
    EXTRACT(hour from ts) AS hour,
    EXTRACT(day from ts) AS day,
    EXTRACT(week from ts) AS week,
    EXTRACT(month from ts) AS month,
    EXTRACT(year from ts) AS year,
    EXTRACT(weekday from ts) AS weekday
FROM temp_time
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
