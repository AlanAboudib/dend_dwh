import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

IAM_ROLE_ARN = config.get('IAM_ROLE', 'ARN')
LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays CASCADE;"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE;"
song_table_drop = "DROP TABLE IF EXISTS songs CASCADE;"
artist_table_drop = "DROP TABLE IF EXISTS artists CASCADE;"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events(
    artist VARCHAR(max),
    auth VARCHAR(max),
    firstName VARCHAR(max),
    gender CHAR(1),
    iteminSession INT,
    lastName VARCHAR(max),
    length NUMERIC,
    level VARCHAR(max),
    location VARCHAR(max),
    method VARCHAR(max),
    page VARCHAR(300),
    registration NUMERIC,
    sessionId VARCHAR(max),
    song VARCHAR(max),
    status INT,
    ts BIGINT,
    userAgent VARCHAR(max),
    userId VARCHAR(max)
);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs(
    num_songs INT4,
    artist_id VARCHAR(max),
    artist_latitude NUMERIC,
    artist_longitude NUMERIC,
    artist_location VARCHAR(max),
    artist_name VARCHAR(max),
    song_id VARCHAR(max),
    title VARCHAR(max),
    duration NUMERIC,
    year INT4
);
""")

songplay_table_create = ("""
CREATE TABLE songplays(
    songplay_id INT8 IDENTITY(0, 1) PRIMARY KEY,
    start_time BIGINT REFERENCES time(start_time),
    user_id VARCHAR(max) NOT NULL REFERENCES users(user_id),
    level VARCHAR(max),
    song_id VARCHAR(max) NOT NULL REFERENCES songs(song_id),
    artist_id VARCHAR(max) NOT NULL REFERENCES users(user_id),
    session_id VARCHAR(max),
    location VARCHAR(max),
    user_agent VARCHAR(max)
);
""")

user_table_create = ("""
CREATE TABLE users(
    user_id VARCHAR(max) PRIMARY KEY,
    first_name VARCHAR(max),
    last_name VARCHAR(max),
    gender CHAR(1),
    level VARCHAR(max)
);
""")

song_table_create = ("""
CREATE TABLE songs(
    song_id VARCHAR(max) PRIMARY KEY,
    title VARCHAR(max),
    artist_id VARCHAR(max),
    year INT4,
    duration NUMERIC
);
""")

artist_table_create = ("""
CREATE TABLE artists(
    artist_id VARCHAR(max) PRIMARY KEY,
    name VARCHAR(max),
    location VARCHAR(max),
    lattitude NUMERIC,
    longitude NUMERIC 
);
""")

time_table_create = ("""
CREATE TABLE time(
    start_time TIMESTAMP PRIMARY KEY,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT4,
    weekday NUMERIC
);
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from '{}'
credentials 'aws_iam_role={}'
json '{}' compupdate on region 'us-west-2';
""").format(LOG_DATA, IAM_ROLE_ARN, LOG_JSONPATH)

staging_songs_copy = ("""
copy staging_songs from '{}'
credentials 'aws_iam_role={}'
json 'auto' compupdate on region 'us-west-2';

""").format(SONG_DATA, IAM_ROLE_ARN)

# FINAL TABLES
    
songplay_table_insert = ("""
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT e.ts, e.userId, e.level, s.song_id, s.artist_id, e.sessionId, e.location, e.userAgent
FROM staging_songs s JOIN staging_events e
ON s.title = e.song
""")

user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level)
SELECT  DISTINCT userId, firstName, lastName, gender, level
FROM staging_events
""")

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, lattitude, longitude)
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday)

    SELECT DISTINCT ts, EXTRACT(HOUR FROM ts), EXTRACT(DAY FROM ts), EXTRACT(WEEK FROM ts), EXTRACT(MONTH FROM ts), EXTRACT(YEAR FROM ts), EXTRACT(WEEKDAY FROM ts)
    FROM( 
       SELECT (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 Second ') as ts
       FROM staging_events)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,  user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_songs_copy, staging_events_copy ]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
