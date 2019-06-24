import configparser
import psycopg2

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

# Drop Tables
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"


# CREATE TABLES --------------------------------------------------------------
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist TEXT,
        auth TEXT,
        firstName TEXT,
        gender TEXT,
        itemInSession INT,
        lastName TEXT,
        length FLOAT,
        level TEXT,
        location TEXT,
        method TEXT,
        page TEXT,
        registration FLOAT,
        sessionId INT,
        song TEXT,
        status INT,
        ts BIGINT,
        userAgent TEXT,
        userId INT
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs INT,
        artist_id TEXT,
        artist_latitude FLOAT,
        artist_longitude FLOAT,
        artist_location TEXT,
        artist_name TEXT,
        song_id TEXT,
        title TEXT,
        duration FLOAT,
        year INT
    )
""")

# Fact table
songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INT IDENTITY(0,1),
        timestamp BIGINT SORTKEY,
        song_id TEXT NOT NULL DISTKEY,
        artist_id TEXT NOT NULL,
        user_id TEXT NOT NULL,
        session_id INT NOT NULL,
        level TEXT NOT NULL,
        location TEXT,
        user_agent TEXT,
        
        PRIMARY KEY (songplay_id)
    )
""")

# User dimension
user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id TEXT NOT NULL,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        level TEXT,
        
        PRIMARY KEY (user_id)
    )
""")

# Song dimension
song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id TEXT NOT NULL DISTKEY,
        title TEXT,
        artist_id TEXT,
        year INT,
        duration FLOAT,
        
        PRIMARY KEY (song_id)
    )
""")

# Artist Dimension
artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id TEXT NOT NULL,
        artist_name TEXT,
        location TEXT,
        latitude FLOAT,
        longitude FLOAT,
        
        PRIMARY KEY (artist_id)
    )
    DISTSTYLE ALL
""")

# Time dimension
time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        timestamp BIGINT NOT NULL,
        hour INT NOT NULL,
        day INT NOT NULL,
        week_of_year INT NOT NULL,
        month INT NOT NULL,
        year INT NOT NULL,
        weekday INT NOT NULL,
        
        PRIMARY KEY (timestamp)
    )
""")

# STAGING TABLES
# https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html
# https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-format.html#copy-format
staging_events_copy = ("""
    COPY staging_events
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    FORMAT json AS {}
""").format(
    config.get("S3", "LOG_DATA"), 
    config.get("IAM_ROLE", "ARN"), 
    config.get("S3", "LOG_JSONPATH")
)

staging_songs_copy = ("""
    COPY staging_songs
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    JSON 'auto'
""").format(
    config.get("S3", "SONG_DATA"), 
    config.get("IAM_ROLE", "ARN"), 
)


# FINAL TABLES INSERT --------------------------------------------------------
songplay_table_insert = ("""
    INSERT INTO songplays (timestamp, song_id, artist_id, user_id, session_id, level, location, user_agent)
    SELECT DISTINCT 
        e.ts AS timestamp, 
        s.song_id,
        s.artist_id,
        e.userId AS user_id,
        e.sessionId AS session_id,
        e.level,
        e.location,
        e.userAgent AS user_agent
    FROM staging_events e
    INNER JOIN staging_songs s
    ON e.song = s.title AND
       e.artist = s.artist_name
    WHERE e.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT
        DISTINCT
        userId AS user_id,
        firstName AS first_name,
        lastName AS last_name,
        gender,
        level
    FROM staging_events
    WHERE user_id IS NOT NULL
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT
        DISTINCT
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, artist_name, location, latitude, longitude)
    SELECT
        DISTINCT
        artist_id,
        artist_name,
        artist_location,
        artist_latitude AS latitude,
        artist_longitude AS longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL
""")

# https://stackoverflow.com/questions/39815425/how-to-convert-epoch-to-datetime-redshift
time_table_insert = ("""
    INSERT INTO time (timestamp, hour, day, week_of_year, month, year,weekday)
    SELECT
        timestamp 'epoch' + your_timestamp_column * interval '1 second',
        EXTRACT(hr from start_time) AS hour,
        EXTRACT(d from start_time) AS day,
        EXTRACT(w from start_time) AS week,
        EXTRACT(mon from start_time) AS month,
        EXTRACT(yr from start_time) AS year, 
        EXTRACT(weekday from start_time) AS weekday 
    FROM staging_events
    
""")


# DELETE COPY TABLES ---------------------------------------------------------

staging_events_drop = ("""
DROP TABLE IF EXISTS staging_events
""")

staging_songs_table_drop = ("""
DROP TABLE IF EXISTS staging_songs
""")

# QUERY LISTS ----------------------------------------------------------------
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create]
copy_table_queries = [staging_events_copy, staging_songs_copy]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert]
delete_copy_tables = [staging_events_drop, staging_songs_table_drop]
