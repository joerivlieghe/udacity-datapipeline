class SqlQueries:
        
    staging_events_table_create = ("""
    CREATE TABLE staging_events(
        event_id INT IDENTITY(0,1),
        artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender VARCHAR,
        itemInSession VARCHAR,
        lastName VARCHAR,
        length VARCHAR,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration VARCHAR,
        sessionId BIGINT SORTKEY DISTKEY,
        song VARCHAR,
        status VARCHAR,
        ts BIGINT,
        userAgent VARCHAR,
        userId INT)
    """)

    staging_songs_table_create = ("""
    CREATE TABLE staging_songs(
        num_songs INT,
        artist_id VARCHAR SORTKEY DISTKEY,
        artist_latitude DECIMAL,
        artist_longitude DECIMAL,
        artist_location VARCHAR(500),
        artist_name VARCHAR(500),
        song_id VARCHAR,
        title VARCHAR(500),
        duration NUMERIC,
        year NUMERIC)
    """)

    songplay_table_create = ("""
    CREATE TABLE songplay(
        songplay_id INT IDENTITY(1,1) PRIMARY KEY,
        start_time TIMESTAMP SORTKEY,
        user_id VARCHAR DISTKEY,
        level VARCHAR,
        song_id VARCHAR,
        artist_id VARCHAR,
        session_ID INT,
        location VARCHAR,
        user_agent VARCHAR)
    """)

    user_table_create = ("""
    CREATE TABLE user_table(
        user_id VARCHAR PRIMARY KEY SORTKEY,
        first_name VARCHAR,
        last_name VARCHAR,
        gender VARCHAR,
        level VARCHAR)
    """)

    song_table_create = ("""
    CREATE TABLE song(
        song_id VARCHAR PRIMARY KEY SORTKEY,
        title VARCHAR,
        artist_id VARCHAR,
        year INT,
        duration DECIMAL(9))
    """)

    artist_table_create = ("""
    CREATE TABLE artist(
        artist_id VARCHAR PRIMARY KEY SORTKEY,
        name VARCHAR,
        location DECIMAL,
        latitude DECIMAL,
        longitude VARCHAR)
    """)

    time_table_create = ("""
    CREATE TABLE time(
        start_time TIMESTAMP PRIMARY KEY SORTKEY DISTKEY,
        hour INT,
        day INT,
        week INT,
        month INT,
        year INT,
        weekday VARCHAR)
    """)

    # STAGING TABLES

    # NOT NEEDED FOR THIS PROJECT - WAS ONLY NEEDED FOR PROJECT 2 AND IS RUN IN THE OPERATOR

    # staging_events_copy = ("""
    #                     COPY staging_events
    #                     FROM {}
    #                     IAM_ROLE {}
    #                     JSON {};
    #                     """).format(config.get('S3','LOG_DATA'),config.get('IAM_ROLE','ARN'),config.get('S3','LOG_JSONPATH'))

    # staging_songs_copy = ("""
    #                     COPY staging_songs
    #                     FROM {}
    #                     IAM_ROLE {}
    #                     JSON 'auto';
    #                     """).format(config.get('S3','SONG_DATA'),config.get('IAM_ROLE','ARN'))

    # FINAL TABLES

    songplay_table_insert = ("""
        INSERT INTO songplay(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        SELECT DISTINCT
            TIMESTAMP 'epoch' + (e.ts / 1000) * INTERVAL '1 second' as start_time,
            e.userId,
            e.level,
            s.song_id,
            s.artist_id,
            e.sessionId,
            e.location,
            e.userAgent
        FROM staging_songs s
        INNER JOIN staging_events e
        ON (s.title = e.song AND s.artist_name = e.artist)
        WHERE e.page = 'NextSong';
            
    """)

    user_table_insert = ("""
        INSERT INTO user_table (user_id, first_name, last_name, gender, level)
        SELECT DISTINCT
            e.userId,
            e.firstName,
            e.lastName,
            e.gender,
            e.level
        FROM staging_events e
        WHERE userId IS NOT NULL
        AND e.page = 'NextSong';
    """)

    song_table_insert = ("""
        INSERT INTO song (song_id, title, artist_id, year, duration)
        SELECT DISTINCT
            s.song_id,
            s.title,
            s.artist_id,
            s.year,
            s.duration
        FROM staging_songs s
        WHERE song_id IS NOT NULL;
    """)

    artist_table_insert = ("""
        INSERT INTO artist (artist_id, name, location, latitude, longitude)
        SELECT DISTINCT
            s.artist_id,
            s.artist_name,
            s.artist_location,
            s.artist_latitude,
            s.artist_longitude
        FROM staging_songs s
        WHERE song_id IS NOT NULL;
    """)

    time_table_insert = ("""
        INSERT INTO time (start_time, hour, day, week, month, year, weekday)
        SELECT DISTINCT
            start_time,
            EXTRACT(hour FROM start_time) AS hour,
            EXTRACT(day FROM start_time) AS day,
            EXTRACT(week FROM start_time) AS week,
            EXTRACT(month FROM start_time) AS month,
            EXTRACT(year FROM start_time) AS year,
            to_char(start_time, 'DAY') AS weekday
        FROM songplay
    """)


    quality_test_songs = ("""
        SELECT COUNT(*) FROM song
        WHERE artist_id is NULL
    """)

    quality_test_artist = ("""
        SELECT COUNT (*) FROM artist
        WHERE location is NULL
    """)

    quality_test_time = ("""
        SELECT COUNT (*) FROM time
        WHERE year is NULL
    """)

    quality_test_user = ("""
        SELECT COUNT (*) FROM user_table
        WHERE user_id is NULL
    """)

    quality_test_songplay = ("""
        SELECT COUNT (*) FROM songplay
        WHERE songplay_id is NULL
    """)

    quality_tests = [quality_test_songs, quality_test_artist, quality_test_time, quality_test_user, quality_test_songplay]
    quality_results = [0,0,0,0,0]