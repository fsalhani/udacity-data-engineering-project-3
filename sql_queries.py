import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events (
  artist         VARCHAR,
  auth           VARCHAR,
  firstName      VARCHAR,
  gender         VARCHAR,
  itemInSession  INTEGER,
  lastName       VARCHAR,
  length         DOUBLE PRECISION,
  level          VARCHAR,
  location       VARCHAR,
  method         VARCHAR,
  page           VARCHAR,
  registration   DOUBLE PRECISION,
  sessionId      INTEGER,
  song           VARCHAR,
  status         INTEGER,
  ts             BIGINT,
  userAgent      VARCHAR,
  userId         INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
  song_id             VARCHAR,
  num_songs           INTEGER,
  title               VARCHAR(255),
  artist_name         VARCHAR(255),
  artist_latitude     DOUBLE PRECISION,
  year                INTEGER,
  duration            DOUBLE PRECISION,
  artist_id           VARCHAR,
  artist_longitude    DOUBLE PRECISION,
  artist_location     VARCHAR(255)
);
""")

songplay_table_create = ("""
CREATE TABLE songplays (
  start_time     TIMESTAMP NOT NULL,
  user_id        INTEGER NOT NULL,
  level          VARCHAR,
  song_id        VARCHAR,
  artist_id      VARCHAR,
  session_id     INTEGER,
  location       VARCHAR,
  user_agent     VARCHAR,
  PRIMARY KEY(start_time),
  FOREIGN KEY(artist_id) REFERENCES artists(artist_id),
  FOREIGN KEY(song_id) REFERENCES songs(song_id),
  FOREIGN KEY(user_id) REFERENCES users(user_id),
  FOREIGN KEY(start_time) REFERENCES time(start_time)
)
DISTSTYLE EVEN
COMPOUND SORTKEY (start_time, user_id);
""")

user_table_create = ("""
CREATE TABLE users (
  user_id        INTEGER NOT NULL,
  first_name     VARCHAR NOT NULL,
  last_name      VARCHAR NOT NULL,
  gender         VARCHAR,
  level          VARCHAR NOT NULL,
  PRIMARY KEY(user_id)
)
DISTKEY (user_id)
SORTKEY (user_id);
""")

song_table_create = ("""
CREATE TABLE songs (
  song_id        VARCHAR NOT NULL,
  title          VARCHAR NOT NULL,
  artist_id      VARCHAR NOT NULL,
  year           INTEGER,
  duration       DOUBLE PRECISION,
  PRIMARY KEY(song_id),
  FOREIGN KEY(artist_id) REFERENCES artists(artist_id)
)
DISTKEY (song_id)
SORTKEY (song_id);
""")

artist_table_create = ("""
CREATE TABLE artists (
  artist_id      VARCHAR NOT NULL,
  name           VARCHAR NOT NULL,
  location       VARCHAR,
  latitude       DOUBLE PRECISION,
  longitude      DOUBLE PRECISION,
  PRIMARY KEY(artist_id)
)
DISTKEY (artist_id)
SORTKEY (artist_id);
""")

time_table_create = ("""
CREATE TABLE time (
  start_time     TIMESTAMP NOT NULL,
  hour           INTEGER NOT NULL,
  day            INTEGER NOT NULL,
  week           INTEGER NOT NULL,
  month          INTEGER NOT NULL,
  year           INTEGER NOT NULL,
  weekday        INTEGER NOT NULL,
  PRIMARY KEY(start_time)
)
DISTKEY (start_time)
SORTKEY (start_time);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events
FROM {0}
IAM_ROLE {1}
REGION 'us-west-2'
FORMAT AS JSON {2};
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_songs
FROM {0}
IAM_ROLE {1}
REGION 'us-west-2'
FORMAT AS JSON 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (
  SELECT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' AS start_time,
         se.userid AS user_id,
         se.level,
         ss.song_id,
         ss.artist_id,
         se.sessionid AS session_id,
         se.location,
         se.useragent AS user_agent
  FROM staging_events AS se
    LEFT JOIN staging_songs AS ss ON ss.title = se.song
  WHERE page = 'NextSong'
);
""")

user_table_insert = ("""
INSERT INTO users (
  SELECT DISTINCT
         se.userId AS user_id,
         se.firstName AS first_name,
         se.lastName AS last_name,
         se.gender AS gender,
         LAST_VALUE(se.level) OVER
           (PARTITION BY se.userId ORDER BY se.ts
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
         ) AS level
  FROM staging_events AS se
    LEFT JOIN users ON users.user_id = se.userId
  WHERE se.page = 'NextSong'
    AND users.user_id IS NULL
);
""")

song_table_insert = ("""
INSERT INTO songs (
  SELECT DISTINCT
         ss.song_id,
         ss.title,
         ss.artist_id,
         ss.year,
         ss.duration
  FROM staging_songs AS ss
    LEFT JOIN songs ON songs.song_id = ss.song_id
  WHERE songs.song_id IS NULL
);
""")

artist_table_insert = ("""
INSERT INTO artists (
  SELECT DISTINCT
         ss.artist_id,
         ss.artist_name AS name,
         ss.artist_location AS location,
         ss.artist_latitude AS latitude,
         ss.artist_longitude AS longitude
  FROM staging_songs AS ss
    LEFT JOIN artists ON artists.artist_id = ss.artist_id
  WHERE artists.artist_id IS NULL
);
""")


time_table_insert = ("""
INSERT INTO time (
  SELECT DISTINCT
         TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' AS start_time,
         EXTRACT(HOUR FROM TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second') AS hour,
         EXTRACT(DAY FROM TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second') AS day,
         EXTRACT(WEEK FROM TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second') AS week,
         EXTRACT(MONTH FROM TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second') AS month,
         EXTRACT(YEAR FROM TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second') AS year,
         EXTRACT(DOW FROM TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second') AS weekday
  FROM staging_events AS se
    LEFT JOIN time ON time.start_time = TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second'
  WHERE se.page = 'NextSong'
    AND time.start_time IS NULL
);
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
