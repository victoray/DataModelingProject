# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE songplays (
    songplay_id   serial PRIMARY KEY,
    start_time    date not null,
    user_id       int not null,
    level         text not null,
    song_id       text not null,
    artist_id     text not null,
    session_id    int not null,
    location      text not null,
    user_agent    text not null
)
""")

user_table_create = ("""
CREATE TABLE users (
    user_id    int PRIMARY KEY,
    first_name text not null,
    last_name  text not null,
    gender     text not null,
    level      text not null
)
""")

song_table_create = ("""
CREATE TABLE songs(
    song_id     text PRIMARY KEY,
    title       text not null,
    artist_id   text not null,
    year        text not null,
    duration    float not null
)
""")

artist_table_create = ("""
CREATE TABLE artists(
    artist_id   text PRIMARY KEY,
    name        text not null,
    location    text not null,
    latitude    text,
    longitude   text
)
""")

time_table_create = ("""
CREATE TABLE time(
    start_time   timestamp PRIMARY KEY,
    hour         text not null,
    day          text not null,
    week         text not null,
    month        text not null,
    year         text not null,
    weekday      text not null
)
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)

""")

user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING
""")

song_table_insert = ("INSERT INTO songs (song_id, title, artist_id, year, duration) \
VALUES (%s, %s, %s, %s, %s)")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
VALUES (%s, %s, %s, %s, %s)
""")


time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday)
VALUES (%s, %s, %s, %s, %s, %s, %s)
""")

# FIND SONGS

song_select = ("""
SELECT songs.song_id, artists.artist_id
FROM songs
JOIN artists ON songs.artist_id = artists.artist_id
WHERE songs.title = %s
AND artists.name = %s
AND songs.duration = %s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]