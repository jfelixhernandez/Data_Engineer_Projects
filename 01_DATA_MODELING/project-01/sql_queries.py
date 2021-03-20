# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS SONGPLAYS"
user_table_drop = "DROP TABLE IF EXISTS  USERS"
song_table_drop = "DROP TABLE IF EXISTS SONGS"
artist_table_drop = "DROP TABLE IF EXISTS ARTISTS"
time_table_drop = "DROP TABLE IF EXISTS TIME"

# CREATE TABLES

songplay_table_create = ("CREATE TABLE SONGPLAYS(songplay_id SERIAL PRIMARY KEY, start_time TIMESTAMP NOT NULL, user_id INT NOT NULL, level VARCHAR(20) , song_id VARCHAR(25) , artist_id VARCHAR(25) , session_id INT NOT NULL, location VARCHAR(100), user_agent text)")

user_table_create = ("CREATE TABLE USERS(user_id INT PRIMARY KEY, first_name VARCHAR(50) NOT NULL, last_name VARCHAR(50) NOT NULL, gender VARCHAR(2), level VARCHAR(20) )")

song_table_create = ("CREATE TABLE SONGS(song_id VARCHAR(25) PRIMARY KEY, title VARCHAR(100) NOT NULL, artist_id VARCHAR(25) NOT NULL, year INT, duration FLOAT)")

artist_table_create = ("CREATE TABLE ARTISTS(artist_id VARCHAR(25) PRIMARY KEY, name VARCHAR(100) NOT NULL, location VARCHAR(80) , latitude FLOAT4 , longitude FLOAT4 ) ")

time_table_create = ("CREATE TABLE TIME(start_time TIMESTAMP PRIMARY KEY, hour INT NOT NULL, day INT NOT NULL, week INT NOT NULL, month INT NOT NULL, year INT NOT NULL, weekday INT NOT NULL)")

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO SONGPLAYS (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)""")

user_table_insert = ("""INSERT INTO USERS(user_id, first_name, last_name, gender, level) VALUES (%s,%s,%s,%s,%s) ON CONFLICT (user_id) DO UPDATE SET (level) = (EXCLUDED.level)""")



song_table_insert = ("""INSERT INTO SONGS(song_id , title , artist_id , year , duration) VALUES (%s,%s,%s,%s,%s) ON CONFLICT (song_id) DO NOTHING""")

artist_table_insert = ("""INSERT INTO ARTISTS(artist_id , name , location, latitude, longitude) VALUES (%s,%s,%s,%s,%s) ON CONFLICT (artist_id) DO NOTHING""")


time_table_insert = ("""INSERT INTO TIME(start_time, hour, day, week, month, year, weekday) VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (start_time) DO NOTHING """)

# FIND SONGS

song_select = ("""SELECT S.song_id, A.artist_id FROM SONGS S INNER JOIN ARTISTS A ON S.artist_id = A.artist_id WHERE S.title = %s OR A.name = %s OR S.duration = %s LIMIT 1 """)

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

# ANALITYCS QUERIES

#TOP 10 ARTISTS SONGS

top_10_songs = ("""SELECT T3.NAME AS ARTIST, T2.TITLE AS SONG, COUNT(T1.SONG_ID) AS TOTAL 
FROM SONGPLAYS T1 JOIN SONGS T2 ON T1.SONG_ID = T2.SONG_ID 
JOIN ARTISTS AS T3 ON T2.ARTIST_ID = T3.ARTIST_ID 
GROUP BY T3.NAME, T2.TITLE 
ORDER BY TOTAL DESC 
LIMIT 10 """)

total_plays_by_week = ("""SELECT  T2.YEAR, T2.MONTH, T2.WEEK, COUNT(T1.SONGPLAY_ID) AS TOTAL_PLAYS 
FROM SONGPLAYS T1 
JOIN TIME T2 ON T1.START_TIME = T2.START_TIME 
GROUP BY T2.YEAR, T2.MONTH, T2.WEEK 
ORDER BY T2.YEAR, T2.MONTH, T2.WEEK """)

total_users_by_level = ("""
SELECT T1.LEVEL, COUNT(T1.USER_ID) AS TOTAL
FROM USERS T1 
GROUP BY T1.LEVEL
""")
