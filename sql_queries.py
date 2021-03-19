import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
                                CREATE TABLE IF NOT EXISTS staging_events
                                (
                                    artist varchar,
                                    auth varchar,
                                    firstName varchar,
                                    gender varchar,
                                    itemInSession int,
                                    lastName varchar,
                                    length numeric,
                                    level varchar,
                                    location varchar,
                                    method varchar,
                                    page varchar,
                                    registration varchar,
                                    sessionId int,
                                    song varchar,
                                    status int,
                                    ts varchar,
                                    userAgent varchar,
                                    userid int
                                  
                                )
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs
                                (
                                    artist_id varchar,
                                    artist_latitude numeric,
                                    artist_location varchar,
                                    artist_longitude numeric,
                                    artist_name varchar,
                                    duration numeric,
                                    num_songs int,
                                    song_id varchar,
                                    title varchar,
                                    year varchar                                   
                                )
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays 
                            (  
                               songplay_id bigint identity(0,1) primary key, 
                               start_time timestamp NOT NULL,
                               user_id int NOT NULL, 
                               level varchar, 
                               song_id varchar NOT NULL,
                               artist_id varchar NOT NULL,
                               session_id int,
                               location varchar,
                               user_agent varchar,
                               UNIQUE(start_time,user_id),
                               CONSTRAINT fk_songplays_start_time
                               FOREIGN KEY(start_time) REFERENCES time(start_time),
                               CONSTRAINT fk_songplays_user_id
                               FOREIGN KEY(user_id) REFERENCES users(user_id),
                               CONSTRAINT fk_songplays_artist_id
                               FOREIGN KEY(artist_id) REFERENCES artists(artist_id),
                               CONSTRAINT fk_songplays_song_id
                               FOREIGN KEY(song_id) REFERENCES songs(song_id)
                            )
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users 
                        (  
                           user_id int PRIMARY KEY, 
                           first_name varchar, 
                           last_name varchar,
                           gender varchar,
                           level varchar
                        )
""")

song_table_create = ("""
                       CREATE TABLE IF NOT EXISTS songs 
                        (  
                           song_id varchar PRIMARY KEY, 
                           title varchar, 
                           artist_id varchar NOT NULL,
                           year int,
                           duration numeric,
                           CONSTRAINT fk_songs_artist_id
                           FOREIGN KEY(artist_id) REFERENCES artists(artist_id)
                        )
""")

artist_table_create = ("""
                    CREATE TABLE IF NOT EXISTS artists  
                        (  
                           artist_id varchar PRIMARY KEY, 
                           name varchar, 
                           location varchar,
                           latitude numeric,
                           longitude numeric
                        )
""")

time_table_create = ("""
                       CREATE TABLE IF NOT EXISTS time  
                        (  
                           start_time timestamp PRIMARY KEY, 
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
    copy staging_events from 's3://udacity-dend/log_data'
    credentials 'aws_iam_role={}'
    json 's3://udacity-dend/log_json_path.json'
    compupdate on region 'us-west-2';
""").format(config.get('IAM_ROLE','ARN'))

staging_songs_copy = ("""
copy staging_songs from 's3://udacity-dend/song_data'
    credentials 'aws_iam_role={}'
    json 'auto'
    compupdate on region 'us-west-2';
""").format(config.get('IAM_ROLE','ARN'))

# FINAL TABLES

songplay_table_insert = ("""
        INSERT INTO songplays
        (
           start_time ,
           user_id , 
           level , 
           song_id,
           artist_id,
           session_id,
           location,
           user_agent
        )
        SELECT cast(TIMESTAMP 'epoch'+ts/1000 *INTERVAL '1 second' as timestamp) as start_time,
               userid,
               level,
               s.song_id,
               a.artist_id,
                sessionid,
                se.location,
                se.useragent
        FROM staging_events se 
        inner join songs s on se.song = s.title 
        inner join artists a on a.name = se.artist 
        WHERE not exists
        (
            SELECT 1
            FROM songplays sp
            WHERE cast(TIMESTAMP 'epoch'+se.ts/1000 *INTERVAL '1 second' as timestamp) = sp.start_time
            and se.userid = sp.user_id
        )

""")

user_table_insert = ("""
    DELETE FROM users
    using staging_events
    WHERE users.user_id = staging_events.userid;


    INSERT INTO users
    SELECT  DISTINCT
            userid,
            firstname,
            lastname,
            gender,
            level
    FROM staging_events 
    WHERE userid is not null

""")

song_table_insert = ("""
    DELETE FROM songs
    USING staging_songs
    WHERE songs.song_id = staging_songs.song_id;

    INSERT INTO songs
    SELECT DISTINCT 
            song_id,
            title,
            artist_id,
            cast(year as int),
            duration
    FROM staging_songs 
""")

artist_table_insert = ("""
    DELETE FROM artists 
USING staging_songs 
WHERE artists.artist_id = staging_songs.artist_id;


INSERT into artists
SELECT DISTINCT artist_id,
       artist_name,
       artist_location,
       artist_latitude,
       artist_longitude
FROM staging_songs

""")

time_table_insert = ("""
    
        delete from time
        using 
        (
        select cast(TIMESTAMP 'epoch'+ts/1000 *INTERVAL '1 second' as timestamp) as ts
        from staging_events 
        ) as cDelete
        where time.start_time = cDelete.ts;


        insert into time
        with cinsert as
        (
        select cast(TIMESTAMP 'epoch'+ts/1000 *INTERVAL '1 second' as timestamp) as ts
        from staging_events 
        )
        select distinct ts as start_time,
               extract(hour from ts) as hour,
               extract(day from ts) as day,
               extract(week from ts) as week,
               extract(month from ts) as month,
               extract(year from ts) as year,
               extract(weekday from ts) as weekday
        from cinsert
        
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create,song_table_create,  time_table_create,songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [artist_table_insert, song_table_insert,time_table_insert, user_table_insert,songplay_table_insert]
