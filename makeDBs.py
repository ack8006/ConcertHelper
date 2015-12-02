from dbconnection import start_db_connection
from contextlib import closing


conn = start_db_connection('local')

with closing(conn.cursor()) as cur:
    sql = '''CREATE TABLE artists (
            name text PRIMARY KEY,
            sk_artist_id VARCHAR(10),
            mbid VARCHAR(40),
            bit_url text,
            spotify_id text,
            spotify_popularity int
            )
            '''
    cur.execute(sql)

    sql = '''CREATE TABLE venues (
            bit_venue_id VARCHAR(10) PRIMARY KEY,
            sk_venue_id VARCHAR(10),
            name text,
            venue_city text,
            venue_state VARCHAR(4),
            capacity int,
            bit_url text
            )
            '''
    cur.execute(sql)

    sql = '''CREATE TABLE events (
            bit_event_id VARCHAR(10) PRIMARY KEY,
            sk_event_id VARCHAR(10),
            artists text[],
            bit_venue_id text,
            event_date timestamp,
            onsale_date timestamp
            )
            '''
    cur.execute(sql)

    sql = '''CREATE TABLE artist_popularity (
            (
            name text NOT NULL,
            spotify_id text,
            echo_nest_id character varying(24),
            spotify_popularity integer,
            echo_nest_hotttnesss numeric,
            spotify_followers integer,
            update_date date NOT NULL,
            CONSTRAINT artist_popularity_pkey PRIMARY KEY (name, update_date)
            )'''
    cur.execute(sql)



conn.commit()
conn.close()




