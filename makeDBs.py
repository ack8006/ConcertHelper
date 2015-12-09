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
            stubhub_id VARCHAR(10),
            name text,
            venue_city text,
            venue_state VARCHAR(4),
            capacity int,
            bit_url text,
            latitude NUMERIC(10,7),
            longitude NUMERIC(10,7)
            )
            '''
    cur.execute(sql)

    sql = '''CREATE TABLE event_artists (
            bit_event_id VARCHAR(10),
            artist text,
            CONSTRAINT event_artist_pkey PRIMARY KEY (bit_event_id, artist)
            )'''
    cur.execute(sql)

    sql = '''CREATE TABLE events (
            bit_event_id VARCHAR(10) PRIMARY KEY,
            sk_event_id VARCHAR(10),
            bit_venue_id text,
            event_date timestamp,
            onsale_date timestamp,
            uploaded_date timestamp
            )
            '''
    cur.execute(sql)

    sql = '''CREATE TABLE artist_popularity (
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

    sql = '''CREATE TABLE stubhub_prices (
            stubhub_id VARCHAR(10),
            zone_id VARCHAR(10),
            zoneName text,
            min_ticket_price real,
            max_ticket_price real,
            min_ticket_quantity int,
            max_ticket_quantity int,
            total_tickets int,
            total_listings int,
            updated_time timestamp,
            CONSTRAINT stubhub_price_pkey PRIMARY KEY (stubhub_id, zone_id, updated_time)
            )'''
    cur.execute(sql)


conn.commit()
conn.close()




