from dbconnection import start_db_connection
from contextlib import closing


#conn = start_db_connection('local')
conn = start_db_connection('AWS')

with closing(conn.cursor()) as cur:


    sql = '''CREATE TABLE artist (
            id SERIAL PRIMARY KEY,
            name VARCHAR(128) UNIQUE NOT NULL,
            skID VARCHAR(10),
            mbidID VARCHAR(40),
            spotifyID VARCHAR(36),
            echonestID VARCHAR(24),
            bitURL text
            )'''
    cur.execute(sql)

    sql = '''CREATE TABLE popularity_point (
            id SERIAL PRIMARY KEY,
            artist_id integer NOT NULL REFERENCES artist(id) ON DELETE CASCADE,
            update_date date NOT NULL,
            UNIQUE(artist_id, update_date)
            )'''
    cur.execute(sql)

    sql = '''CREATE TABLE popularity_type (
            id SERIAL PRIMARY KEY,
            name VARCHAR(32) UNIQUE NOT NULL
            )'''
    cur.execute(sql)

    sql = '''CREATE TABLE popularity_value (
            id SERIAL PRIMARY KEY,
            pp_id integer NOT NULL REFERENCES popularity_point(id) ON DELETE CASCADE,
            pt_id integer NOT NULL REFERENCES popularity_type(id) ON DELETE CASCADE,
            value numeric NOT NULL,
            UNIQUE(pp_id, pt_id)
            )'''
    cur.execute(sql)

    sql = '''CREATE TABLE venue (
            id SERIAL PRIMARY KEY,
            bitID VARCHAR(16) NOT NULL UNIQUE,
            skID VARCHAR(16),
            name VARCHAR(128),
            city VARCHAR(64),
            state VARCHAR(4),
            capacity int,
            latitude numeric(10,7),
            longitude numeric(10,7)
            )'''
    cur.execute(sql)

    sql = '''CREATE TABLE event (
            id SERIAL PRIMARY KEY,
            venue_id integer NOT NULL REFERENCES venue(id) ON DELETE CASCADE,
            bitID VARCHAR(16) NOT NULL UNIQUE,
            event_date timestamp without time zone,
            onsale_date timestamp without time zone
            )'''
    cur.execute(sql)

    sql = '''CREATE TABLE event_artist (
            artist_id integer NOT NULL REFERENCES artist(id) ON DELETE CASCADE,
            event_id integer NOT NULL REFERENCES event(id) ON DELETE CASCADE,
            PRIMARY KEY (artist_id, event_id)
            )'''
    cur.execute(sql)

    sql = '''CREATE TABLE stubhub_listing (
            id SERIAL PRIMARY KEY,
            stubhubid VARCHAR(16) NOT NULL,
            event_id integer NOT NULL REFERENCES event(id) ON DELETE CASCADE,
            UNIQUE(stubhubid, event_id)
            )'''
    cur.execute(sql)

    sql = '''CREATE TABLE stubhub_point (
            id SERIAL PRIMARY KEY,
            sl_id integer NOT NULL REFERENCES stubhub_listing(id) ON DELETE CASCADE,
            update_time timestamp without time zone NOT NULL,
            UNIQUE(sl_id, update_time)
            )'''
    cur.execute(sql)

    sql = '''CREATE TABLE stubhub_zone (
            id integer PRIMARY KEY,
            name VARCHAR(64) NOT NULL
            )'''
    cur.execute(sql)

    sql = '''CREATE TABLE stubhub_price (
            id SERIAL PRIMARY KEY,
            sp_id integer NOT NULL REFERENCES stubhub_point(id) ON DELETE CASCADE,
            sz_id integer NOT NULL REFERENCES stubhub_zone(id) ON DELETE CASCADE,
            min_price real,
            max_price real,
            min_ticket_quantity integer,
            max_ticket_quantity integer,
            total_tickets integer,
            total_listings integer
            )'''
    cur.execute(sql)

    sql = '''CREATE TABLE event_price (
            id SERIAL PRIMARY KEY,
            event_id integer NOT NULL REFERENCES event(id) ON DELETE CASCADE,
            section_name VARCHAR(64),
            min_price real,
            max_price real
            )'''



conn.commit()
conn.close()




