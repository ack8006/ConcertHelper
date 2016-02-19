from dbconnection import start_db_connection
from contextlib import closing

conn = start_db_connection('local')

with closing(conn.cursor()) as cur:
    cur.execute('''SELECT * FROM artist''')
    artists = cur.fetchall()

    cur.execute('''SELECT * FROM event''')
    events = cur.fetchall()

    cur.execute('''SELECT * FROM event_artist''')
    ea = cur.fetchall()

    cur.execute('''SELECT * FROM popularity_point''')
    pp = cur.fetchall()

    cur.execute('''SELECT * FROM popularity_type''')
    pt = cur.fetchall()

    cur.execute('''SELECT * FROM popularity_value''')
    pv=cur.fetchall()

    cur.execute('''SELECT * FROM stubhub_listing''')
    sl =cur.fetchall()

    cur.execute('''SELECT * FROM stubhub_point''')
    sp=cur.fetchall()

    cur.execute('''SELECT * FROM stubhub_price''')
    stpr=cur.fetchall()

    cur.execute('''SELECT * FROM stubhub_zone''')
    sz=cur.fetchall()

    cur.execute('''SELECT * FROM venue''')
    v=cur.fetchall()
conn.close()

conn = start_db_connection('AWS')

with closing(conn.cursor()) as cur:
    for a in artists:
        cur.execute('''INSERT INTO artist (id, name, skid, mbidid, spotifyid,
                    echonestid, biturl) VALUES(%s,%s,%s,%s,%s,%s,%s)''', (
                    a[0],a[1],a[2],a[3],a[4],a[5],a[6]))
    conn.commit()

    for z in v:
        cur.execute('''INSERT INTO venue(id,bitid,skid,name,city,state,capacity,
                    latitude,longitude) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
                    (z[0],z[1],z[2],z[3],z[4],z[5],z[6],z[7],z[8]))

    for z in events:
        cur.execute('''INSERT INTO event (id, venue_id, bitid, event_date,
                    onsale_date) VALUES(%s,%s,%s,%s,%s)''', (z[0],z[1],z[2],z[3],
                    z[4]))
    conn.commit()

    for z in ea:
        cur.execute('''INSERT INTO event_artist(artist_id, event_id) VALUES
                    (%s,%s)''', (z[0],z[1]))
    conn.commit()

    for z in pp:
        cur.execute('''INSERT INTO popularity_point(id, artist_id, update_date)
                    VALUES (%s,%s,%s)''',(z[0],z[1],z[2]))
    conn.commit()

    for z in pt:
        cur.execute('''INSERT INTO popularity_type(id, name) VALUES (%s,%s)''',
                    (z[0],z[1]))
    conn.commit()

    for z in pv:
        cur.execute('''INSERT INTO popularity_value(id, pp_id, pt_id, value)
                    VALUES(%s,%s,%s,%s)''',(z[0],z[1],z[2],z[3]))
    conn.commit()

    for z in sl:
        cur.execute('''INSERT INTO stubhub_listing(id, stubhubid, event_id)
                    VALUES(%s,%s,%s)''', (z[0],z[1],z[2]))
    conn.commit()

    for z in sp:
        cur.execute('''INSERT INTO stubhub_point(id, sl_id, update_time)
                    VALUES(%s,%s,%s)''', (z[0],z[1],z[2]))
    conn.commit()

    for z in sz:
        cur.execute('''INSERT INTO stubhub_zone(id, name) VALUES(%s,%s)''',
                    (z[0],z[1]))
    conn.commit()

    for z in stpr:
        cur.execute('''INSERT INTO stubhub_price(id,sp_id,sz_id,min_price,max_price,
                    min_ticket_quantity,max_ticket_quantity,total_tickets,total_listings)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)''',(z[0],z[1],z[2],z[3],z[4],
                    z[5],z[6],z[7],z[8]))
    conn.commit()
conn.close
