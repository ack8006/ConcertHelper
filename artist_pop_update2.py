from contextlib import closing
from collections import defaultdict
from time import sleep
import requests
#import grequests
import datetime
import psycopg2

import spotipy

from api_keys import spotify_client_id, spotify_client_secret
from dbconnection import start_db_connection

#Notes
    # Okay not updating if spotify ids don't match

#list of tuples
#(name, spotify, echonest)
def get_artists_to_update():
    conn = start_db_connection()
    with closing(conn.cursor()) as cur:
        #SELECTS all artists that have either never been update or haven't been
        #updated in greater than 6 days

        cur.execute('''SELECT id, name, spotifyid
                    FROM
                    (SELECT DISTINCT ON (a.id) a.id, a.name, a.spotifyid,
                    MAX(pp.update_date)
                    FROM artist a LEFT JOIN popularity_point pp
                    on a.id=pp.artist_id
                    WHERE (a.spotifyid IS NOT NULL AND a.spotifyid <> 'no_match')
                    GROUP BY a.id, a.name, a.spotifyid) as foo
                    WHERE (MAX IS NULL OR max < now()-'7 days'::interval);''')

        data = cur.fetchall()
    conn.close()
    return data

def parse_spotify2(data):
    if not data:
        return None, None
    sp_id = data['id']
    followers = data['followers']['total']
    pop = data['popularity']
    return sp_id, pop, followers

#Assuming artists returned in order
def get_batch_artist_data(sp, artists):
    agg_pop_data = []

    id_lookup_dict = defaultdict(list)
    for x in artists:
        id_lookup_dict[x[2]].append((x[0], x[1]))

    spotify_ids = [x[2] for x in artists]
    artist_data = sp.artists(spotify_ids)
    if len(artist_data['artists']) != len(artists):
        print 'LEN MISMATCH'
        print len(artist_data['artists']), len(artists)

    for ind, artist in enumerate(artist_data['artists']):
        sp_id, pop, follow = parse_spotify2(artist)
        for lookup_ids in id_lookup_dict[sp_id]:
            pop_data = {}
            pop_data['spotify_popularity'], pop_data['spotify_followers'] = pop,follow
            pop_data['id'], pop_data['name'] = lookup_ids
            pop_data['spotify_id'] = sp_id
            pop_data['echonest_hotttnesss'] = None
            agg_pop_data.append(pop_data)
            print pop_data['id'], pop_data['name']
    return agg_pop_data

def upload_popularity_data(popularity_data):
    conn = start_db_connection()
    with closing(conn.cursor()) as cur:
        cur.execute('''SELECT name FROM popularity_type''')
        popularity_types = [x[0] for x in cur.fetchall()]
        current_date = datetime.datetime.now().date()
        for pd in popularity_data:
            try:
                cur.execute('''INSERT INTO popularity_point (artist_id, update_date)
                                VALUES (%s, %s)''', (pd['id'], current_date))

                for pop_type in popularity_types:
                    if not pd[pop_type]:
                        continue
                    cur.execute('''INSERT INTO popularity_value (pp_id,
                                pt_id, value) SELECT pp.id, pt.id, %s
                                FROM popularity_point pp, popularity_type pt WHERE
                                pp.update_date = %s AND pp.artist_id = %s
                                AND pt.name = %s''',
                                (pd[pop_type], current_date, pd['id'], pop_type))
                conn.commit()
            except psycopg2.IntegrityError as e:
                print 'Attempted Multiple Upload', pd
                conn.rolback()
    conn.close()

def run():
    print 'Artist Popularity Data'
    artists = get_artists_to_update()
    print len(artists)
    
    sp = spotipy.Spotify()
    for x in xrange(0, len(artists), 50):
        pop_data = get_batch_artist_data(sp, artists[x:x+50])
        upload_popularity_data(pop_data)
        sleep(3)

if __name__ == '__main__':
    run()
