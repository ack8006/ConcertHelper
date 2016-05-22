from api_keys import echo_nest_api
from dbconnection import start_db_connection
from contextlib import closing
from time import sleep
import requests
#import grequests
import datetime
import psycopg2


#list of tuples
#(name, spotify, echonest)
def get_artists_to_update():
    conn = start_db_connection()
    with closing(conn.cursor()) as cur:
        #SELECTS all artists that have either never been update or haven't been
        #updated in greater than 6 days

        cur.execute('''SELECT id, name, spotifyid, echonestid FROM
                    (SELECT DISTINCT ON (a.id) a.id, a.name, a.spotifyid,
                    a.echonestid, MAX(pp.update_date)
                    FROM artist a LEFT JOIN popularity_point pp
                    on a.id=pp.artist_id
                    WHERE (a.spotifyid IS NOT NULL AND a.spotifyid <> 'n/a') OR
                    (a.echonestid IS NOT NULL AND a.echonestid <> 'n/a')
                    GROUP BY a.id, a.name, a.spotifyid, a.echonestid) as foo
                    WHERE (MAX IS NULL OR max < now()-'7 days'::interval);''')

        data = cur.fetchall()
    conn.close()
    return data

#takes data tuple
def build_urls(ar_inf):
    urls = {}
    if ar_inf[3] != 'n/a':
        urls['echonest'] = ('http://developer.echonest.com/api/v4/artist/hotttnesss?api_key'
                '=%s&id=%s&format=json' %(echo_nest_api, ar_inf[3]))
    else:
        urls['echonest'] = None
    if ar_inf[2] != 'n/a':
        urls['spotify'] = 'https://api.spotify.com/v1/artists/%s' %(ar_inf[2])
    else:
        urls['spotify'] = None
    return urls

def request_data(url, attempt=1):
    try:
        r = requests.get(url)
    except requests.exceptions.ConnectionError as e:
        print url
        print e
        #print e.args[0].reason

    if r.status_code == 200:
        return r.json()
    elif attempt > 3:
        return
    else:
        print 'Status Code: %s' %(r.status_code)
        print url
        print 'Sleeping'
        sleep(5*attempt)
        request_data(url, attempt+1)


def loop_through_artists(artists):
    all_popularity_data = []
    for artist in artists:
        #sleep(4)
        sleep(3)
        pop_data = {}
        urls = build_urls(artist)
        if urls['spotify']:
            pop,follow = parse_spotify(request_data(urls['spotify']))
            pop_data['spotify_popularity'],pop_data['spotify_followers'] = pop,follow
        else:
            pop_data['spotify_popularity'],pop_data['spotify_followers'] = None,None
        if urls['echonest']:
            pop_data['echonest_hotttnesss'] = parse_echonest(request_data(urls['echonest']))
            pop_data['echonest_hotttnesss'] = pop_data['echonest_hotttnesss']*100
        else:
            pop_data['echonest_hotttnesss'] = None
        if pop_data:
            pop_data['id'], pop_data['name'], pop_data['spotify_id'], pop_data['echo_nest_id'] = artist
        all_popularity_data.append(pop_data)
        print 'Downloaded: ' + pop_data['name']
    return all_popularity_data

def parse_spotify(data):
    if not data:
        return None, None
    followers = data['followers']['total']
    pop = data['popularity']
    return pop, followers

def parse_echonest(data):
    if not data:
        return None
    return data['response']['artist']['hotttnesss']

def upload_popularity_data(popularity_data):
    conn = start_db_connection()
    with closing(conn.cursor()) as cur:
        cur.execute('''SELECT name FROM popularity_type''')
        popularity_types = [x[0] for x in cur.fetchall()]
        current_date = datetime.datetime.now().date()
        for pd in popularity_data:
            #if not any(v for k,v in dict((i, pd[i]) for i in popularity_types).iteritems()):
            #    continue

            cur.execute('''INSERT INTO popularity_point (artist_id, update_date)
                        SELECT artist.id, %s FROM artist WHERE name = %s''',
                        (current_date, pd['name']))

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
    conn.close()


def run():
    print 'Artist Popularity Data'
    artists = get_artists_to_update()
    all_popularity_data = loop_through_artists(artists)
    upload_popularity_data(all_popularity_data)


if __name__ == '__main__':
    run()
