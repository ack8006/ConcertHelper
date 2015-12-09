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
        #updated in greater than 7 days
        cur.execute('''SELECT name, spotify_id, echo_nest_id FROM
                    (SELECT DISTINCT ON (a.name)
                    a.name, a.spotify_id, a.echo_nest_id, MAX(ap.update_date)
                    FROM artists a
                    LEFT JOIN artist_popularity ap
                    ON a.name = ap.name
                    WHERE (ap.name IS NULL AND
                    ((a.spotify_id IS NOT NULL AND a.spotify_id <> 'n/a') OR
                    (a.echo_nest_id IS NOT NULL AND a.echo_nest_id <> 'n/a')))
                    OR ap.name IS NOT NULL
                    GROUP BY a.name) as foo
                    WHERE max < now()-'7 days'::interval OR max IS NULL;
                    ''')
        data = cur.fetchall()
    conn.close()
    return data

#takes data tuple
def build_urls(ar_inf):
    urls = {}
    if ar_inf[2] != 'n/a':
        urls['echonest'] = ('http://developer.echonest.com/api/v4/artist/hotttnesss?api_key'
                '=%s&id=%s&format=json' %(echo_nest_api, ar_inf[2]))
    else:
        urls['echonest'] = None
    if ar_inf[1] != 'n/a':
        urls['spotify'] = 'https://api.spotify.com/v1/artists/%s' %(ar_inf[1])
    else:
        urls['spotify'] = None
    return urls

def request_data(url):
    try:
        r = requests.get(url)
    except requests.exceptions.ConnectionError as e:
        print url
        print e
        #print e.args[0].reason

    if r.status_code == 200:
        return r.json()
    else:
        print 'Status Code: %s' %(r.status_code)
        print url
        print 'Sleeping'
        sleep(5)

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
            pop_data['hotttnesss'] = parse_echonest(request_data(urls['echonest']))
        else:
            pop_data['hotttnesss'] = None
        if pop_data:
            pop_data['name'], pop_data['spotify_id'], pop_data['echo_nest_id'] = artist
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
        for pd in popularity_data:
            try:
                cur.execute('''INSERT INTO artist_popularity (name, spotify_id,
                            echo_nest_id,spotify_popularity, spotify_followers,
                            echo_nest_hotttnesss, update_date) VALUES (%s,%s,%s,%s,
                            %s,%s,%s)''', (pd['name'],pd['spotify_id'],
                                pd['echo_nest_id'],pd['spotify_popularity'],
                                pd['spotify_followers'],pd['hotttnesss'],
                                datetime.datetime.now().date()))
                conn.commit()
            except psycopg2.IntegrityError as e:
                conn.rollback()
                print e
    conn.close()


def run():
    artists = get_artists_to_update()
    all_popularity_data = loop_through_artists(artists)
    upload_popularity_data(all_popularity_data)

if __name__ == '__main__':
    run()
















