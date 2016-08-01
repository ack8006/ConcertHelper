from time import sleep
from contextlib import closing

import requests

from dbconnection import start_db_connection
from fuzzywuzzy import fuzz, process
#from api_keys import spo


#Notes
#SHould do an &/and replacement
#consider removing the? "Marcus King Band" vs. "The Marcus King Band" But this is probbly not good in all situations
    # The Ghost of Paul Reveer
#Consider Removing punctuation, "L.E.J." vs. "L.E.J" only returns 91 match
#"Boys' Night" vs. "Boys Night"
#"Camille Harris" 100 "Camille Harrison" 93
#"John Molo" 100 "John Moloney" 86
#"The Branches" 100 "The Low Branches" 86

#Ryan Lewis, Edward Sharpe,


def get_unmatched_artists():
    conn = start_db_connection('AWS')
    with closing(conn.cursor()) as cur:
        cur.execute('''SELECT id, name FROM artist
                    WHERE (spotifyid IS NULL
                    OR spotifyid = 'n/a')
                    ORDER BY id''')


        artists = cur.fetchall()
    conn.close()
    return artists

def lookup_artist(artist_name):
    s_data = get_spotify_response(artist_name)
    if not s_data:
        return
#manage if no real return
    match_rank = get_match_ranks(artist_name, s_data)
    if not match_rank:
        return
    print
    print 'Artist_Name: ' + artist_name
    print match_rank [:5]
    best_match = parse_match_rank(match_rank)
    if best_match:
        print 'BEST MATCH!!!: ' + str(best_match)
    return best_match

def get_spotify_response(artist_name):
    sleep(1)
    url = 'https://api.spotify.com/v1/search'
    r = requests.get(url, params={'q': artist_name,
                                  'type':'artist',
                                  'limit': 50})
    if r.status_code != 200:
        print r.status_code
        sleep(3)
        return
    return r.json()

def get_match_ranks(artist_name, s_data):
    s_data = s_data['artists']['items']
    try:
        l_an = artist_name.lower()
        match_rank = [(ind, x['name'], fuzz.ratio(clean_artist(artist_name), clean_artist(x['name'])), x['id'])
                      for ind, x in enumerate(s_data)]
        match_rank.sort(key=lambda x: x[2], reverse = True)
        return match_rank
    except UnicodeDecodeError as e:
        print e

def clean_artist(name):
    return name.lower().replace('.','').replace('&','and')

def parse_match_rank(match_rank):
    if match_rank[0][2] > 95:
        if len(match_rank) == 1:
            return match_rank[0]
        elif match_rank[1][2] < 90:
            return match_rank[0]
    return

#def update_database(artist_id, best_match):
def update_database(artist_id, spotify_id):
    conn = start_db_connection('AWS')
    with closing(conn.cursor()) as cur:
        cur.execute('''UPDATE artist
                        SET spotifyid = '{0}'
                        WHERE id = '{1}'
                    '''.format(spotify_id, artist_id))
        conn.commit()
    conn.close()
    print 'Updated artist_id: ' + str(artist_id)

def run():
    artists = get_unmatched_artists()
    for a in artists:
        best_match = lookup_artist(a[1])
        if best_match:
            update_database(a[0], best_match[3])
        else:
            update_database(a[0], 'no_match')

if __name__ ==  '__main__':
    run()
