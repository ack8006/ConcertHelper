from time import sleep
from contextlib import closing

import requests

from dbconnection import start_db_connection
from fuzzywuzzy import fuzz, process
from api_keys import song_kick_api


#Notes
# Unicode Handling is shit. Get unicode from database.

# MBID
    # Moe. has wrong mbid on sk


def get_unmatched_artists():
    conn = start_db_connection('AWS')
    with closing(conn.cursor()) as cur:
        cur.execute('''SELECT id, name, mbidid FROM artist
                    WHERE skid IS NULL
                    ORDER BY id''')

        artists = cur.fetchall()
    conn.close()
    return artists

def lookup_artist(artist_name, mbid):
    #***
    s_data = get_songkick_response(artist_name)
    if not s_data:
        return None, None
    match_rank = get_match_ranks(artist_name, s_data)
    if not match_rank:
        return None, None
    print
    print 'Artist_Name: ' + artist_name
    #print match_rank [:5]
    best_match = parse_match_rank(match_rank)
    if best_match:
        print 'BEST MATCH!!!: ' + str(best_match)

        if best_match[4] and (best_match[4] == mbid):
            pass
            #print 'MBID MATCH', mbid
        elif not mbid and best_match[4]:
            return best_match, True
            print 'Mbid missing {}'.format(best_match[4])
        elif not best_match[4] and not mbid:
            pass
        else:
            print 'MBID MISMATCH', best_match[4], mbid
    return best_match, False

def get_songkick_response(artist_name):
    sleep(1)
    url = 'http://api.songkick.com/api/3.0/search/artists.json'
    r = requests.get(url, params={'query': artist_name,
                                  'apikey': song_kick_api})
    if r.status_code != 200:
        print r.status_code
        sleep(1.3)
        return
    return r.json()

def get_match_ranks(artist_name, s_data):
    if s_data['resultsPage']['totalEntries'] == 0:
        return
    s_data = s_data['resultsPage']['results']['artist']
    try:
        match_rank = [(ind, 
                       x['displayName'], 
                       fuzz.ratio(clean_artist(artist_name), clean_artist(x['displayName'])), 
                       x['id'], 
                       get_mbid(x))
                      for ind, x in enumerate(s_data)]
        match_rank.sort(key=lambda x: x[2], reverse = True)
        return match_rank
    except UnicodeDecodeError as e:
        print e

def clean_artist(name):
    return name.lower().replace('.','').replace('&','and')

def get_mbid(x):
    if len(x['identifier']) > 0:
        return x['identifier'][0]['mbid']
    return None

def parse_match_rank(match_rank):
    if match_rank[0][2] > 95:
        if len(match_rank) == 1:
            return match_rank[0]
        elif match_rank[1][2] < 90:
            print match_rank[0][1], ', ', match_rank[1][1]
            return match_rank[0]
    return

def update_database(artist_id, skid, mbid = None):
    conn = start_db_connection('AWS')
    with closing(conn.cursor()) as cur:
        query = '''UPDATE artist SET skid = '{0}' '''.format(skid)
        if mbid:
            query += ", mbidid = '{0}' ".format(mbid)
        query += "WHERE id = '{0}'".format(artist_id)

        cur.execute(query)
        conn.commit()
    conn.close()
    print 'Updated artist_id: ' + str(artist_id)

def run():
    artists = get_unmatched_artists()
    for a in artists:
        best_match, update_mbid = lookup_artist(a[1], a[2])
        if best_match:
            if update_mbid:
                update_database(a[0], best_match[3], mbid=best_match[4])
            else:
                update_database(a[0], best_match[3])
        else:
            update_database(a[0], 'no_match', update_mbid)

if __name__ ==  '__main__':
    run()
