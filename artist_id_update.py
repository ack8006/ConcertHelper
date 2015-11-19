from dbconnection import start_db_connection
from api_keys import echo_nest_api
from contextlib import closing
from time import sleep
import requests
import sys


def get_artist_ids(alt_id='spotify'):
    def generate_url(mbid):
        url = '''http://developer.echonest.com/api/v4/artist/profile?api_key=%s
               &id=musicbrainz:artist:%s&format=json&bucket=id:%s''' %(echo_nest_api,
                                                                      mbid, alt_id)
        return url

    #Gentle API Rate Limit
    def request_alt_id(mbid):
        print mbid
        for sleep_length in [3,30,60,120]:
            sleep(sleep_length)
            r = requests.get(generate_url(mbid))
            #NEED TRY
            #ALSO NEED TO CHECK FOR RESPONSE
            if r.status_code == 200:
                return r.json()
            elif r.status_code == 429:
                print '429, sleep increased from %s' %(sleep_length)
                continue
            else:
                print str(r.status_code), str(mbid)
                return None
        return None

    def get_id_from_json(json, mbid):
        ids = [mbid,None,None]
        if not json:
            return ids
        try:
            ids[1] = json['response']['artist']['foreign_ids'][0]['foreign_id'].replace('spotify:artist:','')
        except KeyError as e:
            ids[1] = 'n/a'
        try:
            ids[2] = json['response']['artist']['id']
        except KeyError as e:
            ids[2] = 'n/a'

        return ids

    #return [get_id_from_json(request_alt_id(mbid),mbid) for mbid in get_mbids()[:30]]
    return [get_id_from_json(request_alt_id(mbid),mbid) for mbid in get_mbids()]


def get_mbids():
    conn = start_db_connection()
    with closing(conn.cursor()) as cur:
        cur.execute('''SELECT DISTINCT mbid FROM artists WHERE mbid IS NOT NULL
                    AND spotify_id IS NULL''')
        data = [x[0] for x in cur.fetchall()]
    conn.close()
    return data

def update_artist_db(id_pair):
    conn = start_db_connection()
    with closing(conn.cursor()) as cur:
        cur.execute('''UPDATE artists SET spotify_id=%s, echo_nest_id = %s
                    WHERE mbid=%s''', (id_pair[1],id_pair[2],id_pair[0]))
        conn.commit()
        print 'Updated %s' %(id_pair[0])
    conn.close()


def run():
    map(lambda x:update_artist_db(x), get_artist_ids())

if __name__ == '__main__':
    run()
