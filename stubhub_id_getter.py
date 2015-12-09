import requests
#from api_keys import
from dbconnection import start_db_connection
import fuzzy
from contextlib import closing
from api_keys import stubhub_x_stubhub_user_guid, stubhub_application_token
import datetime
from time import sleep

#https://api.stubhub.com/search/catalog/events/v3?status=active&
#q=jukebox+the+ghost+2016-03-22


#THIS DOES NOT PULL ARTISTS WITHOUT POPULARITY DATA
#artists don't have popularity data without mbid_id
def get_events_to_update():
    conn = start_db_connection()
    with closing(conn.cursor()) as cur:
        cur.execute('''SELECT event_artists.artist, venues.name, venues.venue_state,
                    venues.latitude, venues.longitude, events.event_date,
                    events.onsale_date, artist_popularity.spotify_popularity,
                    artist_popularity.echo_nest_hotttnesss, events.bit_event_id
                    FROM events
                    INNER JOIN venues
                    ON events.bit_venue_id=venues.bit_venue_id
                    INNER JOIN event_artists
                    ON event_artists.bit_event_id = events.bit_event_id
                    INNER JOIN artist_popularity
                    ON artist_popularity.name=event_artists.artist
                    WHERE events.stubhub_id IS NULL AND
                    events.onsale_date < now()
                    ORDER BY events.event_date, events.onsale_date,
                    artist_popularity.echo_nest_hotttnesss DESC''')
        event_list = cur.fetchall()
    conn.close()
    return event_list

def parse_events(event_list):
    if not event_list:
        return None
    events_artists = []
    current = None
    other_artists = []
    p = 0
    while p < len(event_list):
        #print event_list[p]
        if not current:
            current, other_artists = event_list[p][1:], [event_list[p][0]]
        elif current[0]==event_list[p][1] and current[4]==event_list[p][5]:
            other_artists.append(event_list[p][0])
        else:
            events_artists.append((current, other_artists))
            current, other_artists = event_list[p][1:], [event_list[p][0]]
        p += 1
    events_artists.append((current, other_artists))
    return events_artists

def request_ids(event_info):
    event_info, artists = event_info
    bit_event_id = event_info[8]
    authentication_header = generate_authentication_header()
    for artist in artists:
        payload = generate_payload(event_info, artist)
        spotify_event_id = get_event_id(payload, authentication_header, event_info, artist)
    if spotify_event_id:
        return (bit_event_id, spotify_event_id)

def generate_authentication_header():
    return {'Authorization': 'Bearer ' + stubhub_application_token,
            'Accept-Encoding': 'application/json',
            'Accept': 'application/json'}

#event_info: venuename, state, lat, long, eventdate, onsaledate, spotpop, echo hotttness, bit id
def generate_payload(event_info, artist):
    payload = {}
    payload['point'] = gen_point(event_info[2],event_info[3])
    payload['state'] = event_info[1]
    payload['q'] = gen_query(event_info[0], artist)
    return payload

def gen_point(lat, longitude):
    return str(lat)+','+str(longitude)

def gen_query(venue, artist):
    return '+'.join((venue+' '+artist).split())

def get_event_id(payload, authentication_header, event_info, artist):
    base_uri = 'https://api.stubhub.com/search/catalog/events/v3'
    r = requests.get(base_uri, params=payload, headers=authentication_header)
    #print r.url
    if r.status_code != 200:
        return
    data = r.json()
    if not data['numFound']:
        return None
    for event in data['events']:
        if matches(event, event_info, artist):
            print 'StubHub Artist: ' + artist
            return event['id']

def matches(event, event_info, artist):
    def artist_matches():
        performers = [x['name'] for x in event['performers']]
        #print max(fuzzy.best_match_avg(artist, performers)) > 0.85
        return max(fuzzy.best_match_avg(artist, performers)) > 0.85

    def venue_matches():
        #print fuzzy.avg_all_techniques(event_info[0], event['displayAttributes']
        #                                ['primaryName'])
        return fuzzy.avg_all_techniques(event_info[0], event['displayAttributes']
                                        ['primaryName'])

    def date_matches():
        #print event['eventDateLocal'][:10] == event_info[4].strftime('%Y-%m-%d')
        return event['eventDateLocal'][:10] == event_info[4].strftime('%Y-%m-%d')

    return artist_matches() and venue_matches() and date_matches()


def upload_stubhub_ids(event_ids):
    conn = start_db_connection()
    with closing(conn.cursor()) as cur:
        for bit_id, stubhub_id in event_ids:
            cur.execute('''UPDATE events
                        SET stubhub_id = %s
                        WHERE bit_event_id = %s''', (stubhub_id, bit_id))
            conn.commit()
    conn.close()

def run():
    event_ids = []
    for event_info in parse_events(get_events_to_update()):
        sleep(5.75)
        event_id = request_ids(event_info)
        if event_id:
            event_ids.append(event_id)
    upload_stubhub_ids(event_ids)


if __name__ == '__main__':
    run()
