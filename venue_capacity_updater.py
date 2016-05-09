import requests
from requests.exceptions import ConnectionError
from api_keys import song_kick_api
from dbconnection import start_db_connection
from contextlib import closing
import psycopg2
#from fuzzywuzzy import fuzz, process
import fuzzy
from time import sleep




#venues = sorted([x[0] for x in venues])
#for x in xrange(len(venues)):
#    print venues[x]
#    response = fuzzy.best_match_token(venues[x], venues, 5)[1:]
#    print [x for x in response if x[1]>.75]

def get_venues_to_update():
    conn = start_db_connection('AWS')
    with closing(conn.cursor()) as cur:
        #cur.execute('''SELECT name, state FROM venue WHERE skid IS NULL
        cur.execute('''SELECT name, city FROM venue WHERE skid IS NULL
                    AND capacity IS NULL''')
        venues = cur.fetchall()
    conn.close()
    return venues

def request_id(venues):
    venue_dict = {}
    for venue in venues:
        sleep(2.5)
        venue, city = venue
        print venue
        #url = make_url(venue, city)
        #print url
        try:
            #data = get_json(request_page(url))
            data = get_json(request_page(venue, city))
        except ConnectionError as e:
            print e
            continue
        if not data or data['resultsPage']['totalEntries'] < 1:
            sk_id = None
            continue
        else:
            possible_venue_info = get_pos_venues(data)
            sk_id = get_id_based_on_fuzzy(venue, city, possible_venue_info)
        print sk_id
        venue_dict[venue] = sk_id
    return venue_dict

def request_page(venue, city):
    base_url = 'http://api.songkick.com/api/3.0/search/venues.json'
    param_dict = {'apikey': song_kick_api,
              'query': venue + ' ' + city}

    return requests.get(base_url, params=param_dict)

def get_json(r):
    return r.json()

#def make_url(venue, city):
#    query = venue.replace(' ','+')+'+'+city
#    url = ('http://api.songkick.com/api/3.0/search/venues.json?query=%s&'
#            'apikey=%s' %(query, song_kick_api))
#    return url

def get_id_based_on_fuzzy(venue, city, possible_venue_info):
    pos_venue_names = get_pos_venue_names(possible_venue_info)
    fuzzy_matches = fuzzy.best_match_avg(venue, pos_venue_names.keys(),
                                             len(pos_venue_names))
    #print pos_venue_names
    #print fuzzy_matches
    #*****.80 random choice
    for match in [x[0] for x in fuzzy_matches if x[1] > 0.8]:
        if pos_venue_names[match][1]:
            return pos_venue_names[match]
    #else return best match without capacity
    return pos_venue_names[fuzzy_matches[0][0]]

def get_pos_venues(json):
    return json['resultsPage']['results']['venue']

def get_pos_venue_names(possible_venue_info):
    return {x['displayName']: (x['id'], x['capacity']) for x in possible_venue_info}

def upload_venue_capacity(venues):
    for venue, id_cap in venues.iteritems():
        sk_venue_id, capacity = id_cap
        conn = start_db_connection('AWS')
        with closing(conn.cursor()) as cur:
            cur.execute('''UPDATE venue SET skid=%s, capacity=%s
                        WHERE name=%s''', (sk_venue_id, capacity, venue))
            conn.commit()
        conn.close()

def run():
    print 'Venue Capacity Getter'
    venues = request_id(get_venues_to_update())
    upload_venue_capacity(venues)

if __name__ == '__main__':
    run()



