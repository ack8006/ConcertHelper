import requests
import json
from dbconnection import start_db_connection
from contextlib import closing
from datetime import datetime
import psycopg2


#test only file
def test_json():
    with open('on_sale_soon.html') as f:
        return json.load(f)

def make_onsale_url(location, app_id):
    return ('http://api.bandsintown.com/events/on_sale_soon.json?'
           'location={0}&app_id={1}'.format(location, app_id))

def request_page(url):
    return requests.get(url)

def get_requested_json(r):
    return r.json()

def get_event_data(json_data):
    return map(clean_listing_data, json_data)

#keeping unicode for example of foreign artist
def clean_listing_data(listing):
    listing_info = {}
    listing_info['bit_event_id'] = str(listing['id'])
    listing_info['event_date'] = datetime.strptime(listing['datetime'].replace('T',' '),
                                                   '%Y-%m-%d %H:%M:%S')
    listing_info['onsale_date'] = datetime.strptime(listing['on_sale_datetime'].replace('T',' '),
                                                   '%Y-%m-%d %H:%M:%S')
    listing_info['artists'] = map(lambda x: x['name'], listing['artists'])
    listing_info['bit_venue_id'] = str(listing['venue']['id'])
    return listing_info

def get_artist_data(listing):
    return reduce(lambda a,x: a+x, [x['artists'] for x in listing])

def get_venue_data(listing):
    return [x['venue'] for x in listing]


def upload_event_to_db(li):
    #table_queries = {'events': ['''INSERT INTO events (bit_event_id, artists,
    #                 event_date, onsale_date, bit_venue_id) VALUES(%s,%s,%s,%s,%s)''',
    #                 (li['bit_event_id'], li['artists'], li['event_date'],
    #                  li['onsale_date'], li['bit_venue_id'])],
    #                 'artists': ['''INSERT INTO artists (name, mbid, bit_url) VALUES
    #                 (%s,%s,%s)''', (li['name'], li['mbid'], li['url'])],
    #                 'venues': None
    #                 }
    conn = start_db_connection()
    with closing(conn.cursor()) as cur:
        try:
            cur.execute('''INSERT INTO events (bit_event_id, artists, event_date,
                        onsale_date, bit_venue_id) VALUES (%s,%s,%s,%s,%s)''',
                        (li['bit_event_id'], li['artists'], li['event_date'],
                         li['onsale_date'], li['bit_venue_id']))
            conn.commit()
            print 'Uploaded'
        except psycopg2.IntegrityError as e:
            conn.rollback()
            print e
    conn.close()

def upload_artist_to_db(ar):
    conn = start_db_connection()
    with closing(conn.cursor()) as cur:
        try:
            cur.execute('''INSERT INTO artists (name, mbid, bit_url) VALUES
                        (%s,%s,%s)''', (ar['name'], ar['mbid'], ar['url']))
            conn.commit()
            print 'Added %s' %(ar['name'])
        except psycopg2.IntegrityError as e:
            conn.rollback()
            print e
    conn.close()

def upload_venue_to_db(vn):
    conn = start_db_connection()
    with closing(conn.cursor()) as cur:
        try:
            cur.execute('''INSERT INTO venues (bit_venue_id, name, venue_city,
                        venue_state, bit_url) VALUES (%s,%s,%s,%s,%s)''',
                        (vn['id'],vn['name'],vn['city'],vn['region'],vn['url']))
            conn.commit()
            print 'Venue %s Added' %(vn['name'])
        except psycopg2.IntegrityError as e:
            conn.rollback()
            print e
    conn.close()


if __name__ == '__main__':
    r_json = get_requested_json(request_page(make_onsale_url('new%20york,ny', 'YOUR_APP_ID')))
    #r_json = test_json()

    map(lambda x: upload_event_to_db(x), get_event_data(r_json))
    map(lambda x: upload_artist_to_db(x), get_artist_data(r_json))
    map(lambda x: upload_venue_to_db(x), get_venue_data(r_json))
