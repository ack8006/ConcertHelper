import requests
import json
from dbconnection import start_db_connection
from contextlib import closing
from datetime import datetime
import psycopg2

#***********REFACTORS********
# Too many db openings and closings

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

def upload_events_to_db(event_data):
    conn = start_db_connection()
    with closing(conn.cursor()) as cur:
        for li in event_data:
            try:
                cur.execute('''INSERT INTO events (bit_event_id, event_date,
                            onsale_date, bit_venue_id, uploaded_date) VALUES (%s,
                            %s,%s,%s,%s)''', (li['bit_event_id'],
                                li['event_date'], li['onsale_date'],
                                li['bit_venue_id'], str(datetime.now())))
                for artist in li['artists']:
                    cur.execute('''INSERT INTO event_artists (bit_event_id,
                                artist) VALUES (%s,%s)''', (li['bit_event_id'],
                                                            artist))
                conn.commit()
                print 'Uploaded'
            except psycopg2.IntegrityError as e:
                conn.rollback()
                #print e
    conn.close()

def upload_artists_to_db(artist_data):
    conn = start_db_connection()
    with closing(conn.cursor()) as cur:
        for ar in artist_data:
            try:
                cur.execute('''INSERT INTO artists (name, mbid, bit_url) VALUES
                            (%s,%s,%s)''', (ar['name'], ar['mbid'], ar['url']))
                conn.commit()
                print 'Added %s' %(ar['name'])
            except psycopg2.IntegrityError as e:
                conn.rollback()
                print e
    conn.close()

def upload_venues_to_db(venue_data):
    conn = start_db_connection()
    with closing(conn.cursor()) as cur:
        for vn in venue_data:
            try:
                cur.execute('''INSERT INTO venues (bit_venue_id, name, venue_city,
                            venue_state, bit_url,latitude,longitude) VALUES (%s,
                            %s,%s,%s,%s,%s,%s)''', (vn['id'],vn['name'],vn['city'],
                                        vn['region'],vn['url'],vn['latitude'],
                                        vn['longitude']))
                conn.commit()
                print 'Venue %s Added' %(vn['name'])
            except psycopg2.IntegrityError as e:
                conn.rollback()
                print e
    conn.close()


def run():
    r_json = get_requested_json(request_page(make_onsale_url('new%20york,ny', 'YOUR_APP_ID')))
    #r_json = test_json()

    upload_events_to_db(get_event_data(r_json))
    upload_artists_to_db(get_artist_data(r_json))
    upload_venues_to_db(get_venue_data(r_json))
    #map(lambda x: upload_event_to_db(x), get_event_data(r_json))
    #map(lambda x: upload_artist_to_db(x), get_artist_data(r_json))
    #map(lambda x: upload_venue_to_db(x), get_venue_data(r_json))


if __name__ == '__main__':
    run()
