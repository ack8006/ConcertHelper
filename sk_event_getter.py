import requests
import json
from contextlib import closing
from datetime import datetime

from dbconnection import start_db_connection
from api_keys import song_kick_api

#Notes
    # Handle Paging

def get_songkick_response(venue_id, api_key, page=1):
    url = 'http://api.songkick.com/api/3.0/venues/{0}/calendar.json'.format(venue_id)
    params = {'apikey': api_key,
              'page': page}
    r = requests.get(url, params = params)
    if r.status_code != 200:
        print r.status_code
        return
    return r.json()

def check_total_entries(data):
    return data['resultsPage']['totalEntries']

#takes list of results dicts
#returns list of dict of dicts
def parse_raw_json(data_results):
    event_list = []
    for event in data_results['resultsPage']['results']['event'][:1]:
        parsed_event = parse_event(event)
        print parsed_event 

def parse_event(event):
    event_dict = {}
    event_dict['event_status'] = event['status']
    event_dict['event_popularity'] = event['popularity']
    event_dict['event_name'] = event['displayName']
    event_dict['event_age_restriction'] = event['ageRestriction']
    event_dict['event_type'] = event['type']
    event_dict['event_sk_id'] = event['id']
    event_dict['event_date'] = datetime.strptime(event['start']['datetime'].replace('T',' '), '%Y-%m-%d %H:%M:%S')
    event_dict['venue_dict'] = parse_venue(event['venue'])

    return event_dict

def parse_venue(venue):
    venue_dict = {}
    venue_dict['latitude'] = venue['lat']
    venue_dict['longitude'] = venue['lng']
    venue_dict['name'] = venue['displayName']
    venue_dict['sk_venue_id'] = venue['id']
    venue_dict['metro_name'] = venue['metroArea']['displayName']
    venue_dict['metro_country'] = venue['metroArea']['country']['displayName']
    venue_dict['metro_sk_id'] = venue['metroArea']['id']
    venue_dict['metro_state'] = venue['metroArea']['state']['displayName']
    return venue_dict



def main():
    data = get_songkick_response(431031, song_kick_api, 1)
    total_entries = check_total_entries(data)
    parse_raw_json(data)



if __name__ == '__main__':
    main()
