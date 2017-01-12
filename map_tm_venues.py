from contextlib import closing

import requests
import pandas as pd

from dbconnection import start_db_connection
from api_keys import tm_api_key



def search_tm_venue(name, state):
    parameters = {'keyword': name,
                  'stateCode': state,
                  'apikey': tm_api_key}
    base_url = 'https://app.ticketmaster.com/discovery/v2/venues.json'
    r = requests.get(base_url, params = parameters)
    data = r.json()
    for x in data['_embedded']['venues']:
        print x

def main():
    #conn = start_db_connection()
    #with closing(conn.cursor()) as cur:
    #    cur.execute('''SELECT * FROM venue''')
    #    data = cur.fetchall()
    #    print data[:10]
    #conn.close()
    search_tm_venue('Cutting Room', 'NY')

if __name__ == '__main__':
    main()
