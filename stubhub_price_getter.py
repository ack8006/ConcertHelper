import requests
from api_keys import stubhub_application_token
from dbconnection import start_db_connection
from contextlib import closing
import datetime
from time import sleep


def get_stubhub_ids():
    conn = start_db_connection()
    with closing(conn.cursor()) as cur:
        cur.execute('''SELECT stubhub_id FROM events
                    WHERE stubhub_id IS NOT NULL and
                    event_date > now()''')
        stubhub_ids = [x[0] for x in cur.fetchall()]
    conn.close()
    return stubhub_ids

def get_price_data(stubhub_ids):
    stubhub_prices = {}
    for stubhub_id in stubhub_ids:
        #sleep(6)
        header = generate_header()
        payload = generate_payload(stubhub_id)
        #returns list of dicts
        stubhub_prices[stubhub_id] = request_price_data(payload,header)
    return stubhub_prices

def generate_header():
    return {'Authorization': 'Bearer ' + stubhub_application_token,
            'Accept-Encoding': 'application/json',
            'Accept': 'application/json'}

def generate_payload(stubhub_id):
    payload = {}
    payload['eventid'] = stubhub_id
    payload['pricingsummary'] = True
    payload['zonestats'] = True
    return payload

def request_price_data(payload, header):
    base_uri = 'https://api.stubhub.com/search/inventory/v1'
    r = requests.get(base_uri, params=payload, headers=header)
    if r.status_code/100 != 2:
        return
    data = r.json()
    return get_zone_stats(data)

def get_zone_stats(data):
    return data['zone_stats']

def upload_price_points(stubhub_prices):
    current_datetime = datetime.datetime.now()
    conn = start_db_connection()
    with closing(conn.cursor()) as cur:
        for stubhub_id, price_list in stubhub_prices.iteritems():
            for p in price_list:
                cur.execute('''INSERT INTO stubhub_prices (stubhub_id, zone_id,
                            zonename, min_ticket_price, max_ticket_price,
                            min_ticket_quantity, max_ticket_quantity, total_tickets,
                            total_listings, update_time)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''', (stubhub_id,
                            p['zoneId'],p['zoneName'],p['minTicketPrice'],
                            p['maxTicketPrice'],p['minTicketQuantity'],
                            p['maxTicketQuantity'],p['totalTickets'],p['totalListings'],
                            current_datetime))
                conn.commit()
    conn.close()





def main():
    upload_price_points(get_price_data(get_stubhub_ids()))


if __name__ == '__main__':
    main()
