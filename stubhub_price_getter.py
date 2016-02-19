import requests
from api_keys import stubhub_application_token
from dbconnection import start_db_connection
from contextlib import closing
import datetime
from time import sleep


def get_stubhub_ids():
    conn = start_db_connection()
    with closing(conn.cursor()) as cur:
        cur.execute('''SELECT sl.stubhub_id FROM event e
                    JOIN stubhub_listing sl ON e.id=sl.event_id
                    WHERE e.event_date > now()''')
        stubhub_ids = [x[0] for x in cur.fetchall()]
    conn.close()
    print stubhub_ids
    return stubhub_ids

def get_price_data(stubhub_ids):
    stubhub_prices = {}
    for stubhub_id in stubhub_ids:
        print stubhub_id
        header = generate_header()
        payload = generate_payload(stubhub_id)
        #returns list of dicts
        price_data = request_price_data(payload,header)
        if price_data:
            stubhub_prices[stubhub_id] = price_data
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
    sleep(6)
    base_uri = 'https://api.stubhub.com/search/inventory/v1'
    try:
        r = requests.get(base_uri, params=payload, headers=header)
        #print r.url
        if r.status_code/100 != 2:
            return
        data = r.json()
        return get_zone_stats(data)
    except requests.exceptions.ConnectionError as e:
        print e

def get_zone_stats(data):
    return data['zone_stats']

def upload_price_points(stubhub_prices):
    current_datetime = datetime.datetime.now()
    conn = start_db_connection()
    with closing(conn.cursor()) as cur:
        for stubhub_id, price_list in stubhub_prices.iteritems():
            cur.execute('''INSERT INTO stubhub_point (stubhub_listing_id,
                        update_time) SELECT id, %s FROM stubhub_listing WHERE
                        stubhub_id = %s''', (current_datetime, stubhub_id))
            conn.commit()

            for p in price_list:
                cur.execute('''INSERT INTO stubhub_zone (id, name)
                            SELECT %s,%s WHERE NOT EXISTS (SELECT * FROM
                            stubhub_zone WHERE id=%s)''', (p['zoneId'],
                            p['zoneName'], p['zoneId']))

                cur.execute('''INSERT INTO stubhub_price (stubhub_point_id,
                            stubhub_zone_id, min_price, max_price, min_ticket_quantity,
                            max_ticket_quantity, total_tickets, total_listings)
                            SELECT id,%s,%s,%s,%s,%s,%s,%s FROM (SELECT sp.id
                            FROM stubhub_point sp JOIN stubhub_listing sl
                            ON sp.stubhub_listing_id=sl.id
                            WHERE sl.stubhub_id=%s AND sp.update_time=%s)
                            as foo''', (p['zoneId'],
                            p['minTicketPrice'], p['maxTicketPrice'],p['minTicketQuantity'],
                            p['maxTicketQuantity'],p['totalTickets'],p['totalListings'],
                            stubhub_id, current_datetime))
            conn.commit()
    conn.close()

def main():
    upload_price_points(get_price_data(get_stubhub_ids()))


if __name__ == '__main__':
    main()
