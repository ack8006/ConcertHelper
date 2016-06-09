import requests
from dbconnection import start_db_connection
from contextlib import closing
from bs4 import BeautifulSoup
from time import sleep




def get_event_ids():
    conn = start_db_connection()
    with closing(conn.cursor()) as cur:
        cur.execute('''SELECT e.bitid FROM event e
                    LEFT JOIN event_price ep ON e.id=ep.event_id
                    WHERE ep.event_id IS NULL''')

        event_ids = [x[0] for x in cur.fetchall()]
    conn.close()
    return event_ids

def get_event_prices(event_ids):
    event_prices = {}
    for event_id in event_ids:
        prices = request_prices(generate_url(event_id))
        if prices:
            event_prices[event_id] = prices

def generate_url(event_id):
    return 'http://www.bandsintown.com/event/{}/buy_tickets'.format(event_id)

def request_prices(url):
    print url
    page_text = request_page(url).text
    #http://www.ticketmaster.com/event/00004F89F64E7BB8

    soup = BeautifulSoup(page_text, 'lxml')
    tm_id = find_tm_event_id(soup.body.text)

def find_tm_event_id(body_text):
    print body_text
    search_string = 'ticketmaster.com%2Fevent%2F'
    id_length = 16

    if "Hmmm, we didn't find what you were looking for" in body_text:
        print "Hmmm, we didn't find what you were looking for"

    elif body_text.find(search_string) != -1:
        search_ix = body_text.find(search_string)+len(search_string)
        print body_text[search_ix: search_ix+id_length]
    else:
        first = body_text[body_text.rfind('window.location.replace') + 22:]
        print first[first.find("('")+2: first.find("');")]

def request_page(url):
    sleep(1)
    try:
        return requests.get(url)
    except requests.exceptions.ConnectionError as e:
        print e

def run():
    get_event_prices(get_event_ids()[0:1])
    #request_prices('http://www.bandsintown.com/event/11052472/buy_tickets')
    #request_prices('http://www.bandsintown.com/event/11064070/buy_tickets')
    #request_prices('http://www.bandsintown.com/event/11064373/buy_tickets')

if __name__ == '__main__':
    run()
