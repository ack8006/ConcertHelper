from dbconnection import start_db_connection
from contextlib import closing
from selenium import webdriver


def get_seats_to_update():
    conn = start_db_connection('AWS')
    with closing(conn.cursor()) as cur:
        cur.execute('''SELECT name, bitid, stubzone, eventid, stubhubid, v_name FROM
                    (SELECT sz.name, sz.id as stubzone, e.id as eventid, e.event_date as event_date,
                        sl.stubhubid, MAX(sp.update_time), e.bitid, v.name as v_name
                    FROM stubhub_listing sl
                    JOIN event e
                    ON e.id = sl.event_id
                    JOIN venue v
                    ON e.venue_id = v.id
                    JOIN event_artist ea
                    ON ea.event_id=e.id
                    JOIN artist a 
                    ON ea.artist_id=a.id
                    JOIN stubhub_point sp
                    ON sp.sl_id = sl.id
                    JOIN stubhub_price spr
                    ON spr.sp_id = sp.id
                    JOIN stubhub_zone sz
                    ON sz.id=spr.sz_id
                    LEFT JOIN event_price ep
                    ON ep.event_id=e.id AND ep.sz_id=sz.id
                    WHERE e.event_date > now()
                    AND ep.event_id IS NULL AND ep.sz_id IS NULL
                    GROUP BY sz.name, e.bitid, sz.id, e.id, sl.stubhubid, v.name
                    ORDER BY e.bitid) as foo
                    ORDER BY event_date
        	''')
        points = cur.fetchall()
    conn.close()
    return points

def parse_events(points):
    current_event_id = None
    current_points = []
    for point in points:
        if not current_event_id:
            current_event_id = point[1]
            current_points.append(point)
        elif current_event_id == point[1]:
            current_points.append(point)
        else:
            run_points(current_points, current_event_id)
            current_event_id = point[1]
            current_points = [point]

def run_points(current_points, current_event_id):
    print 'run_points'
    driver = open_page(current_event_id)
    prices = []
    for point in current_points:
        print point
        min_price = raw_input('MIN PRICE: ' + point[0] + ': ')
        if not min_price:
            min_price = None
        else:
            min_price = float(min_price)
        max_price = raw_input('MAX PRICE: ' + point[0] + ': ')
        if not max_price:
            max_price=min_price
        else:
            max_price = float(max_price)
        prices.append((point[3], point[2], min_price, max_price))
    driver.close()
    upload_prices(prices)

def open_page(current_event_id):
    url = 'http://www.bandsintown.com/event/{}/buy_tickets'.format(str(current_event_id))
    driver = webdriver.Firefox()
    driver.get(url)
    return driver

def upload_prices(prices):
    conn = start_db_connection('AWS')
    with closing(conn.cursor()) as cur:
        for price in prices:
            cur.execute('''INSERT INTO event_price (event_id, sz_id, min_price, max_price)
                        VALUES (%s,%s,%s,%s)''', (price[0],price[1],price[2],price[3]))
    conn.commit()
    conn.close()


def run():
    points = get_seats_to_update()
    parse_events(points)

if __name__ == '__main__':
    run()
