import smtplib
from base64 import b64decode
from api_keys import yahoo_user, yahoo_pass, host, to_addr, from_addr
from dbconnection import start_db_connection
from contextlib import closing
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime


def get_upcoming_concerts(days=7):
    conn = start_db_connection('AWS')
    upcoming_concerts = None
    with closing(conn.cursor()) as cur:
        cur.execute('''SELECT a.name, e.onsale_date, e.event_date, v.name, v.capacity, mr_pop.value
                    FROM artist a
                    JOIN event_artist ea
                        ON ea.artist_id=a.id
                    JOIN event e
                        ON e.id=ea.event_id
                    JOIN venue v
                        ON v.id=e.venue_id
                    JOIN popularity_point pp
                        ON pp.artist_id = a.id
                    JOIN (SELECT pp.id, pv.pt_id, pv.value, foo.max
                        FROM popularity_point pp
                        JOIN popularity_value pv ON pv.pp_id=pp.id
                        JOIN
                        (SELECT pp.artist_id, pv.pt_id, MAX(pp.update_date)
                        FROM popularity_point pp
                        JOIN popularity_value pv ON pv.pp_id = pp.id
                        GROUP BY pp.artist_id, pv.pt_id
                        ORDER BY pp.artist_id) as foo
                        ON foo.artist_id=pp.artist_id
                            AND foo.pt_id=pv.pt_id
                            AND pp.update_date = foo.max) as mr_pop
                        ON pp.id=mr_pop.id
                    WHERE mr_pop.pt_id=2 AND e.onsale_date > now()
                    ORDER BY e.onsale_date, mr_pop.value DESC;
                    ''')
        upcoming_concerts = cur.fetchall()
    conn.close()
    return upcoming_concerts

def send_email(host, username, password, to_addr, from_addr, msg):
    host = "smtp.mail.yahoo.com"
    try:
        server = smtplib.SMTP(host ,587)
        server.starttls()
        server.login(username,password)
        server.sendmail(from_addr, to_addr, msg)
        print 'Email has been sent'
    except Exception as e:
        print e
        print 'Did not Send Email'
    finally:
        server.quit()

def create_message(from_addr, to_addr, concerts, concert_header):
    def create_html_table():
        table = '<table border=1>'
        table += '<tr><th>'
        table += '</th><th>'.join(concert_header)
        table += '</th></tr>'
        for row in concerts:
            row = format_row(row)
            table += '<tr><td style="padding:0 15px 0 15px;">'
            table += '</td><td style="padding:0 15px 0 15px;">'.join(row)
            table += '</td></tr>'
        return table + '</table>'

    def format_row(row):
        row = list(row)
        row[1] = datetime.strftime(row[1], '%m/%d/%Y %H:%M:%S')
        row[2] = datetime.strftime(row[2], '%m/%d/%Y %H:%M:%S')
        row[-1] = "%.2f"%float(row[-1])
        row = [str(x) if x else '' for x in row]
        return row

    msg = MIMEMultipart('alternative')
    msg['Subject'] = 'Upcoming Concerts ' + datetime.strftime(datetime.now(), '%m/%d/%Y %H:%M:%S')
    msg['From'] = from_addr
    msg['To'] = to_addr
    msg.attach(MIMEText(create_html_table(), 'html'))
    return msg


if __name__ == '__main__':
    concerts = get_upcoming_concerts(7)
    concert_header = ['Artist', 'Onsale Date', 'Event Date', 'Venue',
                      'Capacty', 'Echonest Popularity']
    msg=create_message(from_addr, to_addr, concerts, concert_header)
    send_email(host, yahoo_user, b64decode(yahoo_pass), to_addr,
               from_addr, msg.as_string())
