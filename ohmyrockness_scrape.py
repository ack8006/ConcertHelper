import requests
from datetime import datetime

import dateutil.parser
from selenium import webdriver 
from selenium.common import exceptions 
#from selenium.webdriver.support.select import Select 
from selenium.webdriver.common.by import By 
from selenium.webdriver.support.ui import WebDriverWait 
from selenium.webdriver.support import expected_conditions as EC


executable_path = 'C:\\Vendor\\phantomjs\\phantomjs.exe'


def start_browser(url):
    browser = webdriver.PhantomJS(executable_path)
    browser.set_window_size(1400,1000)
    browser.get(url)
    wait = WebDriverWait(browser, 15)
    wait.until(EC.presence_of_element_located((By.ID, 'shows')))
    return browser

def get_event_list(browser):
    return browser.find_elements_by_xpath('//*[@class="row vevent"]')

def parse_event_date(element):
    date_str = element.find_element_by_class_name('value-title').get_attribute("title")
    return dateutil.parser.parse(date_str)

def parse_bands(e):
    bands = []
    bands_elements = e.find_elements_by_xpath('./*[@class="bands summary"]/a')
    for band_el in bands_elements:
        bands.append((band_el.text, band_el.get_attribute("href").replace('http://www.ohmyrockness.com/bands/','')))
    return bands

def parse_recommended(e):
    try:
        if e.find_element_by_class_name('recommended').text:
            return True
    except exceptions.NoSuchElementException:
        return False

def parse_venue(e):
    return e.find_element_by_xpath('./*[@class="venue"]/*[@class="location vcard url"]').text

def parse_onsale(e):
    onsale_str = e.find_element_by_xpath('./*[@class="venue"]/*[@class="on-sale-at"]').text
    onsale_str = onsale_str.lower().replace('on sale ','').replace(' at ', '')
    print onsale_str
    onsale_date = datetime.strptime(onsale_str, '%a %m/%d%I:%M%p')
    current_date = datetime.now().date()
    if current_date.month > onsale_date.month:
        onsale_date = onsale_date.replace(year=current_date.year + 1)
    else:
        onsale_date = onsale_date.replace(year=current_date.year)
    return onsale_date

def parse_price(e):
    return e.find_element_by_xpath('./*[@class="tickets"]/*[@class="hoffer"]').text

def run():
    urls = {'New York': 'http://www.ohmyrockness.com/shows/on-sale-soon',
            'Los Angeles': 'http://losangeles.ohmyrockness.com/shows/on-sale-soon',
            'Chicago': 'http://chicago.ohmyrockness.com/shows/on-sale-soon'}

    #for url in urls:
    events = []
    for city, url in urls.iteritems():
        browser = start_browser(url)
        event_list = get_event_list(browser)
        for element in event_list:
            event_dict = {}
            event_dict['event_date'] = parse_event_date(element)
            event_dict['bands'] = parse_bands(element)
            event_dict['recommended'] = parse_recommended(element)
            event_dict['venue'] = parse_venue(element)
            event_dict['city'] = city
            event_dict['onsale_date'] = parse_onsale(element)
            event_dict['min_price'] = parse_price(element)
            events.append(event_dict)

    browser.quit()
    print events


if __name__ == '__main__':
    run()
