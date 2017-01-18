import requests

from selenium import webdriver 
#from selenium.common import exceptions 
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


def run():
    urls = ['http://www.ohmyrockness.com/shows/on-sale-soon',
            'http://losangeles.ohmyrockness.com/shows/on-sale-soon',
            'http://chicago.ohmyrockness.com/shows/on-sale-soon']

    #for url in urls:
    for url in urls[:1]:
        browser = start_browser(url)
        event_list = get_event_list(browser)
        for e in event_list:
            print e.text



if __name__ == '__main__':
    run()
