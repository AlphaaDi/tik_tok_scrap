from selenium import webdriver
from selenium.webdriver.firefox.options import Options

import time
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

from scrape_utils import *
from puzzle_solver import *
from tiktok_blocks_scrap import *

p = plt.imshow


def file2links(file_name, pattern = 'https://www.tiktok.com/@{arg}'):
    df = pd.read_csv(file_name)
    args = df[df.columns[0]].to_list()
    links = [pattern.format(arg=arg) for arg in args]
    return links


def get_geo_preferences(coords):
    lat, lng = str(round(coords[0],2)), str(round(coords[1],2))
    geo_str= 'data:application/json,{"location": {"lat": '+lat+', "lng": '+lng+'}, "accuracy": 100.0}'
    return {
        'geo.prompt.testing': True,
        'geo.prompt.testing.allow': True,
        'geo.provider.network.url': geo_str,
    }


def get_options(options, preferences):
    opts = Options()
    for key, val in options.items():
        if val is not None:
            opts.add_argument(f"{key}={val}")
        else:
            opts.add_argument(f"{key}")

    for key,val in preferences.items():
        opts.set_preference(key,val)
    return opts


def init_driver(
        options_dict = {
            '--headless': None,
            '--width': "1600px",
            '--height': "1000px",
        }, 
        preferences={}
    ):
    options = get_options(options_dict, preferences)
    driver = webdriver.Firefox(options=options)
    return driver
    

def wait(driver, secs):
    driver.implicitly_wait(secs)
    time.sleep(secs)


def do_max_scroll(driver, num_of_scrolls, check_hop=5):
    current_height = driver.execute_script("return document.body.scrollHeight")

    for scroll_idx in range(num_of_scrolls):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

        random_time = np.random.randint(1,3)
        wait(driver, random_time)

        if scroll_idx % check_hop == (check_hop-1):
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height <= current_height:
                break
            current_height = new_height


def scrape_pages(driver, queries, scrape_function, num_of_scrolls=50, load_wait_time=2):
    scraped_info_list = []
    for query in queries:
        try:
            print('query', query)
            driver.get(query)
            driver.implicitly_wait(load_wait_time)
            time.sleep(load_wait_time)
            try_solve_capture(driver)
            do_max_scroll(driver, num_of_scrolls)
            scraped_info = scrape_function(driver)
            scraped_info_list.append(scraped_info)
        except BaseException as ex:
            print('scrape_tiktok_pages Exception:', ex)
            continue

    return scraped_info_list
