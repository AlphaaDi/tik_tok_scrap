import concurrent.futures
import numpy as np
import pandas as pd
import argparse
from munch import Munch
from functools import partial
from tiktok_main import init_driver, scrape_tiktok_pages, file2links
from tiktok_blocks_scrap import scrap_bloger_info
from parser_config import *

# parser = argparse.ArgumentParser(description='Process some files.')
# parser.add_argument('--config_path', default='files/scrap_config.yaml', type=str, help='the path to the config file')
# parser.add_argument('--queries_file', metavar='queries_file', type=str, help='the path to the queries file')
# parser.add_argument('--output_file', metavar='output_file', type=str, help='output file to store scrape')
# args = parser.parse_args()

def get_default_args():
    args = Munch(
        config_path='/home/davinci/work/tiktok_scrap/files/scrap_config.yaml',
        queries_file='/home/davinci/work/tiktok_scrap/data/tag_sport_bloggers.csv',
        output_file='/home/davinci/work/tiktok_scrap/data/sport_bloggers_info_v3.csv',
        num_worker=10,
    )
    return args


def scrape_info_from_list_name(args, driver_preferences = {}):
    results = []

    def scrape_and_store(scrap_bloger_info, queries, retry_num = 3):
        retries = 0
        while True:
            if retries >= retry_num:
                break
            try:
                driver = init_driver(preferences=driver_preferences)
                data = scrape_tiktok_pages(driver, queries, scrap_bloger_info, num_of_scrolls=1)
                results.append(data)
                driver.quit()
            except BaseException as ex:
                retries += 1
                print('scrape_and_store Exception:', ex)
                continue
            break
        return


    queries = file2links(args.queries_file)
    queries_chunks = np.array_split(np.array(queries),10)

    scrap_config = create_scrape_config(args.config_path)
    print(scrap_config)

    scrap_bloger_info_scrap = partial(scrap_bloger_info, scrap_config)

    scrape_and_store_bind = partial(scrape_and_store, scrap_bloger_info_scrap)

    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
        executor.map(scrape_and_store_bind, queries_chunks)

    all_result = pd.concat(map(pd.DataFrame,results), ignore_index=True)

    all_result.to_csv(args.output_file, index=False)