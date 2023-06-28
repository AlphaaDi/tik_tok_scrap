import concurrent.futures
import numpy as np
import re
import pandas as pd
import argparse
from munch import Munch
from functools import partial
from tiktok_main import init_driver, scrape_pages, file2links
from tiktok_blocks_scrap import scrap_bloger_info
from parser_config import *

# parser = argparse.ArgumentParser(description='Process some files.')
# parser.add_argument('--config_path', default='files/scrap_config.yaml', type=str, help='the path to the config file')
# parser.add_argument('--queries_file', metavar='queries_file', type=str, help='the path to the queries file')
# parser.add_argument('--output_file', metavar='output_file', type=str, help='output file to store scrape')
# args = parser.parse_args()

def get_args(
        config_path='/home/davinci/work/tiktok_scrap/files/scrap_config_tiktok.yaml',
        queries_file='/home/davinci/work/tiktok_scrap/data/tag_sport_bloggers.csv',
        output_file='/home/davinci/work/tiktok_scrap/data/sport_bloggers_info_v3.csv',
        num_worker=10,
):
    args = Munch(
        config_path=config_path,
        queries_file=queries_file,
        output_file=output_file,
        num_worker=num_worker,
    )
    return args


def save_csv(results, output_file):
    all_result = pd.concat(map(pd.DataFrame,results), ignore_index=True)
    all_result.to_csv(output_file, index=False)


def scrape_info_from_list_name(args, driver_preferences = {}, num_of_scrolls=1, pattern = 'https://www.tiktok.com/@{arg}'):
    results = []

    def scrape_and_store(scrap_bloger_info, queries, retry_num = 3):
        retries = 0
        while True:
            if retries >= retry_num:
                break
            try:
                driver = init_driver(preferences=driver_preferences)
                data = scrape_pages(driver, queries, scrap_bloger_info, num_of_scrolls=num_of_scrolls)
                results.append(data)
                driver.quit()
            except BaseException as ex:
                retries += 1
                print('scrape_and_store Exception:', ex)
                continue
            break
        return


    queries = file2links(args.queries_file, pattern=pattern)
    queries_chunks = np.array_split(np.array(queries),10)

    scrap_config = create_scrape_config(args.config_path)

    scrap_bloger_info_scrap = partial(scrap_bloger_info, scrap_config)

    scrape_and_store_bind = partial(scrape_and_store, scrap_bloger_info_scrap)

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.num_worker) as executor:
        executor.map(scrape_and_store_bind, queries_chunks)

    save_csv(results, args.output_file)