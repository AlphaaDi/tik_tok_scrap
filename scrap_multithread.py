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

from datetime import date
from pathlib import Path
import os
import re
import numpy as np
import pandas as pd
from functools import partial
from uuid import uuid1

from regexps import *

# parser = argparse.ArgumentParser(description='Process some files.')
# parser.add_argument('--config_path', default='files/scrap_config.yaml', type=str, help='the path to the config file')
# parser.add_argument('--queries_file', metavar='queries_file', type=str, help='the path to the queries file')
# parser.add_argument('--output_file', metavar='output_file', type=str, help='output file to store scrape')
# args = parser.parse_args()


def queries2file(queries, temp_dir):
    df = pd.DataFrame(queries)
    file_name = str(uuid1())
    path = os.path.join(temp_dir, f"{file_name}.csv")
    df.to_csv(path, index=False)
    return path


def reparse_csv(file_name):
    lst = pd.read_csv(file_name).iloc[0][0]
    matches = re.findall(r"'(.*?)'", lst)
    pd.DataFrame(matches).to_csv(file_name, index=False)


def get_date():
    current_date = date.today()
    current_date = str(current_date).replace('-', '_')
    return current_date


def merge_csvs(csvs_pathes):
    dfs = [pd.read_csv(csv_path) for csv_path in csvs_pathes]
    stacked_df = pd.concat(dfs, ignore_index=True)
    unique_elems = pd.unique(stacked_df.iloc[:,0])
    stacked_df = pd.DataFrame(unique_elems)
    return stacked_df


def save_csv(results, output_file):
    all_result = pd.concat(map(pd.DataFrame,results), ignore_index=True)
    all_result.to_csv(output_file, index=False)


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


class Scraper:
    def __init__(self, files_dir, temp_dir):
        self.files_dir = files_dir
        self.temp_dir = temp_dir


    def scrape_info_from_list_name(
            self, args, driver_preferences = {}, num_of_scrolls=1,
            pattern = 'https://www.tiktok.com/@{arg}'
        ):
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


    def scrap_search_type(self, search_type, query, query_file_name, geo_preferences):
        config_file_path, link_pattern = tiktok_search_config[search_type]
        names_output_file = os.path.join(self.files_dir, f'{search_type}_{query}_bloggers.csv')
        

        names_scrap_args = get_args(
            config_path=config_file_path,
            queries_file=query_file_name,
            output_file=names_output_file,
            num_worker=1
        )

        self.scrape_info_from_list_name(
            names_scrap_args, 
            driver_preferences=geo_preferences,
            num_of_scrolls=500,
            pattern=link_pattern,
        )

        reparse_csv(names_output_file)
        return names_output_file


    def scrap_blogger_info(self, query):
        query = query.replace(' ', '%20')
        date = get_date()
        
        geo_preferences = {}
            
        query_file_name = queries2file([query], self.temp_dir)

        names_files = []
        for search_type in tiktok_search_config.keys():
            names_output_file = self.scrap_search_type(search_type, query, query_file_name, geo_preferences)
            names_files.append(names_output_file)
        
        all_names_path = os.path.join(self.files_dir, f'{query}_bloggers.csv')

        stacked_df = merge_csvs(names_files)
        stacked_df.to_csv(
            all_names_path, index=False
        )

        for name_file in names_files:
            os.remove(name_file)
        os.remove(query_file_name)

        Path(os.path.join(self.files_dir, query)).mkdir(exist_ok=True)

        info_scrap_args = get_args(
            queries_file=all_names_path,
            output_file=os.path.join(self.files_dir, query, f'{date}.csv'),
        )
        
        self.scrape_info_from_list_name(
            info_scrap_args, 
            driver_preferences=geo_preferences,
        )
        
        os.remove(all_names_path)
        return info_scrap_args.output_file