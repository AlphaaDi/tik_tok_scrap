from bs4 import BeautifulSoup

import numpy as np
import pandas as pd
from functools import partial

from parser_config import *


def scrap_blogges_names(class_name, driver):
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    bloggers = soup.find_all('p', {'data-e2e': class_name})
    bloggers = [blogger.text for blogger in bloggers]
    return bloggers


def get_tag_text(soup, attrs):
    tag = soup.find_all(attrs=attrs)[0]
    value = tag.text
    return value


def get_all_tags_text(soup, attrs):
    tags = soup.find_all(attrs=attrs)
    values = [tag.text for tag in tags]
    return values


FUNC_STR2PARSER = {
    'get_tag_text': get_tag_text,
    'get_all_tags_text': get_all_tags_text,
}


def tiktok_num2count(tiktoknum):
    tiktoknum = str(tiktoknum)
    if tiktoknum.endswith('K'):
        return int(float(tiktoknum[:-1]) * 1e3)
    if tiktoknum.endswith('M'):
        return int(float(tiktoknum[:-1]) * 1e6)
    if tiktoknum.endswith('B'):
        return int(float(tiktoknum[:-1]) * 1e9)
    return int(tiktoknum)


def put_in_dict_bucket(dict_, views):
    for range_, str_range in dict_.items():
        if range_[0] <= views < range_[1]:
            return str_range
    return "None"

def put_in_dict_bucket_items(dict_, items):
    return put_in_dict_bucket(dict_, items['median views'])


def count_video(items):
    return len(items['video views'])


put_in_view_bucket = partial(put_in_dict_bucket_items, {
        (0,50_000): "0-50K",
        (50_000,500_000): "50K-500K",
        (500_000,2_000_000): "500K-2M",
        (500_000,2_000_000): "2M+",
})


def get_tiktok_username2link(items):
    user_title = items['user title']
    return f'https://www.tiktok.com/@{user_title}'


def median_view(items):
    video_views = items['video views']
    views = list(map(tiktok_num2count, video_views))
    median_view = int(np.median(views))
    return median_view


FUNC_STR2ADD_OPTION_FUNC = {
    'get_tiktok_username2link': get_tiktok_username2link,
    'median_view': median_view,
    'put_in_view_bucket': put_in_view_bucket,
    'count_video': count_video,
}


def scrap_bloger_info(scrap_config, driver):
    soup = BeautifulSoup(driver.page_source, 'html.parser')

    items = {}
    for scrap_item in scrap_config:
        if isinstance(scrap_item, ScrapOption):
            field_table_name = scrap_item.field_table_name
            scrap_func_str = scrap_item.scrap_func_str
            attrs = scrap_item.attrs
            scrap_func = FUNC_STR2PARSER[scrap_func_str]
            items[field_table_name] = scrap_func(
                soup,
                attrs
            )
        elif isinstance(scrap_item, AddOption):
            field_table_name = scrap_item.field_table_name
            func_to_tranform = scrap_item.func_to_tranform
            add_option_func = FUNC_STR2ADD_OPTION_FUNC[func_to_tranform]
            items[field_table_name] = add_option_func(items)
        elif isinstance(scrap_item, RemoveOption):
            field_table_name = scrap_item.field_table_name
            del items[field_table_name]

    print('items', items)
    print('scape', items['user title'])
    
    return items
