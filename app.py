import gradio as gr
import os
import numpy as np
import pandas as pd
from functools import partial

from scrape_utils import tiktok_username2link, get_location_by_city
from tiktok_main import init_driver, scrape_tiktok, scrap_blogges_names, get_geo_preferences
from tiktok_blocks_scrap import tiktok_num2count
import info_scrap_multithread

gr.close_all()
files_dir = "/home/davinci/work/tiktok_scrap/data"


search_channel_config = {
    'tag': ('challenge-item-username', 'https://www.tiktok.com/tag/{query}'),
    'search': ('search-user-unique-id', 'https://www.tiktok.com/search/user?q={query}'),
}

def scrap_blogger_info(tag_of_search, location, query):
    query = query.replace(' ', '%20')
    nickname_tag_attr, link_pattern = search_channel_config[tag_of_search]

    geo_preferences = {}
    if location != '':
        coords = get_location_by_city(location)
        geo_preferences = get_geo_preferences(coords)
        print('coords', coords)

    driver = init_driver(preferences=geo_preferences)
    names_output_file = os.path.join(files_dir, f'{tag_of_search}_{query}_bloggers.csv')
    print('scrape_tiktok')
    print(
        link_pattern.format(query=query),
        names_output_file,
        nickname_tag_attr
    )
    scrape_tiktok(
        driver,
        link_pattern.format(query=query),
        names_output_file,
        partial(scrap_blogges_names, nickname_tag_attr)
    )
    driver = init_driver(preferences=geo_preferences)
    info_scrap_args = info_scrap_multithread.get_default_args()
    info_scrap_args.queries_file = names_output_file
    info_scrap_args.output_file = os.path.join(files_dir, f'{tag_of_search}_{query}_information.csv')
    print('info_scrap_args', info_scrap_args)
    info_scrap_multithread.scrape_info_from_list_name(info_scrap_args, driver_preferences=geo_preferences)
    return f'result was saved in {info_scrap_args.output_file} \n '


def list_of_tables():
    return [file for file in os.listdir(files_dir) if file.endswith(".csv")]


def build_range(range_name):
    with gr.Row():
        min_range = gr.inputs.Textbox(lines=1, placeholder=f"min {range_name}", label=f"min {range_name}")
        max_range = gr.inputs.Textbox(lines=1, placeholder=f"max {range_name}", label=f"max {range_name}")
    return min_range, max_range

def get_range_filter(min_val, max_val):
    if (min_val != '') and (max_val != ''):
        return lambda x: int(min_val) <= tiktok_num2count(x) <= int(max_val)
    return lambda x: True

def values2str(values, trunc=20):
    enum_vals = [f"{idx+1}. " + str(val)[:trunc] for idx,val in enumerate(values)]
    enum_str = f"|{'|'.join(enum_vals)}|"
    return enum_str

def show_table_columns(file_name):
    file_path = os.path.join(files_dir, file_name)
    table = pd.read_csv(file_path)
    col_str = values2str(table.columns)
    example_str = values2str(table.iloc[0])
    return col_str + '\n'+ example_str

def filter_file(file_name, min_folowers, max_folowers, min_likes, max_likes, min_views, max_views, bio_search):
    file_path = os.path.join(files_dir, file_name)
    table = pd.read_csv(file_path)
    passed_rows = []
    bio_search_filter = lambda x: bio_search in x
    filters = [
        get_range_filter(min_folowers, max_folowers),
        get_range_filter(min_likes, max_likes),
        get_range_filter(min_views, max_views),
        bio_search_filter
    ]        
    for idx in range(len(table)):
        row = table.iloc[idx]
        info = [
            row['followers count'],
            row['likes count'],
            row['median views'],
            row['user bio'],
        ]

        is_pass = all([filter_(field) for filter_, field in zip(filters, info)])
        if is_pass:    
            passed_rows.append(row)
    
    fields = [
        'user title', 'followers count', 'likes count', 'user link',
        'median views', 'views bucket', 'video count'
    ]

    def do_str_row(row):
        return values2str(row[fields], 100) 

    col_str = values2str(fields)
    passed_row_str = map(do_str_row, passed_rows)
    output = "\n".join(passed_row_str)
    output = col_str + "\n" + output
    return output



css = '''
#list_reloader {width: 100px; height: 50px; !important} 
#list_reloader {left: 0 !important} 
'''

with gr.Blocks(theme=gr.themes.Glass(), css=css) as demo:
    gr.Markdown("Scrap Tools")
    with gr.Tab("Get bloggers nicknames"):
        with gr.Row():
            with gr.Column():
                tag_of_search = gr.inputs.Radio(['tag', 'search'], label="Option", default='search')
                location = gr.inputs.Textbox(lines=1, placeholder="Enter Location Here...", label="Location")
                query = gr.inputs.Textbox(lines=1, placeholder="Enter Search Query Here...", label="Search Query")
            with gr.Column():
                output_result = gr.outputs.Textbox(label="results")
                scrap_button = gr.Button("scrap")

    scrap_button.click(
        scrap_blogger_info,
        inputs=[tag_of_search, location, query],
        outputs=output_result
    )

    with gr.Tab("Analytics table"):
        with gr.Row():
            with gr.Column():
                with gr.Row():
                    file_name = gr.inputs.Dropdown(list_of_tables(), label="Select Video")
                    list_reloader = gr.Button("Reload files list", elem_id='list_reloader')
                columns = gr.inputs.Textbox(lines=1, label="columns + first row")
                min_folowers, max_folowers = build_range("followers")
                min_likes, max_likes = build_range("likes")
                min_views, max_views = build_range("views")
                bio_search = gr.inputs.Textbox(
                    lines=1, default="", placeholder="", 
                    label="show only profiles what contain:")
            with gr.Column():
                output_result = gr.outputs.Textbox(label="results")
                result_button = gr.Button("Get bloggers")
    
        file_name.change(show_table_columns, file_name, columns)
        result_button.click(
            fn=filter_file,
            inputs=[file_name, min_folowers, max_folowers, min_likes, max_likes, min_views, max_views, bio_search],
            outputs=output_result
        )

demo.launch(server_port=8088)