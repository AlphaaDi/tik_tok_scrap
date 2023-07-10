import gradio as gr
import os
import re
import numpy as np
import pandas as pd
from functools import partial
from uuid import uuid1

from scrape_utils import get_location_by_city
from tiktok_main import init_driver, scrap_blogges_names, get_geo_preferences, scrape_pages
from tiktok_blocks_scrap import tiktok_num2count, scrap_bloger_info
from scrap_multithread import Scraper
from parser_config import *
from regexps import *

gr.close_all()
files_dir = "/home/davinci/work/tiktok_scrap/data"
temp_dir = '/home/davinci/work/tiktok_scrap/tmp'


scraper = Scraper(files_dir, temp_dir)


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


def values2csv(values):
    enum_vals = [str(val).replace('\n', ' ') for val in values]
    enum_str = ','.join(enum_vals)
    return enum_str


def show_table_columns(file_name):
    file_path = os.path.join(files_dir, file_name)
    table = pd.read_csv(file_path)
    col_str = values2str(table.columns)
    example_str = values2str(table.iloc[0])

    return col_str + '\n'+ example_str, '\n'.join(table.columns)


def parse_fields(fields_str):
    fields = fields_str.split('\n')
    return fields
    

def filter_file(
        file_name, min_folowers, max_folowers, min_likes, max_likes, 
        min_views, max_views, bio_search, fields_to_show):
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
    
    fields = parse_fields(fields_to_show)

    def do_str_row(row):
        return values2csv(row[fields])

    col_str = values2str(fields)
    passed_row_str = map(do_str_row, passed_rows)
    output = "\n".join(passed_row_str)
    output = col_str + "\n" + output
    return output


def find_emails(files_dir, file_name, info_fow = 'user bio'):
    file_path = os.path.join(files_dir, file_name)
    table = pd.read_csv(file_path)
    emails = []
    for idx in range(len(table)):
        row = table.iloc[idx]
        info = get_social_network(row[info_fow])
        emails.append(info)

    emails_str = ' '.join(emails)
    emails_str = re.sub(" +", "\n", emails_str)
    return emails_str
               


if __name__ == '__main__':
    css = '''
    #list_reloader {width: 100px; height: 50px; !important} 
    #list_reloader {left: 0 !important} 
    '''

    with gr.Blocks(theme=gr.themes.Glass(), css=css) as demo:
        files_dir_block = gr.Textbox(lines=1, value=files_dir, visible=False)

        gr.Markdown("Scrap Tools")
        with gr.Tab("Get bloggers nicknames"):
            with gr.Row():
                with gr.Column():
                    query = gr.inputs.Textbox(lines=1, placeholder="Enter Search Query Here...", label="Search Query")
                with gr.Column():
                    output_result = gr.outputs.Textbox(label="results")
                    scrap_button = gr.Button("scrap")

        scrap_button.click(
            scraper.scrap_blogger_info,
            inputs=[query],
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
                    fields_to_show = gr.outputs.Textbox(label="fields to show")
                    output_result_anal = gr.Textbox(label="results", show_copy_button=True)
                    with gr.Row():
                        result_button = gr.Button("Get bloggers")
                        email_button = gr.Button("Get emails")
        
            file_name.change(show_table_columns, file_name, [columns, fields_to_show])
            result_button.click(
                fn=filter_file,
                inputs=[
                    file_name, min_folowers, max_folowers, min_likes,
                    max_likes, min_views, max_views, bio_search, fields_to_show],
                outputs=output_result_anal
            )
            email_button.click(
                fn=find_emails,
                inputs=[files_dir_block, file_name],
                outputs=output_result_anal
            )

    demo.launch(server_port=8088)