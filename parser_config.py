from collections import namedtuple
import yaml

ScrapOption = namedtuple('ScrapOption', ['field_table_name', 'scrap_func_str', 'attrs'])
AddOption = namedtuple('AddOption', ['field_table_name', 'func_to_tranform'])
RemoveOption = namedtuple('RemoveOption', ['field_table_name'])

Option_Str2Func = {
    'scrap_option': ScrapOption,
    'add_option': AddOption,
    'remove_option': RemoveOption,
}

def create_scrape_config(yaml_file):
    with open(yaml_file, "r") as stream:
        scrap_config = yaml.safe_load(stream)

    config_options = []

    for options in scrap_config:
        class_fn = Option_Str2Func[options]
        for option in scrap_config[options]:
            config_options.append(
                class_fn(**option)
            )
    
    return config_options