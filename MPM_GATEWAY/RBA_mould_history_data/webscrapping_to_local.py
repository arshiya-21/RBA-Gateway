import sys
import os
current_dir_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.abspath(os.path.join(current_dir_path, '..')))


from mpmgateway.core.webscrap_gateway_history import Scrapper
import json

def read_config(config_file):
    with open(config_file, "r") as file:
        config = json.load(file)
    return config
config = read_config(os.path.join(current_dir_path,"scrap_config.json"))
mould_config = read_config(os.path.join(current_dir_path,"etl_config.json"))

x = Scrapper(config,mould_config,etl_config = "/home/manoj/Desktop/MPM_GATEWAY/RBA_mould_history_data/process_dig_mould_data.py")
x.scrapper_to_local()

