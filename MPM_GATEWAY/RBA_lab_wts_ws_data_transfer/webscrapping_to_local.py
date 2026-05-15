import sys
import os
current_dir_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.abspath(os.path.join(current_dir_path, '..')))


from mpmgateway.core.webscrap_gateway_v2 import Scrapper
import json

def read_config(config_file):
    with open(config_file, "r") as file:
        config = json.load(file)
    return config
config = read_config(os.path.join(current_dir_path,"scrap_config.json"))

x = Scrapper(config,etl_config = "/home/manoj/Desktop/MPM_GATEWAY/RBA_lab_wts_ws_data_transfer/process_dig_wet_tensile.py")
x.scrapper_to_local()

