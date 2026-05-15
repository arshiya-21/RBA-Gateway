import sys
import os
current_dir_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.abspath(os.path.join(current_dir_path, '..')))

from mpmgateway.core.S7_protocol_v2 import S7_protocol
import json

def read_config(config_file):
    with open(config_file, "r") as file:
        config = json.load(file)
    return config
config = read_config(os.path.join(current_dir_path,"plc_config.json"))

x = S7_protocol(config,etl_config = None)
x.plc_to_db()