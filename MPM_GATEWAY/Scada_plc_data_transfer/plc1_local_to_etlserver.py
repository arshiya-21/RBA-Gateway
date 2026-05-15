import sys
import os
current_dir_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.abspath(os.path.join(current_dir_path, '..')))

from mpmgateway.core.odbc_gateway import ODBC_gateway
import json

def read_config(config_file):
    with open(config_file, "r") as file:
        config = json.load(file)
    return config
config = read_config(os.path.join(current_dir_path,"local_to_etlserver.json"))

conn1 = ODBC_gateway(config,etl_config = None)
conn1.duplicate_db_source_to_local()