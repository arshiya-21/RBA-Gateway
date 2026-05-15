import pandas as pd

def file_name_logic(string,file_keyword):
    if 'DAILY' in file_keyword:
        return pd.to_datetime(string.split('_')[-1].split('.')[0],format = '%d%m%Y')
    elif 'MIXLOG' in file_keyword:
        return pd.to_datetime((string.split('_',1)[-1].split('.')[0]),format = '%Y_%m_%d')