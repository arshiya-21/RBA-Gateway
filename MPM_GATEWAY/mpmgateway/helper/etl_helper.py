import pandas as pd
from datetime import datetime,time

def get_start_end(shift_timing):
    start = datetime.strptime(shift_timing[0],'%H:%M:%S').time()
    end = datetime.strptime(shift_timing[1],'%H:%M:%S').time()
    return start,end

def get_shift(df,time_column,shift_column,config_shift_timing):
    for shift_name,shift_timing in config_shift_timing.items():
        start,end = get_start_end(shift_timing)
        if start > end:
            mask1 = (df[time_column] >= start) & (df[time_column] <= time(23,59,59))
            mask2 = (df[time_column] >= time(0,0,0)) & (df[time_column] < end)
            for i in [mask1,mask2]:
                df.loc[mask1,shift_column] = shift_name
                df.loc[mask2,shift_column] = shift_name
        else:
            mask = (df[time_column] >= start) & (df[time_column] < end)
            df.loc[mask,shift_column] = shift_name

def get_datetime(row):
    try:
        date = pd.to_datetime(row,format = '%m/%d/%Y %H:%M')
        return date
    except:
        try:
            date = pd.to_datetime(row,format = '%m/%d/%Y %I:%M:%S %p')
            return date
        except:
            try:
                date = pd.to_datetime(row,format = '%m/%d/%Y')
                return date
            except:
                try:
                    date = pd.to_datetime(row,format = '%Y-%m-%d %H:%M:%S')
                    return date
                except:
                    print(row)

def get_column_mapping(column_mapping):
    column_mapping = {v: k for k, v in column_mapping.items() if v != ''}
    return column_mapping