import logging
from hooks.clickhouse_hook import ClickhouseHook
import pandas as pd
import hashlib


MCC = [262,460,310,208,510,404,250,724,234,311]


def load_data_ch(source_path, chunksize=100000, **kwargs):
    hook = ClickhouseHook(conn_id='clickhouse')
    sql = """INSERT INTO public.cell_towers 
        (radio, mcc, net, area, cell, unit, lon, lat, range, samples, changeable, created, updated, averageSignal, hash_id) 
        VALUES"""

    for i,chunk in enumerate(pd.read_csv(source_path, chunksize=chunksize)):
        filter_df = chunk[chunk.mcc.isin(MCC)]
        filter_df['hash_id'] = filter_df.apply(lambda x: hashlib.md5(str(x.values).encode()).hexdigest(), axis = 1)
        hook.insert_dataframe(filter_df, sql, settings=dict(use_numpy=True))
        logging.info(f"loaded {filter_df.shape[0]} rows")
