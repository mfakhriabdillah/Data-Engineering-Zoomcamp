#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import os
from sqlalchemy import create_engine
import argparse

def main(params):
    user = params.user
    password = params.password
    port = params.port
    host = params.host
    db = params.db
    table_name = params.table_name
    url = params.url
    csv_name = 'output.csv'
    
    
    # Download the csv
    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{table_name}')

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)

    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')

    from time import time

    while True:
        t_start = time()
        
        df = next(df_iter)

        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

        df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()

        print('inserted another chunk..., took %.3f second' % (t_end - t_start))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres.')

    # user 
    # password
    # host
    # port
    # databasae_name
    # table name

    parser.add_argument('--user', help='Username for postgres')
    parser.add_argument('--password', help='Password for postgres')
    parser.add_argument('--host', help='Host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='DB Name for postgres')
    parser.add_argument('--table_name', help='Table for postgres')
    parser.add_argument('--url', help='URL for postgres')
    args = parser.parse_args()
    
    main(args)