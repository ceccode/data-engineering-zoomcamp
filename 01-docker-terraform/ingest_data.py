#!/usr/bin/env python
# coding: utf-8

import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine
from time import time
import argparse

def main(params):

    # Import environment variables 
    load_dotenv()
    password = os.getenv("PG_PASSWORD")

    user = params.user
    password = password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    # if url.endswith('.csv.gz'):
    #     csv_name = 'output.csv.gz'
    # else:
    #     csv_name = 'output.csv'

    # os.system(f"wget {url} -O {csv_name}")

    csv_file = "output.csv"
    parquet_file = "output.parquet"

    # Get the parquet file
    os.system(f"wget {url} -O {parquet_file}")

    # Load the file and get the csv
    df = pd.read_parquet(parquet_file, engine="pyarrow")

    # Get the CSV
    df.to_csv(csv_file, index=False)

    engine = create_engine(
        f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Create the iterative df
    df_iter = pd.read_csv(csv_file, iterator=True, chunksize=100000)

    # Get the current df and make formatting
    df = next(df_iter)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    # Insert in database
    # Insert the columns of the dataframe
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")
    df.to_sql(name=table_name, con=engine, if_exists="append")

    # Ingest the remain
    while True:
        try:
            # Start the timer
            t_start = time()

            # Read the next chunk
            df = next(df_iter)

            # Format the columns
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

            # Add data to the existing table
            df.to_sql(name=table_name, con=engine, if_exists="append")

            # End the timer
            t_end = time()

            print('Inserted another chunk... took %.3f second(s)' %
                  (t_end - t_start))

        except StopIteration:
            print("End of data importation.")
            break


if __name__ == '__main__':
    # Def the arguments
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True,
                        help='database name for postgres')
    parser.add_argument('--table_name', required=True,
                        help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')

    args = parser.parse_args()

    main(args)
