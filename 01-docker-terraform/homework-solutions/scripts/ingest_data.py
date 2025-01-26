#!/usr/bin/env python
# coding: utf-8

import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine
from time import time
import argparse


def download_and_convert_to_csv(url: str, csv_file: str, parquet_file: str) -> str:
    """
    Downloads a file from `url`. If it's a Parquet file, convert it to CSV.
    If it's a .csv.gz, unzips to a final CSV. Otherwise, just save the CSV.
    Returns the path to the final CSV file (always uncompressed).
    """
    # If it’s Parquet...
    if url.endswith('.parquet'):
        os.system(f"wget {url} -O {parquet_file}")
        df = pd.read_parquet(parquet_file, engine="pyarrow")
        df.to_csv(csv_file, index=False)
        return csv_file

    # If it’s .csv.gz...
    elif url.endswith('.csv.gz'):
        # Download and store temporarily as output.csv.gz
        os.system(f"wget {url} -O {csv_file}.gz")
        # Unzip (overwrites output.csv if it already exists)
        os.system(f"gunzip -f {csv_file}.gz")
        # Now the final file is 'output.csv' (plain CSV)
        return csv_file

    else:
        # Otherwise, just a normal CSV file
        os.system(f"wget {url} -O {csv_file}")
        return csv_file


def transform_datetime_columns(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    """
    Convert the specified columns to datetime if they exist in df.
    """
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])
    return df


def main(params):

    # Import environment variables
    load_dotenv()

    env_password = os.getenv("PG_PASSWORD")
    cli_password = params.password  # from --password

    datetime_cols = params.datetime_cols.split(
        ',') if params.datetime_cols else []

    user = params.user
    password = cli_password if cli_password else env_password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    # Define file names
    csv_file = "output.csv"
    parquet_file = "output.parquet"

    # Download and possibly unzip/convert to CSV
    final_csv_file = download_and_convert_to_csv(url, csv_file, parquet_file)

    # ---- At this point, final_csv_file is an uncompressed CSV
    df = pd.read_csv(final_csv_file)

    engine = create_engine(
        f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Create the iterative df
    df_iter = pd.read_csv(final_csv_file, iterator=True, chunksize=100000)

    # Get the current df and make formatting
    df = next(df_iter)
    df = transform_datetime_columns(df, datetime_cols)

    # Insert in database
    # Insert the columns of the dataframe
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")
    df.to_sql(name=table_name, con=engine, if_exists="append")

    # Ingest the remain
    while True:
        try:
            t_start = time()

            df = next(df_iter)
            df = transform_datetime_columns(df, datetime_cols)
            df.to_sql(name=table_name, con=engine, if_exists="append")

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
    parser.add_argument('--password', required=False,
                        help='password for postgres. Detected from environment if not provided.')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True,
                        help='database name for postgres')
    parser.add_argument('--table_name', required=True,
                        help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')
    parser.add_argument(
        '--datetime_cols',
        required=False,
        help='Comma-separated list of columns to parse as datetimes.'
    )

    args = parser.parse_args()

    main(args)
