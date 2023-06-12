import os
import requests
from pathlib import Path

def download_one_file_of_raw_data(year, month, raw_data_dir='../data/raw/'):
    URL = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet'
    os.makedirs(raw_data_dir, exist_ok=True)
    RAW_DATA_DIR = Path(raw_data_dir)
    local_file = RAW_DATA_DIR / f'yellow_tripdata_{year}-{month:02d}.parquet'

    if local_file.exists():
        return "File already exists"
    else:
        response = requests.get(URL)
        if response.status_code == 200:
            with open(local_file, 'wb') as f:
                f.write(response.content)
            return local_file
        else:
            raise Exception(f"{URL} is not available")
        

def validate_raw_data(df, year, month):
    df = df[df['tpep_pickup_datetime'] >= f'{year}-{month:02d}-01 00:00:00'].copy()
    df = df[df['tpep_pickup_datetime'] <= f'{year}-{month:02d}-31 23:59:59'].copy()

    return df


def select_and_rename_columns(df):
    df = df[['tpep_pickup_datetime', 'PULocationID']].rename(columns={'tpep_pickup_datetime': 'pickup_datetime', 'PULocationID': 'location_id'}).copy()
    return df


def aggregate_raw_data(df):
    df['pickup_hour'] = df['pickup_datetime'].dt.floor('H')
    df = df.groupby(['pickup_hour', 'location_id']).size().reset_index(name='rides')

    return df