import os
import logging
import argparse
import zipfile
import urllib.request

import xarray as xr
import pandas as pd
import geopandas as gpd

from pathlib import Path
from dotenv import load_dotenv

from extract import extract
from transform import transform_dataset
from load import load_dataset

# Logging
logging.basicConfig(level=logging.INFO)

# Project root directory
ROOT_DIR = Path(__file__).parent.parent.parent

# Load the environment variables
load_dotenv(ROOT_DIR / '.env')

GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
GCS_BUCKET = os.getenv('GCP_GCS_BUCKET')
GCP_SA_CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

def get_url(
        subset: str = 'ACTIVE', geom_type: str = 'points',
        version: str = 'v04r00', file_type: str = 'shapefile'
    ):
    """
    Get the URL for the IBTrACS dataset.
    """
    # Construct the URL for the netCDF file
    # URL: https://www.ncei.noaa.gov/data/international-best-track-archive-for-climate-stewardship-ibtracs/v04r00/access/netcdf/IBTrACS.ACTIVE.v04r00.nc
    if file_type == 'netcdf':
        url = ('https://www.ncei.noaa.gov/data/\
               international-best-track-archive-for-climate-stewardship-ibtracs/'\
              f'v04r00/access/netcdf/IBTrACS.{subset}.{version}.nc')
        
    # Construct the URL for the CSV file
    # URL: https://www.ncei.noaa.gov/data/international-best-track-archive-for-climate-stewardship-ibtracs/v04r00/access/csv/ibtracs.ACTIVE.list.v04r00.csv
    elif file_type == 'csv':
        url = ('https://www.ncei.noaa.gov/data/'\
               'international-best-track-archive-for-climate-stewardship-ibtracs' \
               f'/{version}/access/csv/ibtracs.{subset}.list.{version}.csv')

    # Construct the URL for the shapefile
    # URL: https://www.ncei.noaa.gov/data/international-best-track-archive-for-climate-stewardship-ibtracs/v04r00/access/shapefile/IBTrACS.ACTIVE.list.v04r00.points.zip
    elif file_type == 'shapefile':
        if geom_type == 'points':
            # Cast as 
            url = (f'https://www.ncei.noaa.gov/data/' \
                   'international-best-track-archive-for-climate-stewardship-ibtracs'\
                   f'{version}/access/shapefile/IBTrACS.{subset}.list.{version}.points.zip')
        elif geom_type == 'polygons':
            url = (f'https://www.ncei.noaa.gov/data/' \
                   'international-best-track-archive-for-climate-stewardship-ibtracs'\
                   f'{version}/access/shapefile/IBTrACS.{subset}.list.{version}.points.zip')
            
    return url

def get_filename(url: str):
    return url.split('/')[-1]

def get_local_filepath(name: str, *folders):
    """
    Get the local filepath for the IBTrACS dataset.
    """
    # Construct the local filepath
    data_dir = os.path.join(ROOT_DIR, *folders)
    os.makedirs(data_dir, exist_ok=True)
    local_filepath = os.path.join(data_dir, name)

    # Return the local filepath
    return local_filepath

def get_remote_filepath(name: str, *folders) -> str:
    """
    Get the remote filepath for the IBTrACS dataset.
    """
    # Construct the remote filepath
    remote_filepath = os.path.join(GCS_BUCKET, *folders, name)

    # Return the remote filepath
    return remote_filepath



# Pipeline Function
def run_pipeline(args: Params):
    """
    Run the ETL pipeline for the IBTrACS dataset.
    """
    # Parse the arguments
    subset = args.subset
    file_type = args.file_type
    geom_type = args.geom_type

    # Construct pipeline arguments
    url = get_url(subset=subset, geom_type=geom_type, file_type=file_type)
    filename = get_filename(url)
    local_filepath = get_local_filepath(filename, 'data', 'IBTrACS', file_type, subset)
    remote_filepath = get_remote_filepath(filename, 'data', 'IBTrACS', file_type, subset)
    table_name = f'{subset}_hurricane_{geom_type}'.lower() if geom_type else f'{subset}_hurricanes'.lower()

    # Extract data
    logging.info('EXTRACTING DATASET...')
    extract(url=url, local_filepath=local_filepath)

    # Transform data
    logging.info('TRANSFORMING DATASET...')
    



    # Transform data
    logging.info('TRANSFORMING DATASET...')
    transform_dataset(local_filepath=local_filepath)

    # Load datap
    logging.info('LOADING DATASET...')
    table_name = f'{pipeline_args.subset}_hurricane_{pipeline_args.geom_type}' if pipeline_args.geom_type else f'{pipeline_args.subset}_hurricanes'
    logging.debug(f'{local_filepath = }\t{table_name = }')
    load_dataset(local_filepath=local_filepath, table_name=table_name)

    # Delete the local file
    os.remove(local_filepath,)



# run the main function

if __name__ == '__main__':
    print()
    parser = argparse.ArgumentParser()

    parser.add_argument('--subset', type=str, default='ACTIVE', help='IBTrACS subset')
    parser.add_argument('--file_type', type=str, default='shapefile', help='IBTrACS file type')
    parser.add_argument('--geom_type', type=str, default='points', help='IBTrACS geometry type')

    args = parser.parse_args()

    params = Params(args.subset, args.file_type, args.geom_type)

    run_pipeline(params)

    print()