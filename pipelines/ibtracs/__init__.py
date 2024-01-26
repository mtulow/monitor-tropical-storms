import os
import gcsfs
import logging
import argparse
import zipfile
import urllib.request

import xarray as xr
import pandas as pd
import geopandas as gpd

from pathlib import Path
from dotenv import load_dotenv

from extract import Params, extract_dataset
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


# Pipeline Function
def run_pipeline(pipeline_args: Params):
    """
    Run the ETL pipeline for the IBTrACS dataset.
    """
    # Extract data
    logging.info('EXTRACTING DATASET...')
    local_filepath, remote_filepath = extract_dataset(pipeline_args.file_type, pipeline_args.geom_type, pipeline_args.subset)

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