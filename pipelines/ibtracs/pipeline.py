import os
import glob
import zipfile
import logging
import shutil
import pyproj
import subprocess
import urllib.request
from configparser import ConfigParser
from dotenv import load_dotenv
from pathlib import Path
from argparse import ArgumentParser

# Set up logging
logging.basicConfig(level=logging.INFO)


def ingest_shapefile_to_postgis(url: str, data_dir: str = None, table_name: str = None, database: str = None, username: str = None):
    # Download the shapefile
    filename = os.path.basename(url)
    logging.info(f'Downloading {filename}')
    urllib.request.urlretrieve(url, filename)

    # Extract the contents of the zip file
    data_dir = data_dir or os.path.splitext(filename)[0]
    logging.info(f'Extracting zip file {filename} to {data_dir}')
    with zipfile.ZipFile(filename, 'r') as zip_ref:
        zip_ref.extractall(data_dir)    

    # Load the data into PostGIS
    shp_path = glob.glob(f'{data_dir}/*.shp')
    print()
    input(f'{shp_path=}')
    print()
    command = f'shp2pgsql -d -I -s 4326 {shp_path} {table_name} | psql -d {database} -U {username}'
    logging.info(f'Loading data into PostGIS with `shp2pgsql` and `psql')
    subprocess.run(command, shell=True)

    # Clean up
    shutil.rmtree(os.path.splitext(os.path.basename(url))[0])

def main():
    url = 'https://www.ncei.noaa.gov/data/international-best-track-archive-for-climate-stewardship-ibtracs/v04r00/access/shapefile/IBTrACS.ACTIVE.list.v04r00.points.zip'
    data_dir = 'data/IBTrACS/active/points'
    username = 'postgres'
    database = 'weather_gis'
    schema = 'raw'
    table_name = 'active_hurricanes'

    ingest_shapefile_to_postgis(url, data_dir, f'{schema}.{table_name}', database, username)


if __name__ == '__main__':
    print()
    main()
    print()