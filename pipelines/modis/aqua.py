# Import packages
import os
import sys
import argparse
import fsspec
import shutil
import requests
import urllib.request
import numpy as np
import xarray as xr
import datetime as dt
import geowombat as gw
import rasterio as rio
import rioxarray as rxr
import matplotlib.pyplot as plt
from osgeo import gdal
from pathlib import Path
from netrc import netrc

# -------------------------------------SET UP AUTHENTICATION------------------------------------- #
# Set up authentication
netrc_dir = os.path.join(os.path.expanduser('~'), '.netrc')
urs = 'urs.earthdata.nasa.gov'
username, _, password = netrc(netrc_dir).authenticators(urs)

# -------------------------------------SET UP SERVER AND DATASET--------------------------------- #
# Set up server and dataset
server = 'https://e4ftl01.cr.usgs.gov'


# # ----------------------------------USER-DEFINED VARIABLES--------------------------------------- #
# # Set up command line arguments
# parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
# parser.add_argument('-dir', '--directory', required=True, help='Specify directory to save files to',
#                     default='data/Aqua/MOD09CMG.061/2024.01.10')
# parser.add_argument('-f', '--files', required=True, default='2024.01.09/MOD09CMG.A2024009.061.2024011034135.hdf',
#                     help='A single granule URL, or the location of csv or textfile containing granule URLs')
# args = parser.parse_args()

sub_dir = 'data/Aqua/MOD09CMG.061/2024.01.10' # args.directory  # Set local directory to download to
files = ''        # Define file(s) to download from the LP DAAC Data Pool

file_list = [
    'https://e4ftl01.cr.usgs.gov/MOLA/MYD09CMG.061/2024.01.01/MYD09CMG.A2024001.061.2024003121128.hdf', # 562MB
    'https://e4ftl01.cr.usgs.gov/MOLA/MYD09CMG.061/2024.01.02/MYD09CMG.A2024002.061.2024004191935.hdf', # 571MB
    'https://e4ftl01.cr.usgs.gov/MOLA/MYD09CMG.061/2024.01.03/MYD09CMG.A2024003.061.2024005145218.hdf', # 559MB
    'https://e4ftl01.cr.usgs.gov/MOLA/MYD09CMG.061/2024.01.04/MYD09CMG.A2024004.061.2024006045232.hdf', # 572MB
    'https://e4ftl01.cr.usgs.gov/MOLA/MYD09CMG.061/2024.01.05/MYD09CMG.A2024005.061.2024007040212.hdf', # 558MB
    'https://e4ftl01.cr.usgs.gov/MOLA/MYD09CMG.061/2024.01.06/MYD09CMG.A2024006.061.2024008041848.hdf', # 571MB
    'https://e4ftl01.cr.usgs.gov/MOLA/MYD09CMG.061/2024.01.07/MYD09CMG.A2024007.061.2024009085229.hdf', # 569MB
    'https://e4ftl01.cr.usgs.gov/MOLA/MYD09CMG.061/2024.01.08/MYD09CMG.A2024008.061.2024010121142.hdf', # 563MB
    'https://e4ftl01.cr.usgs.gov/MOLA/MYD09CMG.061/2024.01.09/MYD09CMG.A2024009.061.2024011180142.hdf', # 567MB
    'https://e4ftl01.cr.usgs.gov/MOLA/MYD09CMG.061/2024.01.10/MYD09CMG.A2024010.061.2024012072609.hdf', # 562MB
]



"""
| Parameters | Description | Argument |
| --- | --- | --- |
| driver | the name of the desired format driver | 'GTiff' |
| width | the number of columns of the dataset | Z.shape[1] |
| height | the number of rows of the dataset | Z.shape[0] |
| count | a count of the dataset bands | 1 |
| dtype | the data type of the dataset | Z.dtype |
| crs | a coordinate reference system identifier or description | '+proj=latlong' |
| transform | an affine transformation matrix | Affine.translation(x[0] - xres / 2, y[0] - yres / 2) * Affine.scale(xres, yres) |
| nodata | a “nodata” value | -9999 |
"""


# -------------------------------------ELT FUNCTIONS--------------------------------- #

def download_file(file_list: list[str], sub_dir: str):
    """
    Downloads a list of files to a specified directory

    Args:
        file_list: list of files to download
        sub_dir: directory to save files to
    """
    # Delete directory if it exists
    if os.path.exists(sub_dir):
        shutil.rmtree(sub_dir)

    # Create directory
    os.makedirs(sub_dir, exist_ok=True)
    
    # Loop through and download all files to the directory specified above, and keeping same filenames
    for f in file_list:

        # Construct local file path
        dst_file = os.path.join(sub_dir, os.path.basename(f))

        # Create and submit request and download file
        with requests.get(f.strip(), verify=False, stream=True, auth=(username, password)) as response:
            if response.status_code != 200:
                print("{} not downloaded. Verify that your username and password are correct in {}".format(os.path.basename(f), netrc_dir))
            else:
                print(f'Downloading file {f} to {dst_file} ...')
                with open(dst_file, 'wb') as d:
                    # Write the file to local directory
                    response.raw.decode_content = True
                    content = response.raw
                    while True:
                        chunk = content.read(16 * 1024)
                        if not chunk:
                            break
                        d.write(chunk)
                    


def main():
    """
    Main function
    """
    # Download file(s)
    print('Downloading file(s) to {}'.format(sub_dir))
    print()
    print('Files to download:')
    for f in file_list:
        print(f)
        download_file(file_list, sub_dir)



# -----------------------------------------DOWNLOAD FILE(S)-------------------------------------- #
if __name__ == '__main__':
    print()
    main()
    print()