import os
import fsspec
import logging
import urllib.request
import geopandas as gpd
from zipfile import ZipFile
from sqlalchemy import create_engine
from dotenv import load_dotenv


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Download the data
def download_data(url: str, file_name: str = None, data_dir: str = None):
    """
    Download the data
    """
    # Create the data directory if it does not exist
    data_dir = data_dir or 'data'
    os.makedirs(data_dir, exist_ok=True)

    # Default file name to basename of url
    file_name = file_name or os.path.basename(url)

    # Create the file path
    file_path = os.path.join(data_dir, file_name)

    # Download the data
    logging.info(f'Downloading file: {file_path} ...')
    output, msg = urllib.request.urlretrieve(url, file_path,)
    
    # Check if the download was successful
    print()
    print(f'Output: {output}')
    print(f'Message: {msg}')
    print()

    # Return the file path
    return file_path





# Create a connection to the database
def _connect_to_postgres(env_file: str = '.env',):
    """
    Connect to the database
    """
    # Load environment variables
    load_dotenv()

    # Read the environment variables
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT")
    dbname = os.getenv("POSTGRES_DB")

    # Create the connection string
    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
    
    # Create the engine
    return create_engine(connection_string)

# Load the data into postgres
def upload_to_postgres(df: gpd.GeoDataFrame, table_name: str, schema: str = 'raw',
                       dtype: dict = None, if_exists: str = 'replace'):
    """
    Load the data into postgres
    """
    # Connect to the database
    engine = _connect_to_postgres()

    # Load the data into postgres
    dtype = dtype or {'geometry': 'geometry'}
    df.to_postgis(table_name, engine, schema=schema, if_exists=if_exists,
                  index=False, chunksize=10_000, dtype=dtype)
    

def main():
    # Create the url and file path
    url = 'https://www.ncei.noaa.gov/data/international-best-track-archive-for-climate-stewardship-ibtracs/v04r00/access/shapefile/IBTrACS.ACTIVE.list.v04r00.points.zip'
    file_path = os.path.join('data', 'IBTrACS', os.path.basename(url))

    # Download the data
    output = download_data(url, file_path)

    
    # # Read data into a geopandas dataframe
    # df = gpd.read_file( ZipFile(url).open('IBTrACS.ACTIVE.list.v04r00.points.shp') )
    # fsspec.open_zip(url).read()


if __name__ == '__main__':
    print()
    main()
    print()

