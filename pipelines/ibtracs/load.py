import os
import logging
import geopandas as gpd
from sqlalchemy import create_engine

# Set up logging
logging.basicConfig(level=logging.INFO)

# Load Function
def upload_to_gcs():
    pass

def ingest_netcdf_to_postgres(local_filepath: str, file_type: str = None):
    pass

def ingest_csv_to_postgres(local_filepath: str, file_type: str = None):
    pass

def ingest_shapefile_to_postgres(local_filepath: str, table_name: str, schema: str = 'dev'):
    # Get the .shp file from the zip archive
    for file in filter(lambda f: f.endswith('.shp'), os.listdir(local_filepath.replace('.zip', ''))):
        local_filepath = os.path.join(local_filepath.replace('.zip', ''), file)
        break

    # Read the shapefile
    data = gpd.read_file(local_filepath)

    # Create the database engine
    user    = os.getenv('POSTGRES_USER')
    pwd     = os.getenv('POSTGRES_PASSWORD')
    host    = os.getenv('POSTGRES_HOST')
    port    = os.getenv('POSTGRES_PORT')
    db      = os.getenv('POSTGRES_DB')
    schema  = os.getenv('POSTGRES_SCHEMA')

    engine = create_engine(f'postgresql://{user}:{pwd}@{host}:{port:04s}/{db}')

    # Connect to the database
    with engine.connect() as conn:

        # Ingest the data into the database
        data_shape = '({} x {})'.format(*data.shape)
        logging.info(f'Uploading {data_shape} data into {db}.{schema}.{table_name} in postgres...')
        data.to_sql(table_name, con=conn, schema=schema,
                    if_exists='replace', index=False,
                    method='multi', chunksize=10_000)
        logging.info(f'Uploaded {len(data)} rows into {table_name} to database.')
    
def upload_to_postgres(local_filepath: str, table_name: str, schema: str = 'dev'):
    if local_filepath.endswith('.nc'):              # netcdf
        ingest_netcdf_to_postgres(local_filepath, table_name, schema)
    elif local_filepath.endswith('.csv'):             # csv
        ingest_csv_to_postgres(local_filepath, table_name, schema)
    elif local_filepath.endswith('.zip'):             # shapefile
        ingest_shapefile_to_postgres(local_filepath, table_name, schema)
    else:
        raise ValueError('local_filepath must be a netcdf, csv, or shapefile file')
    

def upload_to_bigquery():
    pass

def load_dataset(local_filepath: str, table_name: str):
    # Upload to GCS
    upload_to_gcs()

    # Upload to PostgreSQL
    # logging(f'{local_filepath = }\t{table_name = }')
    upload_to_postgres(local_filepath=local_filepath, table_name=table_name)

    # Upload to BigQuery
    pass
