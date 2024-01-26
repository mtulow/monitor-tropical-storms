import os
import gcsfs
import logging
import urllib.request
from pathlib import Path
from dotenv import load_dotenv



# Logging
logging.basicConfig(level=logging.INFO)

# Project root directory
ROOT_DIR = Path(__file__).parent.parent.parent

# Load the environment variables
load_dotenv(ROOT_DIR / '.env')

GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
GCS_BUCKET = os.getenv('GCP_GCS_BUCKET')
GCP_SA_CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

class Params:
    """
    IBTrACS ingestion pipeline parameters.
    """
    def __init__(self, subset: str = 'ACTIVE', file_type: str = 'netcdf', geom_type: str = None):
        # Given arguments
        self.subset = subset
        self.file_type = file_type
        self.geom_type = geom_type
        
        # Constructed arguments
        self.url = self._get_url()
        self.local_filepath = self._get_local_filepath(local_dir=f'data/IBTrACS/{file_type}')
        self.remote_filepath = self._get_remote_filepath(remote_dir=f'gs://{GCS_BUCKET}/ibtracs/{file_type}')

    @property
    def _get_subsets(cls):
        """
        Get the available subsets of the IBTrACS dataset; options are:
        - `ACTIVE`
        - `ALL`         
        - `NA`              # North Atlantic
        - `SA`              # South Atlantic
        - `NI`              # North Indian
        - `SI`              # South Indian
        - `EP`              # Eastern Pacific
        - `SP`              # South Pacific
        - `WP`              # Western Pacific
        - `last3years`
        - `since1980`
        """
        subsets = (
            'ACTIVE','ALL', 'NA', 'SA', 'NI', 'SI', 'EP', 'SP', 'WP',
            'last3years', 'since1980',)
        return subsets

    def _get_server(self):
        """
        Get the IBTrACS base URL for the given file type.
        """
        # Available file types
        if self.file_type not in ['netcdf', 'csv', 'shapefile']:
            raise ValueError('file_type must be one of: `netcdf`, `csv`, `shapefile`')
        # Set the IBTrACS server
        server = ('https://www.ncei.noaa.gov/data/'
                'international-best-track-archive-for-climate-stewardship-ibtracs/'
                f'v04r00/access/{self.file_type}/')
        return server
    
    def _get_filename(self):
        """
        Get the filename for the given file type and subset.
        """
        # NetCDF file name
        if self.file_type == 'netcdf':
            filename = f'IBTrACS.{self.subset}.v04r00.nc'
        # CSV or text file name
        elif self.file_type in ('csv', 'txt', 'text'):
            filename = f'ibtracs.{self.subset}.list.v04r00.csv'
        # Shapefile name
        elif self.file_type == 'shapefile':
            # Shapefile by vector type
            if self.geom_type is not None:
                filename = f'IBTrACS.{self.subset}.list.v04r00.{self.geom_type.lower()}.zip'
            else:
                raise ValueError('geom_type must be one of: `points` or `lines`')
        
        return filename
    
    def _get_local_filepath(self, local_dir: str = None):
        """
        Get the local file path for the given file type and subset.
        """
        # Get the file name
        filename = self._get_filename()
        
        # Create the local file path
        if local_dir:
            # Get the root directory
            root = Path(__file__).parent.parent.parent
            # Get the data directory
            data_dir = os.path.join(root, local_dir)
            # Create the data directory if it does not exist
            os.makedirs(data_dir, exist_ok=True)
            # Get the local file path
            local_filepath = os.path.join(data_dir, filename)
        
        else:
            # Get the local file path
            local_filepath = filename
        
        return local_filepath
    
    def _get_remote_filepath(self, remote_dir: str = None):
        """
        Get the remote file path for the given file type and subset.
        """
        # Get the file name
        filename = self._get_filename()
        
        # Create the remote file path
        if remote_dir:
            # Create the remote file system on gcs
            fs = gcsfs.GCSFileSystem(project=GCP_PROJECT_ID,
                                    #  bucket=GCS_BUCKET,
                                     token=GCP_SA_CREDENTIALS,)
            # Create the remote directory if it does not exist
            fs.mkdir(f"gs://{GCS_BUCKET}", remote_dir)
            # Get the remote file path
            remote_filepath = os.path.join(remote_dir, filename)
        else:
            # Get the remote file path
            remote_filepath = filename
        
        return remote_filepath
    
    def _get_url(self) -> str:
        """
        Get the URL for the given file type and subset.
        """
        return self._get_server() + self._get_filename()
    
    def __str__(self) -> str:
        return f'IBTrACS File: {self._get_filename()}'
   
    def __repr__(self) -> str:
        return f'IBTrACS File: {self._get_filename()}'



# Extract Function
def extract_dataset(file_type: str, geom_type: str, subset: str = 'ACTIVE',
                    local_dir: str = None, remote_dir: str = None,
    ) -> tuple[str, str]:
    """
    Extract data from the IBTrACS website.

    Args:
        subset (str): Subset of the IBTrACS dataset, valid options are: 
            `ACTIVE`, `ALL`, `NA`, `SA`, `NI`, `SI`, `EP`, `SP`, `WP`, `last3years`, `since1980`.
        file_type (str): File type to download, valid options are: `netcdf`, `csv`, `shapefile`.
        geom_type (str): Vector type to download, valid options are: `points` or `lines`.

    Returns:
        tuple: Tuple containing the local and remote file paths.
    """
    # Create a IBTrACS pipeline argument container object
    params = Params(subset, file_type, geom_type)

    # Get the url
    url = params.url
    
    # Get the local file path
    local_filepath = params.local_filepath
    
    # Get the remote file path
    remote_filepath = params.remote_filepath

    # Download the data
    try:
        # Fetching the contents of the url, store in local file
        logging.info(f'Downloading {local_filepath} ...')
        urllib.request.urlretrieve(url, local_filepath)
        logging.info(f'Downloaded {params.file_type} to {local_filepath}')    

    except Exception as err:
        print ("Error:",err)
        return None
    
    return local_filepath, remote_filepath

