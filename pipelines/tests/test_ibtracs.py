import os
import sys

# IBTrACS is not installed, so add the parent directory to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from ibtracs import elt
from itertools import product
from collections import defaultdict


def test_ibtracs_data_files():
    # File types
    file_types = ('netcdf','csv','shapefile')

    # Subset
    subsets = (
            'ACTIVE','NA','SA','NI','SI','EP','SP','WP',
            'last3years',)
    
    # Geometry data types
    geom_types = ('points','lines')

    # Test all combinations, store the filepaths
    all_filepaths = defaultdict(str)
    for file_type, subset, geom_type in product(file_types, subsets, geom_types):
        
        # Download the data file
        filepaths = elt.extract_data(subset=subset, file_type=file_type, geom_type=geom_type)

        
    # Print the filepaths, collect the failed downloads
    failed = []
    for k, v in filepaths.items():
        print()
        print(k)
        print('  -', v)
        if not v:
            failed.append(k)
        
    # Assert that all files were downloaded
    assert all(filepaths.values()), 'Failed to download: {}'.format(', '.join(failed))


if __name__ == '__main__':
    print()
    test_ibtracs_data_files()
    print()