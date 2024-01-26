import zipfile

# Transform Function
def transform_dataset(local_filepath: str) -> str:
    """
    Preprocess the data file.
    """
    # Decompress the file
    with zipfile.ZipFile(local_filepath, 'r') as zip_ref:
        zip_ref.extractall(local_filepath.replace('.zip', ''))

    return local_filepath
