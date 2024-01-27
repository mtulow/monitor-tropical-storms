# Import packages
import s3fs
import gdal
import xarray as xr
import matplotlib.pyplot as plt

# Connect to S3 bucket
fs = s3fs.S3FileSystem(anon=True, client_kwargs={
    'endpoint_url': 'https://climate.uiogeo-apps.sigma2.no/'
})

# Specify the file name and path
file_name = 'AQUA_MODIS.20231115.L3m.DAY.PIC.pic.4km.NRT.nc'
file_path = 'ESGF/obs4MIPs/MODIS/MODIS6.1terra/' + file_name

# Open the file as a GDAL dataset
ds = gdal.Open('/vsis3/' + file_path, gdal.GA_ReadOnly, options=['AWS_S3_ENDPOINT=climate.uiogeo-apps.sigma2.no', 'AWS_VIRTUAL_HOSTING=no'])

# Read the first band as a numpy array
band = ds.GetRasterBand(1)
data = band.ReadAsArray()

# Close the dataset
ds = None

# Plot the data using Matplotlib
plt.imshow(data, cmap='viridis')
plt.colorbar()
plt.title(file_name)
plt.show()
