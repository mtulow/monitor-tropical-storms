import os
import ee
import geemap
from IPython.display import display
# import folium


# Initialize the Earth Engine API.
geemap.ee_initialize()
# ee.Initialize()

# Set the date range.
start_date = '2023-01-01'
end_date = '2023-01-02'

# Select the bands.
rgb_bands = ['sur_refl_b01', 'sur_refl_b04', 'sur_refl_b03']

# Define the visualization parameters.
rgb_vis = {
    'min': -100.0,
    'max': 8000.0,
}

# Define the area of interest.
geom = ee.Geometry.Polygon(
    [
        [
            [-115.413031, 35.889467],
            [-115.413031, 36.543157],
            [-114.034328, 36.543157],
            [-114.034328, 35.889467],
            [-115.413031, 35.889467],
        ]
    ]
)
roi = ee.Feature(geom, {}).geometry()

# Define the image collection.
rgb = ee.ImageCollection('MODIS/061/MOD09GA')\
    .filter(ee.Filter.date(start_date, end_date))\
    .select(rgb_bands)

# Clip the image collection to the area of interest.
rgb.clip(roi).unmask()

# Create 
out_dir = os.path.join(os.getcwd(), 'data', 'MODIS', 'Terra')
os.makedirs(out_dir, exist_ok=True)

geemap.ee_export_image_collection(rgb, out_dir=out_dir)

# # Set the map center and add the layer to the map.
# m = geemap.Map(center=[-7.03125, 31.0529339857], zoom=2)
# m.addLayer(rgb, rgb_vis, 'True Color (143)')
# display(m)
