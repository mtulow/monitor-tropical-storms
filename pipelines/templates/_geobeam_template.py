# import apache_beam as beam
# from geobeam.io import earthengine as eeio
# from geobeam.io import bigquery as bqio

# # Red Wavelength: 625–740 nm
# # Green Wavelength: 495–570 nm
# # Blue Wavelength: 450–495 nm


# # sur_refl_b01      620-670nm       Surface reflectance for band 1  (Red)
# # sur_refl_b02      841-876nm       Surface reflectance for band 2
# # sur_refl_b03      459-479nm       Surface reflectance for band 3  (blue)
# # sur_refl_b04      545-565nm       Surface reflectance for band 4  (green)
# # sur_refl_b05      1230-1250nm     Surface reflectance for band 5
# # sur_refl_b06      1628-1652nm     Surface reflectance for band 6
# # sur_refl_b07      2105-2155nm     Surface reflectance for band 7


# def main():
#     """Main Entry Point; defines and runs the ELT pipeline."""
#     import argparse
#     import logging

#     logging.getLogger().setLevel(logging.INFO)
#     logging.info("Starting pipeline.")

#     # Parse arguments from the command line
#     parser = argparse.ArgumentParser()
#     parser.add_argument(
#         "--output",
#         dest="output",
#         required=True,
#         help="Output BigQuery table for results specified as: PROJECT:DATASET.TABLE or DATASET.TABLE.",
#     )
#     args, pipeline_args = parser.parse_known_args()


#     # Define the pipeline
#     with beam.Pipeline() as pipeline:
        
#         (
#             p
#             | 'Read from Earth Engine' >> eeio.ReadFromEarthEngine(
#                 collection="MODIS/006/MOD09GA",
#                 start_date="2023-01-01",
#                 end_date="2023-01-12",
#                 bands=["sur_refl_b01", "sur_refl_b04", "sur_refl_b03"],
#                 scale=10,
#                 # region=eeio.geometry.Rectangle(2.0, 48.0, 3.0, 49.0),
#             )
#             | 'Display' >> beam.Map(print)s
#         )



var dataset = ee.ImageCollection('MODIS/061/MOD09GA')
                  .filter(ee.Filter.date('2018-04-01', '2018-06-01'));
var trueColor143 =
    dataset.select(['sur_refl_b01', 'sur_refl_b04', 'sur_refl_b03']);
var trueColor143Vis = {
  min: -100.0,
  max: 8000.0,
};
Map.setCenter(-7.03125, 31.0529339857, 2);
Map.addLayer(trueColor143, trueColor143Vis, 'True Color (143)');