import subprocess
import pandas as pd
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions




# Write a driver function that will run the pipeline.
def run():

    # 
    with beam.Pipeline() as pipeline:

        # https://www.ncei.noaa.gov/data/international-best-track-archive-for-climate-stewardship-ibtracs/v04r00/access/shapefile/IBTrACS.ACTIVE.list.v04r00.points.zip


        ## Read file from URL
        # url = https://www.ncei.noaa.gov/data/international-best-track-archive-for-climate-stewardship-ibtracs/v04r00/access/shapefile/IBTrACS.since1980.list.v04r00.points.zip
        # url = https://www.ncei.noaa.gov/data/international-best-track-archive-for-climate-stewardship-ibtracs/v04r00/access/shapefile/IBTrACS.ACTIVE.list.v04r00.points.zip
        pass





if __name__ == '__main__':
    print()
    run()
    print()