# Import the necessary modules
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery


def run():
    # Define the pipeline options
    options = PipelineOptions(
        project='YOUR_PROJECT_ID',
        runner='DataflowRunner',
        temp_location='gs://YOUR_BUCKET_NAME/temp',
        region='YOUR_REGION',
        job_name='YOUR_JOB_NAME'
    )

    # Create the pipeline object
    p = beam.Pipeline(options=options)

    # Read the data from the source
    source_data = p | 'Read from source' >> beam.io.ReadFromText('gs://YOUR_BUCKET_NAME/source_data.csv')

    # Apply any transformations to the data
    transformed_data = source_data | 'Transform data' >> beam.Map(lambda x: x.upper())

    # Write the data to the BigQuery table
    transformed_data | 'Write to BigQuery' >> WriteToBigQuery(
        table='YOUR_TABLE_ID',
        dataset='YOUR_DATASET_ID',
        schema='name:STRING,age:INTEGER',
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
    )

    # Run the pipeline
    p.run()

