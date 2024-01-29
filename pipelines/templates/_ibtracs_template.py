

def run(pipeline_args, known_args):
    import apache_beam as beam
    from apache_beam.io.gcp.internal.clients import bigquery as beam_bigquery
    from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
    from geobeam.io import ShapefileSource
    from geobeam.fn import format_record, make_valid, filter_invalid


    # Define pipeline options
    pipeline_options = PipelineOptions([
        '--experiments', 'use_beam_bq_sink',
    ] + pipeline_args)

    # Create a pipeline
    with beam.Pipeline(options=pipeline_options) as pipeline:

        p = (pipeline
            | 'Read IBTrACS Points' >> ShapefileSource(
                file_pattern=known_args.url,
                layer_name=known_args.layer,
                fields=known_args.fields,
                in_epsg=4326,)
            | 'Make Valid' >> beam.Map(make_valid)
            | 'Filter Invalid' >> beam.Filter(filter_invalid)
            | 'Format Records' >> beam.Map(format_record)
            | 'Write To BigQuery' >> beam.io.WriteToBigQuery(
             beam_bigquery.TableReference(
                 datasetId=known_args.dataset,
                 tableId=known_args.table),
             method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
             write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
             create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER))



if __name__ == '__main__':
    print()

    import logging
    import argparse

    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument('--gcs_url')
    parser.add_argument('--dataset')
    parser.add_argument('--table')
    parser.add_argument('--layer_name')
    parser.add_argument('--in_epsg', type=int, default=None)
    known_args, pipeline_args = parser.parse_known_args()

    run(pipeline_args, known_args)

    print()