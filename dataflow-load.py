"""
ETL AS A SINGLE-FILE
--------------------------------------------------------------------------------
Etl to be deployed in dataflow. There are 3 stages defined :
    - 
    -
    - 
Each need their imports and globals, due to the parallelized nature of the job.
"""
from __future__ import annotations

import time
import json

from argparse   import ArgumentParser
from datetime   import datetime
from typing     import List
from logging    import getLogger, INFO
from typing     import List, Dict, Iterable
from pyarrow    import schema
from pprint     import pprint

import apache_beam as beam

from apache_beam        import PCollection
from apache_beam.pvalue import PBegin
from apache_beam.io     import WriteToBigQuery

from apache_beam.options.pipeline_options import PipelineOptions as POpts
from apache_beam.options.pipeline_options import SetupOptions    as SOpts
from apache_beam.options.pipeline_options import StandardOptions as StdOpts
from apache_beam.options.pipeline_options import GoogleCloudOptions as GCPOpts
from apache_beam.options.pipeline_options import WorkerOptions as WOpts

from google.cloud import storage


# ------------------------------------------------------------------------------

META={
    "head"      : "File for configuration for ETL",
    "source_name" : "yellow_tripdata",
    "staging_cols" : [
            "VendorID",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "pickup_longitude" ,
            "pickup_latitude",
            "RateCodeID",
            "store_and_fwd_flag",
            "dropoff_longitude",
            "dropoff_latitude",            
            "payment_type",
            "fare_amount",
            "extra",
            "mta_tax",
            "tip_amount",
            "tolls_amount" ,
            "improvement_surcharge",
            "total_amount",
            "pickup_geom",
            "dropoff_geom",
            'id',
        ],
    "bq_schema" : {
        "tripdata" : [
            {'name': "id",                      "type": "STRING"},
            {"name": "VendorID",                "type": "STRING"},
            {"name": "RateCodeID",              "type": "STRING"},
            {"name": "store_and_fwd_flag",      "type": "BOOL"},
            {"name": "payment_type",            "type": "STRING"},
            {"name": "tpep_pickup_datetime",    "type": "STRING"},
            {"name": "tpep_dropoff_datetime",   "type": "STRING"},
            {"name": "passenger_count",         "type": "FLOAT64"},
            {"name": "trip_distance",           "type": "FLOAT64"},
            {"name": "fare_amount",             "type": "FLOAT64"},
            {"name": "extra",                   "type": "FLOAT64"},
            {"name": "mta_tax",                 "type": "FLOAT64"},
            {"name": "tip_amount",              "type": "FLOAT64"},
            {"name": "tolls_amount",            "type": "FLOAT64"},
            {"name": "improvement_surcharge",   "type": "FLOAT64"},
            {"name": "total_amount",            "type": "FLOAT64"},
            {"name": "pickup_longitude",    "type": "FLOAT64"},
            {"name": "pickup_latitude",     "type": "FLOAT64"},
            {"name": "pickup_geom",         "type": "GEOGRAPHY"},
            {"name": "dropoff_longitude",   "type": "FLOAT64"},
            {"name": "dropoff_latitude",    "type": "FLOAT64"},
            {"name": "dropoff_geom",        "type": "GEOGRAPHY"}
        ]
    }
}


NOW = datetime.utcnow()
logger = getLogger(__name__)

# ------------------------------------------------------------------------------

def fill_nan(schema):
    nan_dtype = {
        'FLOAT64': 0,
        'BOOL': False,
        'STRING': ':',
        "GEOGRAPHY": 'POINT(0 0)'
    }

    def wrap(row):
        out = {}
        for col in schema:
            if col['name'] in row.keys():
                out[col['name']] = row[col['name']]
            else:
                out[col['name']] = nan_dtype[col['type']]
        return out
    return wrap

def get_parser():
    """ Definition of pipeline arguments to pass at terminal. There are two 
    """
    parser = ArgumentParser()

    # FILES AND FILTER BY DATES
    parser.add_argument(
        '--input', required=True, help='Input bucket storage as glob pattern'
    ) #'gs://${PROJECT}/raw/${DATASETNAME}_{date}*.csv'

    parser.add_argument(
        '--output', required=True, help='Output dataset to dump tables'
    ) # gs://${PROJECT}/dataflow/${DATASETNAME}_{date}
    parser.add_argument(
        "--bq_temp",
        help="temp folder for json files when creating temp table",
        default = "gs://${PROJECT}/bq_temp/"
    )
    parser.add_argument(
        "--date",
        help = 'YYYY-MM strings with the monthly data to process joined by pipe "|"',
        default = '2015-07', type=str
    )

    # GOOGLE CLOUD PLATFORM ARGUMENTS
    parser.add_argument('--project',required=True, help='Specify Google Cloud project')
    parser.add_argument('--region', required=True, help='Specify Google Cloud region')
    parser.add_argument('--runner',required=True,  help='Specify Google Cloud runner')
    parser.add_argument('--setup_file', help='Path to setup.py')
    parser.add_argument('--num_workers', required=True, help='Num of CE instances')

    # CUSTOM APACHE BEAM WITH FILES
    #parser.add_argument(
    # '--sdk_container_image', required=True, help='URI for image in artifact-registry'
    # )

    return parser


def get_beam_option(known_args, pipeline_args, save_main_session):

    _options = POpts(pipeline_args)

    _options.view_as(SOpts).save_main_session = save_main_session
    _options.view_as(StdOpts).runner = known_args.runner
    _options.view_as(WOpts).num_workers = int(known_args.num_workers)

    _options.view_as(GCPOpts).project = known_args.project    
    _options.view_as(GCPOpts).region = known_args.region
    _options.view_as(GCPOpts).staging_location = f'gs://{known_args.project}/staging/'
    _options.view_as(GCPOpts).temp_location = f'gs://{known_args.project}/tmp/'
    _options.view_as(GCPOpts).job_name = '{0}{1}'.format(
        META['source_name'].replace('_',''), 
        time.time_ns()
    )  

    return _options


# ------------------------------------------------------------------------------


def run(known_args, pipeline_args, save_main_session):
    """
    """
    pipeline_options = get_beam_option(
        known_args=known_args, 
        pipeline_args=pipeline_args, 
        save_main_session=save_main_session
    )

    with beam.Pipeline(options=pipeline_options) as extract_p:

        tables = META['bq_schema']
        columns = META['staging_cols']

        rows = (
            extract_p 
            | "Load parquet" >> beam.io.ReadFromParquet(
                    file_pattern=known_args.input.format(date=known_args.date),
                    columns=columns
                )
        ) # list of string blob patterns to processs

        table_name = 'tripdata'
        tripdata_bq = (
            rows
                | "fillnan data" >> beam.Map(fill_nan(schema=tables[table_name]))
                | "tripdata data to bq" >> WriteToBigQuery(
                        known_args.output+'.'+table_name,
                        schema={'fields': tables[table_name]},
                        # Creates the table in BigQuery if it does not yet exist.
                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                        # Deletes all data in the BigQuery table before writing.
                        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                        custom_gcs_temp_location="gs://graphite-bliss-388109/tmp/bq"
                    )
        )


def main(argv=None, save_main_session=False):
    """ Defines the argument parser, parses them, configures logging and 
    activates the extract-run method of the extraction etl.
    """

    parser = get_parser()
    known_args, pipeline_args = parser.parse_known_args(argv)

    run(
        known_args=known_args, 
        pipeline_args=pipeline_args, 
        save_main_session=save_main_session
    )


# ------------------------------------------------------------------------------

if __name__ == '__main__':
    getLogger().setLevel(INFO)
    main()


# ------------------------------------------------------------------------------
# end