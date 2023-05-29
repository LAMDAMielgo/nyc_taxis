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
    "source_name"   : "yellow_tripdata",
    "key" : {
        "payment_type" : {
            "1" : "credit_card",
            "2" : "cash",
            "3" : "no_charge",
            "4" : "dispute",
            "5" : "unknown",
            "6" : "voided trip"
        },
        "RateCodeID" : {
            "1" : "standard_rate",
            "2" : "jfk",
            "3" : "newark",
            "4" : "nassau_or_westchester",
            "5" : "negotiated_fare",
            "6" : "group_fare"
        },
        "VendorID" : {
            "1" : "Creative Mobile Technologies, LLC.",
            "2" : "VeriFone Inc."
        },
        "store_and_fwd_flag": {
            "Y" : True, "N": False
        }
    },
    "raw" : {
        "encoding"  : "utf-8",
        "delimiter" : "\t",
        "schema"    : {
            "VendorID"              : { "dtype": "int",       "comment" : "TPEP Provider"},  
            "tpep_pickup_datetime"  : { "dtype": "datetime",  "comment" : ""},
            "tpep_dropoff_datetime" : { "dtype": "datetime",  "comment" : ""},
            "passenger_count"       : { "dtype": "int",       "comment" : "Num passenger per ride"},
            "trip_distance"         : { "dtype": "float",     "comment" : "miles, by taximeter"},
            "pickup_longitude"      : { "dtype": "float",     "comment" : "x geom"},
            "pickup_latitude"       : { "dtype": "float",     "comment" : "y geom"},
            "RateCodeID"            : { "dtype": "int",       "comment" : "usd/meter applied"},
            "store_and_fwd_flag"    : { "dtype": "bool",      "comment" : "type of data processing from device"},
            "dropoff_longitude"     : { "dtype": "float",     "comment" : ""},
            "dropoff_latitude"      : { "dtype": "float",     "comment" : ""},
            "payment_type"          : { "dtype": "int",       "comment" : ""},
            "fare_amount"           : { "dtype": "float",     "comment" : "usd/fare"},
            "extra"                 : { "dtype": "float",     "comment" : "usd"},
            "mta_tax"               : { "dtype": "float",     "comment" : "usd, cte"},
            "tip_amount"            : { "dtype": "float",     "comment" : "usd"},
            "tolls_amount"          : { "dtype": "float",     "comment" : "usd"},
            "improvement_surcharge" : { "dtype": "float",     "comment" : "usd"},
            "total_amount"          : { "dtype": "float",     "comment" : "usd"}
        },
       "dateformat" : "%Y-%m-%d %H:%M:%S"
    },
    "staging" : {
        "delimiter" : "|",
        "type"      : "str",
        "bq_schema" : [
            {"name": "VendorID",                "type": "STRING"},
            {"name": "RateCodeID",              "type": "STRING"},
            {"name": "store_and_fwd_flag",      "type": "STRING"},
            {"name": "tpep_pickup_datetime",    "type": "STRING"},
            {"name": "tpep_dropoff_datetime",   "type": "STRING"},
            {"name": "payment_type",        "type": "STRING"},
            {"name": "passenger_count",     "type": "FLOAT"},
            {"name": "trip_distance",       "type": "FLOAT"},            
            {"name": "fare_amount",         "type": "FLOAT"},
            {"name": "extra",               "type": "FLOAT"},
            {"name": "mta_tax",             "type": "FLOAT"},
            {"name": "tip_amount",          "type": "FLOAT"},
            {"name": "tolls_amount",            "type": "FLOAT"},
            {"name": "improvement_surcharge",   "type": "FLOAT"},
            {"name": "total_amount",         "type": "FLOAT"},
            {"name": "pickup_longitude",    "type": "FLOAT"},
            {"name": "pickup_latitude",     "type": "FLOAT"},
            {"name": "pickup_geom",         "type": "GEOGRAPHY"},
            {"name": "dropoff_longitude",   "type": "FLOAT"},
            {"name": "dropoff_latitude",    "type": "FLOAT"},
            {"name": "dropoff_geom",        "type": "GEOGRAPHY"},
        ]
    }
}


NOW = datetime.utcnow()
logger = getLogger(__name__)


def print_row_on_error(func):
    def wrap(row):
        try:
            return func(row)
        except:
            logger.error(f'error proces row: {row}')

            raise
    return wrap

# ------------------------------------------------------------------------------

@beam.ptransform_fn
@beam.typehints.with_input_types(beam.pvalue.PBegin)
@beam.typehints.with_output_types(List[str])
def ReadCSVLines(pbegin:PBegin, fpattern:str) -> List[str]:
    """ Given a file pattern a date and a month to process, groups all the steps
    to have each line on memory.
    """
    import csv
    import io
    from dateutil.parser import parse, ParserError
    from datetime        import datetime
    from apache_beam.io.filesystems import FileSystems

    HEADER = list(META['raw']['schema'].keys())
    ENCODING = META['raw']['encoding']
    DELIMITER = META['raw']['delimiter']

    def expand_pattern(fpattern: str) -> Iterable[str]:
        """ Yields all fields that match every file pattern
        """
        for match_result in FileSystems.match([fpattern])[0].metadata_list:
            yield match_result.path

    def read_csv_lines(fname:str) -> Iterable[Dict[str, str]]:
        with FileSystems.open(fname) as f:
            # Beam reads files as bytes, but csv expects strings,
            # so we need to decode the bytes into utf-8 strings.
            for row in csv.DictReader(
                    io.TextIOWrapper(f, ENCODING),
                    fieldnames=HEADER,
                    delimiter = DELIMITER
                ):
                yield row

    print(fpattern)
    return (
        pbegin 
            | beam.Create([fpattern])
            | 'Expand file patterns' >> beam.FlatMap(expand_pattern)
            | 'Read and parse CSV lines' >> beam.FlatMap(read_csv_lines)
    )

# ------------------------------------------------------------------------------

@beam.ptransform_fn
@beam.typehints.with_output_types(PCollection[Dict[str, str]])
def ParseAndValidateGeometry(pcol:PCollection) -> PCollection[Dict[str, str]]:
    """ Basic transformation from raw data: Not store data we cannot do basic
    analytics over it.
    """
    # Variables

    HEADER = list(META['raw']['schema'])
    wkt_geopoint = 'POINT({x} {y})'
    geom_cols = list(filter(lambda s: s.endswith('itude'), HEADER))

    def create_centroid(row):
        _latitude = [row[_] for _ in geom_cols if _.endswith('latitude')]
        _longitude = [row[_] for _ in geom_cols if _.endswith('longitude')]

        row['centroid_latitude'] = sum(_latitude)/len(_latitude)
        row['centroid_longitude'] = sum(_longitude)/len(_longitude)
        return row

    def create_WKTPoints(_type):
        """ Geometries are going to be uploaded with their Well Known Text format
        https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry
        """
        def wrap(row):
            cols = list(filter(lambda c: c.startswith(_type), geom_cols))
            row[f"{_type}_geom"] = wkt_geopoint.format(
                x = str(row[list(filter(lambda c: c.endswith('longitude'), cols))[0]]),
                y = str(row[list(filter(lambda c: c.endswith('latitude'), cols))[0]])
            )
            return row
        return wrap

    def parse_coords(row):
        """ We unify the precision of all the data to avoid presition problems in 
        the transformed data.

        Info about EPSG:4326:  https://epsg.io/4326
        https://wiki.openstreetmap.org/wiki/Precision_of_coordinates

        In the link above it is state that aprox 5 decimals are needed to get a 
        precision of about 1 m. And about 0.1 arc secs are needed to get a precision
        of about 3m: 
            1 arc hour ; {"archour": 1,"arcmin" : 900, "arcsecond" : 54000}
        """
        _round = lambda i, p: round(float(i), p)
        try: 
            for col in geom_cols:
                row[col] = _round(row[col], 7)
            return row
        
        except Exception as e:
            for col in geom_cols:
                row[col] = 0
            return row  #NOTE: If exception then geometry is casted as zero
    
    def drop_notNumeric_values(row):
        try:
            return any(abs(float(row[k])) != 0 for k in geom_cols)        
        except ValueError as e:
            return False
        
    return (
        pcol
            | "Parse coords to float" >> beam.Map(parse_coords)
            | 'Drop zero geom rows' >> beam.Filter(drop_notNumeric_values)
            | "Create WKT for pickup" >> beam.Map(create_WKTPoints('pickup'))
            | "Create WKT for dropoff" >> beam.Map(create_WKTPoints('dropoff'))
            | "Create centroid" >> beam.Map(create_centroid)
            | beam.Map(lambda row: {k: str(row[k]) for k in row})
    )


@beam.ptransform_fn
@beam.typehints.with_input_types(PCollection)
@beam.typehints.with_output_types(Dict[str,str])
def ParseAndValidateDatetime(pcol:PCollection) -> PCollection[Dict[str,str]]:
    """ Basic transformation from raw data: Not store data we cannot do basic
    analytics over it.
    """
    from datetime import datetime

    HEADER = list(META['raw']['schema'])
    DTFORMAT = META['raw']['dateformat']
    datetime_cols = list(filter(lambda s: s.endswith('datetime'), HEADER))

    
    def str_to_datetime(row):
        for c in datetime_cols:
            row[c] = datetime.strptime(row[c], DTFORMAT)
        return row
    
    def dtime_to_str(row):
        for c in datetime_cols:
            row[c] = row[c].strftime(DTFORMAT)
        return row

    def validate_pickup_and_dropoff(row):
        if row['tpep_pickup_datetime'] <= row['tpep_dropoff_datetime']:
            return True
        else:
            return False

    return (
        pcol
            | 'Convert dtype' >> beam.Map(str_to_datetime)
            | 'Filter wrong tpep' >> beam.Filter(validate_pickup_and_dropoff)
            | beam.Map(dtime_to_str)

    )


# @beam.ptransform_fn
# @beam.typehints.with_output_types(PCollection[Dict[str, str]])
# def ParseAndMapNumbers(pcol:PCollection) -> PCollection[Dict[str,str]]:
    
#     def map_key(_key):
#         def wrap(row):
#             row.update({_key: META[_key][row[_key]]})
#             return row
#         return wrap
    
#     def eval_nums(row):
#         row.update({k: literal_eval(row[k]) for k in num_cols})
#         return row

#     for k in ids_cols:
#         ( pcol | f"Map {k} values" >> beam.Map(map_key(k)))

#     return ( pcol | "Parse numeric columns" >> beam.Map(eval_nums) )

# ------------------------------------------------------------------------------

def get_all_files_from_bucket(project, globpath):
    """ Through the gs client, retrieves the names of all the files to process
    """

    with storage.Client() as client:
        for blob in client.list_blobs(
            project, 
            prefix=globpath.split(project)[-1][1:]
        ):
            yield blob


def get_parser():
    """ Definition of pipeline arguments to pass at terminal. There are two 
    """
    parser = ArgumentParser()

    # FILES AND FILTER BY DATES
    parser.add_argument(
        '--input', required=True, help='Input bucket storage as glob pattern'
    ) #'gs://${PROJECT}/raw/${DATASETNAME}_{date}*.csv'

    parser.add_argument(
        '--output', required=True, help='Output file to process'
    ) # gs://${PROJECT}/dataflow/${DATASETNAME}_{date}

    parser.add_argument(
        "--date",
        help = 'YYYY-MM strings with the monthly data to process joined by pipe "|"',
        default = '2015-01', type=str
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

        lines = (
            extract_p 
            | ReadCSVLines(
                    fpattern=known_args.input.format(date=known_args.date),
                )
        ) # list of string blob patterns to processs

        output_path = known_args.output.format(date=known_args.date)

        lines_parsed = (
            lines
                | 'Parse and Validate Geometry' >> ParseAndValidateGeometry()
                | 'Parse and Validate Datetime' >> ParseAndValidateDatetime()
                # | "Write ROW to GCP BQ" >> WriteToBigQuery(
                #         output_path,
                #         schema={'fields': META['staging']['bq_schema']},
                #         # Creates the table in BigQuery if it does not yet exist.
                #         create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                #         # Deletes all data in the BigQuery table before writing.
                #         write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                #         ignore_unknown_columns=True
                #     )
                | beam.Map(lambda row: json.dumps(row))
                | 'Write to GS in JSON' >> beam.io.WriteToText(
                    output_path, '.json'
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