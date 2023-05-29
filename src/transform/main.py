from __future__ import annotations

import argparse
import logging
import json
import csv
import io
import zipfile
import codecs
import re
import hashlib

from datetime   import time, timedelta, datetime
from dateutil.parser import parse, ParserError
from typing     import List, Dict, Iterable
from pprint     import pprint
from numpy      import round
from ast        import literal_eval

import apache_beam as beam

from apache_beam        import PCollection
from apache_beam.pvalue import PBegin

from apache_beam.io import ReadFromText, ReadFromCsv
from apache_beam.io import WriteToText, WriteToBigQuery
from apache_beam.io.filesystems import FileSystems
from apache_beam.io.filesystem  import CompressedFile
from apache_beam.io.fileio      import MatchFiles, ReadMatches
from apache_beam.transforms.deduplicate import Deduplicate, DeduplicatePerKey
from apache_beam.dataframe.transforms   import DataframeTransform

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

with open("./src/metadata.json") as f: 
    META = json.load(f)

NOW = datetime.utcnow()
STAGE_NAME = 'transform'

OTYPE = META['staging']['type']
DELIMITER = META['staging']['delimiter']
HEADER = list(META['raw']['schema'])

logger = logging.getLogger(__name__)


datetime_cols = list(filter(lambda s: s.endswith('datetime'), HEADER))
geom_cols = [_ for _ in HEADER if _.endswith('itude')]
ids_cols = list(filter(lambda s: s.endswith('ID') or s.endswith('flag'), HEADER))
num_cols = list(set(HEADER).difference(set(datetime_cols+geom_cols+ids_cols)))

# ------------------------------------------------------------------------------

def print_row_on_error(func):
    def wrap(row):
        try:
            return func(row)
        except:
            logger.error(f'error proces row: {row}')

            raise
    return wrap

# -------------------------------------------------------------
# -----------------

# NOTE: not used; the idea was to create a hash with each row values and 
# deduplicate same values. But since it is paralell processing, it is not efficient
# although having the hash in the datase it probably usefull to query dedups
@beam.ptransform_fn
@beam.typehints.with_input_types(PCollection)
@beam.typehints.with_output_types(Dict[str,str])
def DeduplicateRows(pcol:PCollection) -> PCollection[Dict[str,str]]:
    """ First step from the Extraction Process : given a gs bucket glob string
    constructor and the number of months to process, reads all data within the
    compressed csv files.
    """
    geom_cols = [_ for _ in HEADER if _.endswith('itude')]
    dtime_cols= [_ for _ in HEADER if _.endswith('datetime')]
    hash_cols = geom_cols + dtime_cols + [
        'passenger_count', 'trip_distance', 'fare_amount', 'VendorID'
    ]

    def create_hash(row):

        filtered_row = "".join((row[k] for k in hash_cols)).encode('utf-8')
        row.update(
            {'hash': hashlib.shake_128(filtered_row).hexdigest(6)}
        )
        return row

    def tuple_to_dict(tuple):
        # todo
        pass

    return (
        pcol
        | "Create Hash column" >> beam.Map(create_hash)
        | 'Group by hash' >> beam.GroupBy(hash=lambda row: row['hash'])
        | 'Sample 1' >> beam.combiners.Sample.FixedSizeGlobally(1)
        # | 'Print' >> beam.Map(pprint)
        | "Dump" >> beam.io.WriteToText(
            'ho.txt'
        )
    )


@beam.ptransform_fn
@beam.typehints.with_input_types(PCollection)
@beam.typehints.with_output_types(Dict[str,str])
def ParseAndValidateDatetime(pcol:PCollection) -> PCollection[Dict[str,str]]:
    """ Basic transformation from raw data: Not store data we cannot do basic
    analytics over it.
    """

    
    def str_to_datetime(row):
        for c in datetime_cols:
            row[c] = datetime.strptime(row[c], META['raw']['dateformat'])
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
    )


@beam.ptransform_fn
@beam.typehints.with_output_types(PCollection[Dict[str, str]])
def ParseAndValidateGeometry(pcol:PCollection) -> PCollection[Dict[str,str]]:
    
    wkt_geopoint = 'POINT({x} {y})'

    HEADER =
    datetime_cols =
    
    def create_WKTPoints(_type):
        def wrap(row):
            cols = list(filter(lambda c: c.startswith(_type), geom_cols))
            row[f"{_type}_geom"] = wkt_geopoint.format(
                x = str(row[list(filter(lambda c: c.endswith('longitude'), cols))[0]]),
                y = str(row[list(filter(lambda c: c.endswith('latitude'), cols))[0]])
            )
            return row
        return wrap

    @print_row_on_error
    def filter_by_precision(decimal:str='.', min_precision:int=5): 
        """ Since the data is in a degree-based coordinate system (EPSG:4326), it is
        very sensible to precision problems based on the lack of decimals.

        Info about EPSG:4326:  https://epsg.io/4326
        https://wiki.openstreetmap.org/wiki/Precision_of_coordinates

        In the link above it is state that aprox 5 decimals are needed to get a 
        precision of about 1 m. And about 0.1 arc secs are needed to get a precision
        of about 3m.
        """
        def wrap(row):
            try:
                if any(row[col][::-1].find(decimal) <= min_precision for col in geom_cols):
                    return True
            except:
                return False
            
        return wrap
    
    @print_row_on_error
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

        for col in geom_cols:
            row[col] = _round(row[col], 6)
        return row
        
    
    return (
        pcol             
            | "Filter geoms with not enough precision" >> beam.Filter(filter_by_precision)
            | "Parse coords to float" >> beam.Map(parse_coords)
            | "Create WKT for pickup" >> beam.Map(create_WKTPoints('pickup'))
            | "Create WKT for dropoff" >> beam.Map(create_WKTPoints('dropoff'))
    )


@beam.ptransform_fn
@beam.typehints.with_input_types(PCollection)
@beam.typehints.with_output_types(Dict[str,str])
def ParseAndMapNumbers(pcol:PCollection) -> Dict[str,str]:


    
    def map_key(_key):
        def wrap(row):
            row.update({_key: META[_key][row[_key]]})
            return row
        return wrap
    
    def eval_nums(row):
        row.update({k: literal_eval(row[k]) for k in num_cols})
        return row

    for k in ids_cols:
        ( pcol | f"Map {k} values" >> beam.Map(map_key(k)))

    return ( pcol | "Parse numeric columns" >> beam.Map(eval_nums) )


# ------------------------------------------------------------------------------


def parse_args():
    """ Definition of pipeline arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest = 'input',
        help = 'Input bucket storage as glob pattern',
        type = str,
        default = '{source_name}/{stage_name}/{process_date}/{source_name}_2015-04*.parquet'.format(
            source_name=META['source_name'],
            stage_name='raw',
            process_date=NOW.strftime(META['staging']['dateisoformat'])
        )
    )
    parser.add_argument(
        '--output',
        dest = 'output',
        help = 'Output file to process',
        type = str,
        # default = "gs://qwiklabs-gcp-02-8bb69c7551a2/RES/result.csv"  # to bucket (then connection)
        # default = '{0}:logs'.format('qwiklabs-gcp-02-8bb69c7551a2')   # to GCP BQ
        default = '{source_name}/{stage_name}/{source_name}'.format(
            source_name=META['source_name'],
            stage_name='transform'
        )
    )

    # GOOGLE CLOUD PLATFORM ARGUMENTS
    # parser.add_argument(
    #     '--project',
    #     help='Specify Google Cloud project', 
    #     default = 'qwiklabs-gcp-02-8bb69c7551a2'
    # )
    # parser.add_argument(
    #     '--region', 
    #     help='Specify Google Cloud region', 
    #     default = 'us-east1'
    # )
    return parser



def run(argv=None, save_main_session=False):
    """
    """
    parser = parse_args()
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as transfrom_p:

        output = (
            transfrom_p
                | 'Read CSV Lines' >> beam.io.ReadFromParquet(
                        file_pattern=known_args.input,
                        min_bundle_size=10000,
                        columns=HEADER
                    )
                | 'Datetime rows' >> ParseAndValidateDatetime()
                | 'Geom rows' >> ParseAndValidateGeometry()
                | 'IDs rows' >> ParseAndMapNumbers()
                | 'SAMPLE 1' >> beam.combiners.Sample.FixedSizeGlobally(2)
                | 'Print' >> beam.Map(pprint)
                # | "write to bq" >> WriteToBigQuery(
                #     known_args.output,
                #     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                #     write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
                #     )
        )
        

        # table_1 = {
        #     output | 
        # }
        # table_2 = (
        #     outout |
        # )

        # table_3 (
            
        # )
        print('hey')


# ------------------------------------------------------------------------------


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()


# ------------------------------------------------------------------------------
# end


