from __future__ import annotations

import argparse
import logging
import json
import csv
import io
import zipfile
import time

from datetime   import datetime
from dateutil.parser import parse, ParserError
from typing     import List, Dict, Iterable
from pyarrow    import schema

import apache_beam as beam

from apache_beam        import PCollection
from apache_beam.pvalue import PBegin
from apache_beam.io     import WriteToBigQuery
from apache_beam.io.filesystems import FileSystems

from apache_beam.options.pipeline_options import PipelineOptions as POpts
from apache_beam.options.pipeline_options import SetupOptions    as SOpts
from apache_beam.options.pipeline_options import StandardOptions as StdOpts
from apache_beam.options.pipeline_options import GoogleCloudOptions as GCPOpts
from apache_beam.options.pipeline_options import WorkerOptions as WOpts
from apache_beam.options.pipeline_options import DebugOptions as DOpts

from .metadata import META

NOW = datetime.utcnow()
STAGE_NAME = 'raw'

OUT_TYPE = META['staging']['type']
OUT_DELIMITER = META['staging']['delimiter']
OUT_HEADER = list(META['raw']['schema'])

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------------------


@beam.ptransform_fn
@beam.typehints.with_input_types(beam.pvalue.PBegin)
@beam.typehints.with_output_types(List[str])
def ConstructExtractionPatterns(pbegin:PBegin, fpattern:str, date_input:str|None) -> List[str]:
    """ Given a file pattern a date
    """

    # TODO: Rethink without parse
    # def validate_dateformat(date:List[str]):
    #     from dateutil.parser import parse, ParserError

    #     if isinstance(parse(date), datetime):
    #         return date
    #     else:
    #         raise ParserError(f"Wrong date input : {date}")


    def construct_patterns(fpattern) -> List[str]:
        """Updates de file pattern if a date is passed, in order to only process
        those files.
            if date is not passed : /sample_data/yellow_tripdata*.csv.zip
            if date == '2015-01'  : /sample_data/yellow_tripdata_2015-01*csv.zip   
        """
        def wrap(date):
            if date:
                return fpattern.replace('*', f"{META['source_name']}_{date}_*")
            else:
                return fpattern.replace('*', f"{META['source_name']}_*")
        return wrap

    return (
        pbegin 
            | "Read input text" >> beam.Create([date_input])
            | "Map dates" >> beam.ParDo(lambda x: x.split('|'))
            # | "Validate dates" >> beam.Map(validate_dateformat)
            | "Construct glob strings" >> beam.Map(construct_patterns(fpattern))
    )


@beam.ptransform_fn
@beam.typehints.with_output_types(Dict[str,str])
def ReadCsvFiles(pcol:PCollection) -> PCollection[Dict[str,str]]:
    """ First step from the Extraction Process : given a gs bucket glob string
    constructor and the number of months to process, reads all data within the
    compressed csv files.
    """

    def expand_pattern(fpattern: str) -> Iterable[str]:
        """ Yields all fields that match every file pattern
        """
        for match_result in FileSystems.match([fpattern])[0].metadata_list:
            yield match_result.path

    # def read_csv_lines(fname: str) -> Iterable[Dict[str, str]]:
    #     """ Yields every row from the csv contained inside every csv.zip
    #     """
    #     with zipfile.ZipFile(fname, 'r') as zipf:
    #         # there is only one file inside each csv.zip file
    #         csv_file = zipf.namelist()[0]
    #         print(csv_file)
    #         # if zipfile.is_zipfile(fname):
    #         try:
    #             with zipf.open(csv_file, 'r') as f:
    #                 # Beam reads files as bytes, but csv expects strings, 
    #                 # so we need to decode the bytes into utf-8 strings.
    #                 reader = csv.DictReader(
    #                     # codecs.iterdecode(f, META['raw']['encoding'),
    #                     io.TextIOWrapper(f, META['raw']['encoding']),
    #                     fieldnames=OUT_HEADER,
    #                     delimiter = META['raw']['delimiter']
    #                 )
    #                 for row in reader:
    #                     yield dict(row)

    #         except zipfile.BadZipFile as e:
    #             _message = "Extraction failed : {e} : {fname}"
    #             print(_message)    

    def read_csv_lines(fname:str) -> Iterable[Dict[str, str]]:
        with FileSystems.open(fname) as f:
            # Beam reads files as bytes, but csv expects strings,
            # so we need to decode the bytes into utf-8 strings.
            for row in csv.DictReader(
                    io.TextIOWrapper(f, META['raw']['encoding']),
                    fieldnames=OUT_HEADER,
                    delimiter = META['raw']['delimiter']
                ):
                yield dict(row)

    return (
        pcol
        | 'Expand file patterns' >> beam.FlatMap(expand_pattern)
        | 'Read CSV lines' >> beam.FlatMap(read_csv_lines)
    )



@beam.ptransform_fn
@beam.typehints.with_output_types(List[str])
def TransformRawData(pcol:PCollection) -> List[str]:
    """ Basic transformation from raw data: Not store data we cannot do basic
    analytics over it.
    """
    # Variables
    datetime_cols = list(filter(lambda s: s.endswith('datetime'), OUT_HEADER))
    non_null_cols = list(filter(lambda s: s.endswith('itude'), OUT_HEADER))
    non_zero_cols = non_null_cols # + ['trip_distance']
    

    def drop_nans_values(row):
        return any(row[k] is not None for k in non_null_cols+datetime_cols)
        
    def drop_nonzero_values(row):
        try:      
            return any(abs(float(row[k])) >=0.01 for k in non_zero_cols)
        except ValueError as e:
            return False

    def format_output_to_csv(row):
        out = []
        for k in row:
            if k in OUT_HEADER:
                out.append(row.get(k))

        return OUT_DELIMITER.join(out)
    
    def format_output_to_dict(row):
        out = {}
        for k in row:
            if k in OUT_HEADER:
                out[k] = row[k]
        return out
    

    return (
        pcol
            | 'Drop data with nonnull columns' >> beam.Filter(drop_nans_values)
            | 'Drop data from nonzero columns' >> beam.Filter(drop_nonzero_values)
            # | 'Format output' >> beam.Map(format_output)
    )


# ------------------------------------------------------------------------------


def get_beam_option(known_args, pipeline_args, save_main_session):

    _options = POpts(pipeline_args)

    _options.view_as(SOpts).save_main_session = save_main_session
    _options.view_as(StdOpts).runner = known_args.runner

    _options.view_as(GCPOpts).project = known_args.project
    _options.view_as(WOpts).sdk_container_image = known_args.sdk_container_image
    
    _options.view_as(GCPOpts).region = known_args.region
    _options.view_as(GCPOpts).staging_location = f'gs://{known_args.project}/staging/'
    _options.view_as(GCPOpts).temp_location = f'gs://{known_args.project}/tmp/'
    _options.view_as(GCPOpts).job_name = '{0}{1}'.format(
        META['source_name'].replace('_',''), 
        time.time_ns()
    )  

    return _options


def run(known_args, pipeline_args, save_main_session):
    """
    """
    pipeline_options = get_beam_option(
        known_args=known_args, 
        pipeline_args=pipeline_args, 
        save_main_session=save_main_session
    )


    with beam.Pipeline(options=pipeline_options) as extract_p:

        fpatterns = (
            extract_p 
                | ConstructExtractionPatterns(
                    fpattern= known_args.input, 
                    date_input=known_args.date
                    )
        ) # list of string blob patterns to processs


        output_filepath = known_args.output.format(
            PROJECT=known_args.project,
            source_name=META['source_name'],
            stage_name=STAGE_NAME,
            process_date=NOW.strftime(META['staging']['dateisoformat']),
            date=known_args.date
        )

        output = (
            fpatterns
                | 'Read files from source' >> ReadCsvFiles()
                | 'Basic data transformations' >> TransformRawData()
                | 'Write to parquet' >> beam.io.WriteToParquet(
                    file_path_prefix=output_filepath,
                    file_name_suffix='.parquet',
                    schema=schema([(col, OUT_TYPE) for col in OUT_HEADER]),
                    row_group_buffer_size=209715200  # 200 MB
                )
                # | "write to bq" >> WriteToBigQuery(
                #     known_args.output,
                #     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                #     write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
                #     )
        )

# ------------------------------------------------------------------------------
# end

