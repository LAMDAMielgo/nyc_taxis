"""
Entrypoint for the extraction ETL
"""
from __future__ import annotations

from argparse   import ArgumentParser
from typing     import List
from logging    import getLogger, INFO


# ------------------------------------------------------------------------------


def get_parser():
    """ Definition of pipeline arguments
    """
    parser = ArgumentParser()
    parser.add_argument(
        '--input',
        dest = 'input',
        help = 'Input bucket storage as glob pattern',
        type = str,
        # default = 'sample_data/*00.csv.zip' # for local tests
        # default = 'gs://data_eng_test/*.csv.zip'
    )
    parser.add_argument(
        '--output',
        dest = 'output',
        help = 'Output file to process',
        type = str,
        # default = '{0}:logs'.format('qwiklabs-gcp-02-8bb69c7551a2')   # to GCP BQ
        default = 'gs://{PROJECT}/dataflow/{source_name}/{stage_name}/{process_date}/{source_name}_{date}'
    )
    parser.add_argument(
        "--date",
        dest = 'date',
        help = 'YYYY-MM strings with the monthly data to process joined by pipe "|"',
        type = str,
        default = '2015-01'
    )

    # GOOGLE CLOUD PLATFORM ARGUMENTS
    parser.add_argument('--project',required=True, help='Specify Google Cloud project')
    parser.add_argument('--region', required=True, help='Specify Google Cloud region')
    parser.add_argument('--runner',required=True,  help='Specify Google Cloud region')
    parser.add_argument('--setup_file', help='Path to setup.py')
    parser.add_argument('--sdk_container_image', required=True, help='URI for custom image located in artifact-registry')
    return parser


def main(argv=None, save_main_session=False):
    """ Defines the argument parser, parses them, configures logging and 
    activates the extract-run method of the extraction etl.
    """

    from extract.main import run

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