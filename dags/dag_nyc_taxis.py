import os

from datetime import datetime, timedelta
from dataclasses import dataclass
from google.cloud import storage

from airflow.models import Variable

from airflow import DAG
from airflow import models
from airflow.operators.python   import PythonOperator


from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryGetDatasetOperator,
    BigQueryUpdateDatasetOperator
)
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowConfiguration,
    DataflowCreatePythonJobOperator,
    DataflowTemplatedJobStartOperator,
    DataflowJobCompleteSensor
)

# In the test promp, it is said that the data is updated monthly, 
# therefore in the wild this should be a monthly triggered DAG
# algthough for the purposes of this test it is going to be manual
# with fake datetimes
NOW = datetime(2015, 1, 10)
# dag_interval = "@monthly" 

_PROJECT = 'graphite-bliss-388109'
_REGION =  'europe-southwest1'
_DATASETNAME = 'yellow_tripdata'
_CODEHOME='/home/laura/Pruebas/nyc_taxis'

default_args = {
    'owner': 'lamdam',
    'depends_on_past': False,
    'retries': 0
}
dag_desc = """Dag to get all the data from the NYC Taxis Source. This
data is updated monthly and has N amount of parts."""
bq_datasets = {
    'tmp': 'tripdata_{date}_tmp', 
    'apppend': ['tripdata', 'pickup', 'dropdown']
}


DAG_PARAMS={
    'source_uri'    : "gs://data_eng_test/",
    'raw_path'      : 'gs://{project}/{dataset}/{date}/',
    'staging_path'  : 'gs://{project}/dataflow/{dataset}/',
    'bq_temp'       : '{project}:{dataset}_tmp.',
    'bq'            : '{project}:{dataset}.'
}

# ------------------------------------------------------------------------------


def config(ti):

    ti.xscom_push(key='date', value=NOW.strftime('YYYY-MM'))

    for param_name, param_value in DAG_PARAMS.items():
        DAG_PARAMS.update({
            param_name: param_value.format(
                project=_PROJECT, 
                region =_REGION, 
                dataset=_DATASETNAME,
                date=ti.xcom_pull(key='date')
            )
        })
        ti.xcom_push(key=param_name, value = param_value)


def update_config_by_file(ti, f):

    _PARAMS = {}
    _PARAMS['filepath_at_raw'] = ti.xcom_pull('raw_uri')+f+'*.csv'
    _PARAMS['filepath_at_staging'] = ti.xcom_pull('staging_path')+f+'*.parquet'
    _PARAMS['bq_tmp_table'] = ti.xcom_pull('bq_temp')+f

    for k ,v in _PARAMS.items():
        ti.xcom_push(key=k, value=v)    
    return _PARAMS


def find_source_files(ti):
    """ Retrieves all the parts that need to be unziped and processed.
    """
    uri = ti.xscom_pull(key='source_uri')
       
    with storage.CLient() as client:        
        _files = map(lambda b: b.name, client.list_blobs(uri.split('/')[2]))
         # blob.nane -> yellow_tripdata_2015-01_00.csv.zip
        
        for f in _files:
            filename, _ = os.path.splitext(f)
            return filename


# ------------------------------------------------------------------------------

with DAG(
    'etl_nyc_taxis', 
    default_args=default_args, 
    description=dag_desc,
    schedule_interval=dag_interval, 
    tags=['etl', 'nyc_taxi_data'],
    max_active_runs=1, 
    concurrency=1, 
    catchup=False
    ) as dag:

    _env  = Variable.get('ENVIRONMENT')

    # initiate process
    # --------------------------------------------------------------------------    
    create_temp_monthly_dataset = BigQueryCreateEmptyTableOperator(
        task_id="create_monthly_temp_dataset", 
        dataset_reference={"friendlyName": "New tripdata temp dataset"},
        project_id=default_args['project'],
        location=default_args['location'],
        dataset_id=bq_datasets['tmp'].format(date=_date),
        if_exists="fail" # fail dag if the temp dataset for this month already exists
    )

    list_of_new_files = PythonOperator(
        task_id='find_source_files', 
        python_callable=find_source_files,
        show_return_value_in_logs=True
    )


    create_temp_monthly_dataset >> list_of_new_files
    
    # --------------------------------------------------------------------------

    for filename in list_of_new_files[:2]:
        
        _PARAMS = PythonOperator(
            task_id=f'paths_for_{filename}', 
            python_callable=update_config_by_file,
            op_args={'f': filename}
        )
        _DF_DEFAULT = {"project": _PROJECT,"region" : _REGION}
        _DF_OPJOB   = {"project_id":_PROJECT, "location":_REGION, "wait_until_finished": True}

        # first we unzip        
        unzip = DataflowTemplatedJobStartOperator(
            job_name=f"unzip_file_{filename}",
            template=f"gs://dataflow-templates-{_REGION}/latest/Bulk_Decompress_GCS_Files",
            location=_REGION,
            options ={"staging" : f"gs://${_PROJECT}/temp"},
            parameters={
                "inputFilePattern":     _PARAMS['source_uri'],
                "outputDirectory" :     _PARAMS['raw_path'],
                "outputFailureFile" :   _PARAMS['raw_path']+'decomperror.txt'
            },
            wait_until_finished=True
        )

        # once it is unziped we transfrom

        transform = DataflowCreatePythonJobOperator(
            py_file=os.path.join(_CODEHOME,"dataflow", "dataflow-tranform.py"),
            job_name=f"dataflow-transform",
            task_id=f"dataflow-transform_file_{filename}",
            dataflow_default_options=_DF_DEFAULT,
            pipeline_options={
                "output":  _PARAMS['filepath_at_staging'],
                "input" :  _PARAMS['filepath_at_raw']
            },
            py_interpreter="python3.9",
            py_requirements=[
                "apache-beam[gcp]==2.47.0",
                "google-cloud-storage==2.9.0"
            ],
            **_DF_OPJOB
        )

        # then we create the temp bigquery table
        bqload = DataflowCreatePythonJobOperator(
            py_file=os.path.join(_CODEHOME,"dataflow", "dataflow-load.py"),
            job_name="dataflow-load",
            task_id=f"dataflow-load_file_{filename}",
            dataflow_default_options=_DF_DEFAULT,
            pipeline_options={
                "output":  _PARAMS['filepath_at_staging'],
                "input" :  _PARAMS['filepath_at_raw']
            },
            py_interpreter="python3.9",
            py_requirements=[
                "apache-beam[gcp]==2.47.0",
                "google-cloud-storage==2.9.0"
            ],
            **_DF_OPJOB
        )



        list_of_new_files >> transform >> bqload


    
    # step 4 : update final database and delete tmp 
    # --------------------------------------------------------------------------
    """
    append_bq_tmp_into_fixed = BigQueryUpdateDatasetOperator(
    )
     append_bq_tmp_into_fixed = BigQueryUpdateDatasetOperator(
    )
    append_bq_tmp_into_fixed = BigQueryUpdateDatasetOperator(
    )
    delete_bq_tmp = BigQueryDeleteDatasetOperator(
    )

    """
    

# ------------------------------------------------------------------------------