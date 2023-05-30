import os

from datetime import datetime, timedelta
from dataclasses import dataclass
from google.cloud import storage

from airflow.models import Variable

from airflow import DAG
from airflow import models
from airflow.operators.python   import PythonOperator
from airflow.decorators import task

# from airflow.sensors.dataflow_batch_plugin import DataflowJobCompleteSensor
from airflow.operators.subdag import SubDagOperator


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
    DataflowTemplatedJobStartOperator
)

# In the test promp, it is said that the data is updated monthly, 
# therefore in the wild this should be a monthly triggered DAG
# algthough for the purposes of this test it is going to be manual
# with fake datetimes
NOW = datetime(2015, 1, 10).strftime('YYYY-MM')
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

    ti.xscom_push(key='date', value=NOW)

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


def update_config_by_file(f):

    _PARAMS = {}
    _PARAMS['filepath_at_raw'] = DAG_PARAMS['raw_path']+f+'*.csv'
    _PARAMS['filepath_at_staging'] = DAG_PARAMS['staging_path']+f+'*.parquet'
    _PARAMS['bq_tmp_table'] = DAG_PARAMS['bq_temp']+f
  
    return _PARAMS



def dataflow_subdag(
        parent_dag_name, 
        child_dag_name, 
        start_date, 
        # schedule_interval,
        filename_partial
    ):
    
    with DAG(
        '%s.%s' % (parent_dag_name, child_dag_name),
        # schedule_interval=schedule_interval,
        start_date=start_date,
    ) as subdag:
        
        _PARAMS = update_config_by_file(filename_partial)

        _DF_DEFAULT = {"project": _PROJECT,"region" : _REGION}
        _DF_OPJOB   = {"project_id":_PROJECT, "location":_REGION, "wait_until_finished": True}

        # first we unzip        
        unzip = DataflowTemplatedJobStartOperator(
            job_name=f"unzip_file",
            task_id=f"unzip_file_{filename_partial}",
            template=f"gs://dataflow-templates-{_REGION}/latest/Bulk_Decompress_GCS_Files",
            location=_REGION,
            options ={"staging" : f"gs://${_PROJECT}/temp"},
            parameters={
                "inputFilePattern":     DAG_PARAMS['source_uri']+filename_partial,
                "outputDirectory" :     DAG_PARAMS['raw_path'],
                "outputFailureFile" :   DAG_PARAMS['raw_path']+'decomperror.txt'
            },
            wait_until_finished=True
        )

        # once it is unziped we transfrom
        transform = DataflowCreatePythonJobOperator(
            py_file=os.path.join(_CODEHOME,"dataflow", "dataflow-tranform.py"),
            job_name=f"dataflow-transform",
            task_id=f"dataflow-transform_file_{filename_partial}",
            dataflow_default_options={
                "project": _PROJECT,
                "region" : _REGION,
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
            task_id=f"dataflow-load_file_{filename_partial}",
            dataflow_default_options={
                "project": _PROJECT,
                "region" : _REGION,
                "output":  _PARAMS['bq_tmp_table'],
                "input" :  _PARAMS['filepath_at_staging']
            },
            py_interpreter="python3.9",
            py_requirements=[
                "apache-beam[gcp]==2.47.0",
                "google-cloud-storage==2.9.0"
            ],
            **_DF_OPJOB
        )


        unzip >> transform >> bqload

        
# ------------------------------------------------------------------------------

def main_dag(
    dag_id='etl_nyc_taxis', 
    default_args=default_args, 
    description=dag_desc,
    start_date=datetime(2015,8,1),
    # schedule_interval=dag_interval, 
    tags=['etl', 'nyc_taxi_data']
    ):

    with DAG(
        dag_id=dag_id, 
        default_args=default_args, 
        description=dag_desc,
        start_date=start_date,
        # schedule_interval=dag_interval, 
        tags=tags,
        max_active_runs=1, 
        concurrency=1, 
        catchup=False
        ) as dag:

        # _env  = Variable.get('ENVIRONMENT')

        config_task = PythonOperator(task_id='config', python_callable=config)
        # initiate process
        # --------------------------------------------------------------------------    
        create_temp_monthly_dataset = BigQueryCreateEmptyTableOperator(
            task_id="create_monthly_temp_dataset", 
            dataset_id= DAG_PARAMS['bq_temp'],
            table_id  = bq_datasets['tmp'].format(date=NOW),
            project_id= _PROJECT,
            location  = _REGION,
            if_exists = "fail" # fail dag if the temp dataset for this month already exists
        )


        config_task >> create_temp_monthly_dataset
        
        def find_source_files():
            """ Retrieves all the parts that need to be unziped and processed.
            """
            uri = DAG_PARAMS['source_uri']
            
            st_client = storage.Client() 
                 
            _files = map(lambda b: b.name, st_client.list_blobs(uri.split('/')[2]))
            # blob.nane -> yellow_tripdata_2015-01_00.csv.zip            
            for f in _files:
                filename, _ = os.path.splitext(f)
                return filename


        list_of_new_files = find_source_files()

        for file in list_of_new_files:

            dg_sub_dag = dataflow_subdag(
                parent_dag_name=dag_id, 
                child_dag_name=file, 
                start_date=start_date, 
                filename_partial=file
            )

    # --------------------------------------------------------------------------

    
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



main_dag()
# ------------------------------------------------------------------------------