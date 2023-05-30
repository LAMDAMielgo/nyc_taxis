import os

from datetime import datetime, timedelta

from google.cloud import storage

from airflow.hooks.base import BaseHook
from airflow.models import Variable

from airflow import DAG
from airflow import models

from airflow.operators.bash     import BashOperator
from airflow.operators.python   import PythonOperator
from airflow.providers.apache.beam.hooks.beam import BeamHook, BeamRunnerType

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryGetDatasetOperator,
    BigQueryUpdateDatasetOperator
)
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowConfiguration,
    DataflowCreatePythonJobOperator,
    DataflowTemplatedJobStartOperator,
    BeamRunPythonPipelineOperator
)
from airflow.utils.trigger_rule import TriggerRule

class airflow.providers.google.cloud.operators.dataflow.CheckJobRunning

now = datetime(2015, 1, 10)

default_args = {
    'owner': 'lamdam',
    'depends_on_past': False,
    'retries': 0,
    'project' : 'etl_nyc_taxi',
    'location': 'europe-southwest1,'
    'source_uri' : "gs://data_eng_test/",
    "dataset_name": 'yellow_tripdata'
}

dag_desc = """Dag to get all the data from the NYC Taxis Source. This
data is updated monthly and has N amount of parts."""
dag_interval = "@monthly" 
bq_datasets = {
    'tmp': 'tripdata_{date}_tmp', 
    'apppend': ['tripdata', 'pickup', 'dropdown']
}

# ------------------------------------------------------------------------------


def find_source_files(date, uri, target_fext, dataset_name):
    """ Retrieves all the parts that need to be unziped and processed
    """
    uri += dataset_name+'_'+date+'*'+target_fext
    
    with storage.CLient() as client:        
        return map(lambda b: b.name, client.list_blobs(uri))


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

    _date = now.strptime('$Y-$m')
    _env  = Variable.get('ENVIRONMENT')

    # step 1 - crete bigquery datasets to store data
    # create final datasets if they still do not exists
    # --------------------------------------------------------------------------
    create_temp_monthly_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_monthly_temp_dataset", 
        dataset_reference={"friendlyName": "New tripdata temp dataset"},
        project_id=default_args['project'],
        location=default_args['location'],
        dataset_id=bq_datasets['tmp'].format(date=_date),
        if_exists="fail" # fail dag if the temp dataset for this month already exists
    )


    # CREATE FINAL TABLES IF NOT EXIST
    create_tripdata_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_tripdata_dataset", 
        dataset_reference={"friendlyName": "Initial tripdata dataset"},
        project_id=default_args['project'],
        location=default_args['location'],
        dataset_id=bq_datasets['append'][0],
        if_exists="ignore" # ignore this step if already exists 
    )
    create_dropdown_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dropdown_dataset", 
        dataset_reference={"friendlyName": "Initial dropdowm geom dataset"},
        project_id=default_args['project'],
        location=default_args['location'],
        dataset_id=bq_datasets['append'][1],
        if_exists="ignore" # ignore this step if already exists 
    )
    create_pickup_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_pickup_dataset", 
        dataset_reference={"friendlyName": "Initial pickup dataset"},
        project_id=default_args['project'],
        location=default_args['location'],
        dataset_id=bq_datasets['append'][2],
        if_exists="ignore" # ignore this step if already exists 
    )


    # step 2 : find files to unzip and unzip them into GS dataset raw storage
    # --------------------------------------------------------------------------
    find_files_to_unzip = PythonOperator(
        python_callable=,
        op_args=,
        tmepaltes_dicts=,
        show_return_value_in_logs=True
    )    
    unzip_dataflow = DataflowTemplatedJobStartOperator(

    )

    # step 3- process files
    # --------------------------------------------------------------------------
    transform_dataflow = DataflowCreatePythonJobOperator(
        py_file="dataflow-tranform.py".
        job_name="dataflow-transform",
        dataflow_default_options=,
        py_options=[],
        pipeline_options={
            "output": GCS_OUTPUT,
            "input" : 
            "file"  :
        },
        py_requirements=[
           "apache-beam[gcp]==2.46.0"
        ],
        py_interpreter="python3",
        py_system_site_packages=False,
        dataflow_config={"location": LOCATION},
        project_id='',
        location=,
        wait_until_finished=True
    )


    load_dataflow = ()


    # step 4 : update final database and delete tmp 
    # --------------------------------------------------------------------------

    append_bq_tmp_into_fixed = (

    )
     append_bq_tmp_into_fixed = (

    )
     append_bq_tmp_into_fixed = (

    )
    delete_bq_tmp = (
       
    )

    def create_job(date):
        args = ['-country', country, '-datafactory_schema', schema]

        return UdaKubernetesPodOperator(
            task_id='pois_assets_' + country + '_' + schema,
            image="eu.gcr.io/data-etl-323813/pois_assets",
            env_vars={'ENVIRONMENT': environment,
                        'SQL_INSTANCE': sql_instance,
                        'POSTGIS_USER': sql_user,
                        'POSTGIS_PASS': sql_pass},
            arguments=args,
            execution_timeout=timedelta(hours=20),
            inject_data_etl_credentials=True,
            dag=dag
        )
   
    create_job(_date)

# ------------------------------------------------------------------------------