import os

from datetime import datetime, timedelta
from dataclasses import dataclass
from google.cloud import storage

from airflow.models import Variable

from airflow import DAG

from airflow.operators.python   import PythonOperator
from airflow.operators.empty    import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from airflow.decorators import task

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
# NOW = datetime(2015, 1, 10).strftime('$Y-$m')
NOW = '2015-01'
# dag_interval = "@monthly" 

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


def update_config():
    _ = {
        'DATASETNAME'   : 'yellow_tripdata',
        "PROJECT"       : 'graphite-bliss-388109',
        "REGION"        : 'europe-southwest1',
        "CODEHOME"      : '/home/laura/Pruebas/nyc_taxis',
    }
    paths_={
        'source_uri'    : "gs://data_eng_test/",
        'raw_path'      : 'gs://{project}/{dataset}/{date}/',
        'staging_path'  : 'gs://{project}/dataflow/{dataset}/',
        'bq_ds_temp'    : '{dataset}_tmp',
        'bq_ds'         : '{dataset}'
    }

    for param_name, param_value in paths_.items():
        paths_.update({
            param_name: param_value.format(
                project= _["PROJECT"], 
                region =_["REGION"],
                dataset=_["DATASETNAME"],
                date=NOW
            )
        })
    
    paths_.update(_)
    return paths_

DAG_PARAMS = update_config()

# ------------------------------------------------------------------------------

def main_dag(
    dag_id='etl_nyc_taxis_', 
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

        
        dag_start = EmptyOperator(task_id="dag_start")        
        dag_end = EmptyOperator(task_id="dag_end")
        _report = EmptyOperator(task_id="generate_report")

        def find_source_files(ti):
            """ Retrieves all the parts that need to be unziped and processed.
            """
            uri = DAG_PARAMS['source_uri']
            
            st_client = storage.Client() 
                 
            _files = map(lambda b: b.name, st_client.list_blobs(uri.split('/')[2]))
            # blob.nane -> yellow_tripdata_2015-01_00.csv.zip   
            _f =[]
            for f in _files:
                filename, _ = os.path.splitext(f)
                _f.append(filename)

            ti.xcom_push(key='filesnames', value=_f)

        def iter_through_files(ti):
            """Launches a new dag for each fil
            """

            iter_start = EmptyOperator(task_id="dag_start")        
            iter_end = EmptyOperator(task_id="dag_end")
            _iter_report = EmptyOperator(task_id="generate_report")
                
            for filename in ti.xcom_pull(key='filesnames'):

                source_uri = DAG_PARAMS['raw_path']+filename+'*.csv'
                filepath_at_raw = DAG_PARAMS['raw_path']+filename+'*.csv'
                filepath_at_staging = DAG_PARAMS['staging_path']+filename+'*.parquet'
                bq_tmp_table = DAG_PARAMS['bq_ds_temp']+filename

                _DF_OPJOB = {
                    "project_id": DAG_PARAMS["PROJECT"], 
                    "location":DAG_PARAMS["REGION"], 
                    "wait_until_finished": True
                }

                unzip = DataflowTemplatedJobStartOperator(
                    job_name = f"unzip_file",
                    task_id  = f"unzip_file_{filename}",
                    template = f"gs://dataflow-templates-{DAG_PARAMS['REGION']}/latest/Bulk_Decompress_GCS_Files",
                    location = DAG_PARAMS["REGION"],
                    options  = {"staging" : f"gs://${DAG_PARAMS['PROJECT']}/temp"},
                    parameters = {
                        "inputFilePattern"  :   source_uri+filename,
                        "outputDirectory"   :   filepath_at_raw,
                        "outputFailureFile" :   filepath_at_raw+'decomperror.txt'
                    },
                    wait_until_finished=True
                )

                # once it is unziped we transfrom
                transform = DataflowCreatePythonJobOperator(
                    py_file=os.path.join(DAG_PARAMS["CODEHOME"], "dataflow", "dataflow-tranform.py"),
                    job_name=f"dataflow-transform",
                    task_id=f"dataflow-transform_file_{filename}",
                    dataflow_default_options={
                        "project": DAG_PARAMS["PROJECT"],
                        "region" : DAG_PARAMS["REGION"],
                        "output" : filepath_at_staging,
                        "input"  : filepath_at_raw
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
                    py_file=os.path.join(DAG_PARAMS["CODEHOME"], "dataflow", "dataflow-load.py"),
                    job_name="dataflow-load",
                    task_id=f"dataflow-load_file_{filename}",
                    dataflow_default_options={
                        "project": DAG_PARAMS["PROJECT"],
                        "region" : DAG_PARAMS["REGION"],
                        "output":  bq_tmp_table,
                        "input" :  filepath_at_staging
                    },
                    py_interpreter="python3.9",
                    py_requirements=[
                        "apache-beam[gcp]==2.47.0",
                        "google-cloud-storage==2.9.0"
                    ],
                    **_DF_OPJOB
                )


                iter_start >> dag_start >> transform >> _report
            
            _report >> iter_end

        # --------------------------------------------------------------------------  

        create_temp_monthly_dataset = BigQueryCreateEmptyTableOperator(
            task_id="create_monthly_temp_dataset", 
            dataset_id= DAG_PARAMS['bq_ds_temp'],
            table_id  = bq_datasets['tmp'].format(date=NOW),
            project_id= DAG_PARAMS['PROJECT'],
            location  = DAG_PARAMS['REGION'],
            if_exists = "fail" # fail dag if the temp dataset for this month already exists
        )
        
        fetch_files = PythonOperator(
            task_id=f'find_souce_files_for_{NOW}',
            python_callable=find_source_files, 
            show_return_value_in_logs=True,
            provide_context=True
        )

        iter_dataflow = PythonOperator(
            task_id=f'iter_dataflow_{NOW}',
            python_callable=iter_through_files, 
            show_return_value_in_logs=True,
            provide_context=True
        )

        # --------------------------------------------------------------------------  

        dag_start >> create_temp_monthly_dataset    >> _report >> dag_end
        dag_start >> fetch_files >> iter_dataflow   >> _report >> dag_end

        
    # --------------------------------------------------------------------------


main_dag()
# ------------------------------------------------------------------------------