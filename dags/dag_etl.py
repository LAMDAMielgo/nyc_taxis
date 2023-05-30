import os

from datetime   import datetime

from airflow import DAG
from airflow.decorators import task

from airflow.operators.python   import PythonOperator
from airflow.operators.empty    import EmptyOperator

from airflow.providers.google.cloud.operators.dataflow import (
    DataflowCreatePythonJobOperator,
    DataflowTemplatedJobStartOperator
)


# DAG2
with DAG(
    dag_id="nyct_dag_etl",
    start_date=datetime(2015,8,1),
    tags=['etl', 'nyc_taxi_data']
) as dag: 

    _start = EmptyOperator(task_id="dag_start")        
    _end   = EmptyOperator(task_id="dag_end")

    """
    {
        "CODEHOME"  :"/home/laura/Pruebas/nyc_taxis"
        "NOW"       :"2015-01"
        "PROJECT"   :"graphite-bliss-388109"
        "REGION"    :"europe-southwest1"
        "bq_tmp_table":"yellow_tripdata_tmpyellow_tripdata_2015-01_01"
        "filename":"yellow_tripdata_2015-01_01"
        "filepath_at_raw":"gs://graphite-bliss-388109/yellow_tripdata/2015-01/yellow_tripdata_2015-01_01*.csv"
        "filepath_at_staging":"gs://graphite-bliss-388109/dataflow/yellow_tripdata/yellow_tripdata_2015-01_01*.parquet"
        "source_uri":"gs://graphite-bliss-388109/yellow_tripdata/2015-01/yellow_tripdata_2015-01_01*.csv"
    }
    """

    def _task(**context):
       
        PROJECT = context["dag_run"].conf["PROJECT"]
        REGION = context["dag_run"].conf["REGION"]
        CODEHOME = context["dag_run"].conf["CODEHOME"]
        NOW=context["dag_run"].conf["NOW"]

        filename = context["dag_run"].conf["filename"]
        source_uri = context["dag_run"].conf["source_uri"]
        filepath_at_raw = context["dag_run"].conf["filepath_at_raw"]
        filepath_at_staging = context["dag_run"].conf["filepath_at_staging"]
        bq_tmp_table = context["dag_run"].conf["bq_tmp_table"]

        unzip = DataflowTemplatedJobStartOperator(
            job_name = f"unzip_file",
            task_id  = f"unzip_file_{filename}",
            template = f"gs://dataflow-templates-{REGION}/latest/Bulk_Decompress_GCS_Files",
            location = REGION,
            options  = {"staging" : f"gs://${PROJECT}/temp"},
            parameters = {
                "inputFilePattern"  :   source_uri+filename,
                "outputDirectory"   :   filepath_at_raw,
                "outputFailureFile" :   filepath_at_raw+'decomperror.txt'
            },
            wait_until_finished=True
        )
        print(f"unziping file: {filename}")
        unzip.execute(context)

        
        transform =  DataflowCreatePythonJobOperator(
            py_file=os.path.join(CODEHOME, "dataflow", "dataflow-tranform.py"),
            job_name=f"dataflow-transform",
            task_id=f"dataflow-transform_file_{filename}",
            dataflow_default_options={
                "project": PROJECT,
                "region" : REGION,
                "output" : filepath_at_staging,
                "input"  : filepath_at_raw
            },
            py_interpreter="python3.9",
            py_requirements=[
                "apache-beam[gcp]==2.47.0",
                "google-cloud-storage==2.9.0"
            ],
            project_id=PROJECT, 
            location=REGION, 
            wait_until_finished= True
        ) 
        print(f"transform file: {filename}")
        transform.execute(context)


        load = DataflowCreatePythonJobOperator(
            py_file=os.path.join(CODEHOME, "dataflow", "dataflow-load.py"),
            job_name=f"dataflow-transform",
            task_id=f"dataflow-transform_file_{filename}",
            dataflow_default_options={
                "project": PROJECT,
                "region" : REGION,
                "output" : bq_tmp_table,
                "input"  : filepath_at_staging
            },
            py_interpreter="python3.9",
            py_requirements=[
                "apache-beam[gcp]==2.47.0",
                "google-cloud-storage==2.9.0"
            ],
            project_id=PROJECT, 
            location=REGION, 
            wait_until_finished= True
        )      
        print(f"loading file: {filename}")
        load.execute(context)



    etl = PythonOperator(
        task_id=f'etl',
        python_callable=_task, 
        show_return_value_in_logs=True,
        provide_context=True
    )

    _start >> etl >> _end


# -----------------------------------------------------------------------------

