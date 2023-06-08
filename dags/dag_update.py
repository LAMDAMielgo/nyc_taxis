import os
import time

from datetime import datetime, timedelta
from dataclasses import dataclass
from google.cloud import storage

from airflow.models import Variable

from airflow import DAG

from airflow.operators.python   import PythonOperator, BranchPythonOperator
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
NOW = datetime(2015, 1, 10).strftime("%Y-%m")
# dag_interval = "@monthly" 

default_args = {
    'owner': 'lamdam',
    'depends_on_past': False,
    'retries': 0
}
dag_desc = """Dag to get all the data from the NYC Taxis Source. This
data is updated monthly and has N amount of parts."""


def update_config():
    _ = {
        'DATASETNAME'   : 'yellow_tripdata',
        "PROJECT"       : 'graphite-bliss-388109',
        "REGION"        : 'europe-southwest1',
        "CODEHOME"      : {
            'LOCAL': '/home/laura/Pruebas/nyc_taxis/dataflow',
            'GCS'  : "gs://{project}/code"
        }
    }
    paths_={
        'source_uri'    : "gs://data_eng_test/",
        'raw_path'      : 'gs://{project}/{dataset}/{date}/',
        'staging_path'  : 'gs://{project}/dataflow/{dataset}/',
        'bq_ds_temp'    : '{dataset}_tmp2',
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

# DAG1
with DAG(
    dag_id='nyc_update', 
    default_args=default_args, 
    description=dag_desc,
    start_date=datetime(2015,8,2),
    schedule_interval=None,
    is_paused_upon_creation=True,
    tags=['etl', 'nyc_taxi_data'],
    max_active_runs=1, 
    max_active_tasks=4,
    concurrency=3, 
    catchup=False
) as dag:

    dag_start = EmptyOperator(task_id="dag_start")        
    dag_end   = EmptyOperator(task_id="dag_end")
    # _report = EmptyOperator(task_id="generate_report")

    def find_files(**context):
            """ Retrieves all the parts that need to be unziped and processed.
            """
            uri = DAG_PARAMS['source_uri']
            print(F"\n\nURI : {uri}")
            
            st_client = storage.Client() 
                 
            _files = list(map(lambda b: b.name, st_client.list_blobs(uri.split('/')[2])))
            # blob.nane -> yellow_tripdata_2015-01_00.csv.zip   

            _f =[]
            for f in _files:
                # Debido a la forma en qu llamamos a las cosas mas adelante, no necesitamos
                # la extensión del archivo, por lo que filtramos
                # Nos lee todo, asi que lo filtramos por fecha a parserar
                if f.endswith('.csv.zip') and (NOW in f):
                    f = f.strip('.csv.zip')
                    _f.append(f)

                    source_uri = DAG_PARAMS['source_uri']+f+'.csv.zip'
                    filepath_at_raw = DAG_PARAMS['raw_path']+f+'.csv'
                    filepath_at_staging = DAG_PARAMS['staging_path']+f
                    bq_tmp_table = DAG_PARAMS['bq_ds_temp']

                    build_dag_run_conf_and_trigger_dag_update = TriggerDagRunOperator(
                        task_id=f'trigger_etl_for_{f}',
                        trigger_dag_id="nyc_etl",
                        conf={
                            'PROJECT'                 : DAG_PARAMS["PROJECT"],
                            "REGION"                  : DAG_PARAMS["REGION"],
                            "CODEHOME"                : DAG_PARAMS["CODEHOME"]['GCS'].format(project=DAG_PARAMS["PROJECT"]),
                            "NOW"                     : NOW,
                            "filename"                : f,
                            "source_uri"              : source_uri,
                            "filepath_at_raw"         : filepath_at_raw,
                            "filepath_at_staging"     : filepath_at_staging,
                            "bq_tmp_table"            : bq_tmp_table
                        }
                    )

                    print(f"Trigger dag: {f}")
                    build_dag_run_conf_and_trigger_dag_update.execute(context)

            context["ti"].xcom_push(key='filesnames', value=_f)
            print(f"Found : {len(_f)}")


    # --------------------------------------------------------------------------  
        
    create_temp_monthly_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_temp_monthly_dataset", 
        dataset_id= DAG_PARAMS['bq_ds_temp'],
        project_id= DAG_PARAMS["PROJECT"],
        location  = DAG_PARAMS["REGION"],
        # if_exists = "log" # fail dag if the temp dataset for this month already exists
        exists_ok = True
    )
    
    create_temp_monthly_table = BigQueryCreateEmptyTableOperator(
        task_id="create_temp_monthly_table", 
        dataset_id= DAG_PARAMS['bq_ds_temp'],
        table_id  = DAG_PARAMS['DATASETNAME']+NOW+'_tmp',
        project_id= DAG_PARAMS["PROJECT"],
        location  = DAG_PARAMS["REGION"],
        # if_exists = "log" # fail dag if the temp dataset for this month already exists
        exists_ok = True
    )

    iter_dataflow = PythonOperator(
        task_id=f'trigger_dataflow',
        python_callable=find_files, 
        show_return_value_in_logs=True,
        provide_context=True
    )

    # --------------------------------------------------------------------------  

    dag_start >> create_temp_monthly_dataset >>  create_temp_monthly_table  >> dag_end
    dag_start >> iter_dataflow                                              >> dag_end
# --------------------------------------------------------------------------

