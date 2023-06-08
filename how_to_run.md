## HOW TO RUN THIS ETL (wip)

----



* **Extract, Unzip files from source**: the files can be processed from a DirectRunner launched locally, but when launched in the cloud it throughs a `FileNotFound`. I have used a [public dataflow template](!https://stackoverflow.com/questions/49541026/how-do-i-unzip-a-zip-file-in-google-cloud-storage) that allows to unzip files from a gcp gs:

  ```
  gcloud dataflow jobs run unzip \
  --gcs-location gs://dataflow-templates-${REGION}/latest/Bulk_Decompress_GCS_Files \
  --region ${REGION} \
  --num-workers 1 \
  --staging-location gs://${PROJECT}/temp \
  --parameters \
  inputFilePattern=gs://data_eng_test/*.csv.zip,\
  outputDirectory=gs://${PROJECT}/${DATASETNAME}/,\
  outputFailureFile=gs://${PROJECT}/${DATASETNAME}/decomperror.txt
  ```

* **Process data to BigQuery: all the code has been developed in a script called **`main.py` that can be launched into a dataflow job locally with the following code:

  ```
  python dataflow/dataflow-transform.py\
    --project=${PROJECT} \
    --region=${REGION} \
    --runner=DataFlow\
    --input=gs://${PROJECT}/${DATASETNAME}/${DATASETNAME}_{date}*03.csv\
    --output=gs://${PROJECT}/dataflow/${DATASETNAME}_{date}_03\
    --date=${DATE}
  
    
    python dataflow-transform.py\
    --input=gs://${PROJECT}/${DATASETNAME}/${DATASETNAME}_{date}_10*.csv\
    --output=gs://${PROJECT}/dataflow/${DATASETNAME}_{date}\
  
    --worker_harness_container_image=apache/beam_python3.9_sdk:2.47.0
    --setup_file=./setup.py
  
    --output=${PROJECT}:${DATASETNAME}.raw_{date}\
    
  ```

  **The last couple two arguments are only run if the artifact is deployed onto a registry repository; and**

**Steps followed to make the ETL:**

* **Extract, Unzip files from source**: the files can be processed from a DirectRunner launched locally, but when launched in the cloud it throughs a `FileNotFound`. I have used a [public dataflow template](!https://stackoverflow.com/questions/49541026/how-do-i-unzip-a-zip-file-in-google-cloud-storage) that allows to unzip files from a gcp gs:

  ```
  gcloud dataflow jobs run unzip \
  --gcs-location gs://dataflow-templates-${REGION}/latest/Bulk_Decompress_GCS_Files \
  --region ${REGION} \
  --num-workers 1 \
  --staging-location gs://${PROJECT}/temp \
  --parameters \
  inputFilePattern=gs://data_eng_test/*.csv.zip,\
  outputDirectory=gs://${PROJECT}/${DATASETNAME}/,\
  outputFailureFile=gs://${PROJECT}/${DATASETNAME}/decomperror.txt
  ```

* **Process data to BigQuery: all the code has been developed in a script called **`main.py` that can be launched into a dataflow job locally with the following code:

  ```
  python dataflow/dataflow-transform.py\
    --project=${PROJECT} \
    --region=${REGION} \
    --runner=DataflowRunner\
    --input=gs://${PROJECT}/${DATASETNAME}/${DATASETNAME}_{date}*03.csv\
    --output=gs://${PROJECT}/dataflow/${DATASETNAME}_{date}_03\
    --date=${DATE}
  
    
    python dataflow-transform.py\
    --input=gs://${PROJECT}/${DATASETNAME}/${DATASETNAME}_{date}_10*.csv\
    --output=gs://${PROJECT}/dataflow/${DATASETNAME}_{date}\
  
    --worker_harness_container_image=apache/beam_python3.9_sdk:2.47.0
    --setup_file=./setup.py
  
    --output=${PROJECT}:${DATASETNAME}.raw_{date}\
    
  ```

  **The last couple two arguments are only run if the artifact is deployed onto a registry repository; and**

**Steps followed to make the ETL:**

* **Extract, Unzip files from source**: the files can be processed from a DirectRunner launched locally, but when launched in the cloud it throughs a `FileNotFound`. I have used a [public dataflow template](!https://stackoverflow.com/questions/49541026/how-do-i-unzip-a-zip-file-in-google-cloud-storage) that allows to unzip files from a gcp gs:

  ```
  gcloud dataflow jobs run unzip \
  --gcs-location gs://dataflow-templates-${REGION}/latest/Bulk_Decompress_GCS_Files \
  --region ${REGION} \
  --num-workers 1 \
  --staging-location gs://${PROJECT}/temp \
  --parameters \
  inputFilePattern=gs://data_eng_test/*.csv.zip,\
  outputDirectory=gs://${PROJECT}/${DATASETNAME}/,\
  outputFailureFile=gs://${PROJECT}/${DATASETNAME}/decomperror.txt
  ```

* **Process data to BigQuery: all the code has been developed in a script called **`main.py` that can be launched into a dataflow job locally with the following code:

  ```
  python dataflow/dataflow-transform.py\
    --project=${PROJECT} \
    --region=${REGION} \
    --runner=DataflowRunner\
    --input=gs://${PROJECT}/${DATASETNAME}/${DATASETNAME}_{date}*03.csv\
    --output=gs://${PROJECT}/dataflow/${DATASETNAME}_{date}_03\
    --date=${DATE}
  
    
    python dataflow-transform.py\
    --input=gs://${PROJECT}/${DATASETNAME}/${DATASETNAME}_{date}_10*.csv\
    --output=gs://${PROJECT}/dataflow/${DATASETNAME}_{date}\
  
    --worker_harness_container_image=apache/beam_python3.9_sdk:2.47.0
    --setup_file=./setup.py
  
    --output=${PROJECT}:${DATASETNAME}.raw_{date}\
    
  ```

  **The last couple two arguments are only run if the artifact is deployed onto a registry repository; and**

## Dependencies

```

```

```
gsutil iam ch serviceAccount:${PROJECT}@storage-transfer-service.iam.gserviceaccount.com:roles/storage.admin gs://${PROJECT}/raw
```

```
gsutil cat gs://data_eng_test/*.csv.zip | zcat |  gsutil cp - gs://${PROJECT}/raw/*.csv
```

```
source .env/bin/activate
export PROJECT=graphite-bliss-388109
export REGION='europe-southwest1'  
export DATASETNAME='yellow_tripdata'

python dataflow/dataflow-transform.py \
  --project=${PROJECT} \
  --region=${REGION} \
  --runner=DataFlowRunner\
  --input=gs://${PROJECT}/${DATASETNAME}/${DATASETNAME}_2015-04_18.csv\
  --output=gs://${PROJECT}/dataflow/${DATASETNAME}_2015-04_18




source .env/bin/activate
export PROJECT=graphite-bliss-388109
export REGION='europe-southwest1'  
export DATASETNAME='yellow_tripdata'

python dataflow/dataflow-load.py \
  --project=${PROJECT} \
  --region=${REGION} \
  --input=gs://${PROJECT}/dataflow/${DATASETNAME}_2015-04_26*.parquet\
  --output=${PROJECT}:${DATASETNAME}\
  --runner=DataFlowRunner
```

```
gcloud auth application-default set-quota-project ${PROJECT}
```

**pip install "apache-airflow[gcp]==2.6.0" --constraint "**[https://raw.githubusercontent.com/apache/airflow/constraints-2.6.0/constraints-3.9.txt](https://raw.githubusercontent.com/apache/airflow/constraints-2.6.0/constraints-3.9.txt)"

```
python -m export-data-pipeline \
  --input="crm_integration.user_attribute_export_20200401" \
  --runner=Dataflow \
  --project="${PROJECT_ID}" \
  --job_name="export-attributes-to-crm-platform" \
  --staging_location="gs://${BUCKET_NAME}/dataflows/export_to_crm_platform/staging" \
  --temp_location="gs://${BUCKET_NAME}/dataflows/export_to_crm_platform/temp" \
  --region=europe-west1 \
  --enable_streaming_engine \
  --machine_type=n1-standard-2 \
  --max_num_workers=50 \
  --disk_size_gb=25 \
  --flexrs_goal=COST_OPTIMIZED
```

```

```

**cp dags/dag_nyc_taxis.py ~/Pruebas/nyc_taxis/.env/lib/python3.9/site-packages/airflow/example_dags/**

**cp dags/dag_etl.py ~/Pruebas/nyc_taxis/.env/lib/python3.9/site-packages/airflow/example_dags/**

**cp dags/dag_update.py ~/Pruebas/nyc_taxis/.env/lib/python3.9/site-packages/airflow/example_dags/**

# COMPOSER

```
gcloud composer environments describe ${PROJECT} \
    --location europe-west3 \
    --format="get(config.dagGcsPrefix)"
    
gcloud composer environments run nyc-etl-env \
  --location europe-west3 \
  variables set -- \
  project_id ${PROJECT}
  
gcloud composer environments run nyc-etl-env \
  --location europe-west3 \
  variables set -- \
  project_id ${PROJECT}
```

```
# Get current project's project number
PROJECT_NUMBER=gcloud projects list \
  --filter="$(gcloud config get-value project)" \
  --format="value(PROJECT_NUMBER)" \
  --limit=1

# Add the Cloud Composer v2 API Service Agent Extension role
gcloud iam service-accounts add-iam-policy-binding \
    $PROJECT_NUMBER-compute@developer.gserviceaccount.com \
    --member serviceAccount:service-$PROJECT_NUMBER@cloudcomposer-accounts.iam.gserviceaccount.com \
    --role roles/composer.ServiceAgentV2Ext
```

```
gcloud iam service-accounts keys create ~/sa-private-key.json \
    --iam-account=airflow@${PROJECT}.iam.gserviceaccount.com
```

## Project Structure

```
nyc_taxis_etl
 |                           # local files
 | .env/
 | .sample_data/
	| ..
 | etl / 					# custom dataflow etl job for GS to BQ
 	| main.py
 | dag /					# airflow dag
 	| DockerFile
 	| dag_yellow_tripdata.py
 | artifact /				# code and docker to build custom apache img
 	| DockerFile
    | main.py
    | setup.py
 	| src /
 		| __init__.py
 		| run.py
 		| transformations.py
 		| metadata.py
 | requirements.txt
 | data_discovery.ipynb
 | data_discovery.ipynb
 | data_discovery.ipynb
 | .gitgignore
 | README.md
 
 
```

---