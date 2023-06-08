## CLOUD CONFIGURATION

**All this project runs on Google Cloud Platform connected to my device. For this, I have created a mock gmail account from which access the free $300/270€ credits available and used the default project ***My First Project*

**The steps that needed to be followed after having the online console available:**

* **Authentified and verified in local: **

  ```
  gcloud auth login
  gclod auth logic list
  ```

* **Created env variables for project_id, region, image repository; set gcloud config and created a main bucket to work with. We also need to enable some APIs.**

  ```
  export PROJECT=graphite-bliss-388109
  export REGION='europe-southwest1'  
  export DATASETNAME='yellow_tripdata'
  
  gcloud config set project ${PROJECT}
  gcloud config set compute/region ${REGION}
  
  gcloud services enable bigquery.googleapis.com
  gcloud services enable dataflow.googleapis.com
  gcloud services enable composer.googleapis.com
  
  gcloud storage buckets create gs://${PROJECT}
  bq mk --location=${REGION} -d ${DATASETNAME}
  ```

* **Verified that I can access the raw data: **

  ```
  gcloud storage ls --recursive gs://data_eng_test/
  ```

* **Google Cloud Composer / Airflow** : I needed to set an airflow server to run the ETL from gcloud composer, I mainly followed the instructions on this [link](!) and created a webserver through the console with the following specs: 

  * Name : `airflow-nyc-env`

    ```
    gcloud composer environments create airflow-env \
        --location europe-west3 \
        --image-version composer-2.2.1-airflow-2.5.1
    
    ```

    

  * Created environments in gcp console: in `europe-west3`, with a *MEDIUM* machine.

  * Created custom service account for all airflow workers.

  * Setup env variables:

    ```
    gcloud composer environments run airflow-env \
      --location europe-west3 \
      variables set --project_id ${PROJECT}
      
    gcloud composer environments run airflow-nyc-env \
      --location europe-west3 \
      bucket_path BUCKET_PATH 
    ```

    

  * Copy local dag files into environments:

    ```
    gcloud composer environments storage dags import --environment airflow-env  --location europe-west3  --source dags/dag_etl.py
    
    gcloud composer environments storage dags import --environment airflow-env  --location europe-west3  --source dags/dag_update.py
    ```

  * Copy local dataflow files in a gs bucket, in order to be able to be found by the dags.

    ```
    #wip

  

  

* Google Cloud Registry (Docker) : When moving from local development to gcloud, I realized that having a custom img may be useful, though rollback of this idea because of time constrains: 

  * I don't have much experience with DataFlow, didn't know it only takes the .py for the instance it creates; therefore not really able to develop with different files.

    

  I finally tried to avoid using imgs because of time constrains (apache beam is close to the max 5GB allowed in free gcp and takes 15min to build) but here are the CLI commands to  create a repo and build an img from a local folder:

  ```
  export CUSTOM_IMG='beam_247'
  export REPOSITORY='repository'
  export REGION='europe-west3'
  
  gcloud artifacts repositories create {$REPOSITORY}\
     --repository-format=docker \
     --location=${REGION} \
     --async
  
  gcloud auth configure-docker REGION-docker.pkg.dev
  
    
  gcloud builds submit --tag REGION-docker.pkg.dev/PROJECT_ID/REPOSITORY/dataflow/FILE_NAME:TAG .
    
    
  gcloud auth configure-docker ${REGION}-docker.pkg.dev
  
  gcloud builds submit --tag ${REGION}-docker.pkg.dev/${PROJECT}/${REPOSITORY}/dataflow/${CUSTOM_IMG} .
  ```


----

## BOTTLENECKS ENCOUNTERED THROUGH OUT THIS PROJECT

* **Apache Aiflow vs Composer differences**

  I had installed a locally a 2.6.0 airflow; though composer lastest version is 2.5.1; this has the following conflicts:

  * BigQueryCreateEMptyTableOperator has different args

  * Gcloud components are stored in different paths.

  * DataFlow instances are launched in python=3.8.12 and apache_beam=2.46.0 instead of the latest version available (when launched locally) python=3.9 and apache_beam=2.47.0

    

* Airflow infrastructure in Comp: first instance of composer was launched in the default *SMALL* machine, which is not enough to install the apache beam dependencies when launching a DataFlow instance.

  

* Free GCP has quota limitations:

  * DataFlow max workers : only 24, this constrains a lot the ability to launch several instances at the same time: the more instances, the less workers available. This is a headache in airflow.

---

---