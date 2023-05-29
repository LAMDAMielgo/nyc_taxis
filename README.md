![](https://purewows3.imgix.net/images/articles/2018_05/nyc_yellow_taxi_in_times_square_hero.jpg?auto=format,compress&cs=strip)

# NYC Taxi ETL

---

Every month New York City releases data about trips taken in NYC taxis. A new ride sharing company is your client. 

They are looking to start up a rival service to Uber or Lyft in NYC and are interested in using the Taxi Data to get insight into the transportation sector in New York.  

They have tasked you to prepare the datasets and perform an initial exploratory analysis to get to know the data at hand.

The client is  launching a new ride sharing program in New York similar to Uber or  Lyft.



There are three tasks to do in this project:

1. **Write an ETL process able to do the following:**

   **a.** Download Taxi data from our own Google Cloud Storage bucket. The data is stored in this public bucket:

   ```	gs://data_eng_test/ ```

   ​	

   **b**. Transform the data (if applicable) in a way that Google BigQuery is able to ingest it. And upload it to a lake in Google BigQuery.

   

   c. Design a DB by splitting the resulting table into data and geometries (data and geometries should be joinable by a common key)

   

2. **Data quality**:

   a. Assess the quality of the data. Identify and document any issues with the data.

   

3. **Write the SQL queries that help you answer the following questions**:

   a. What is the average fare per mile?

   b. Which are the 10 pickup taxi zones with the highest average tip?



----

## PROJECT CONFIGURATION

All this project runs on Google Cloud Platform connected to my device. For this, I have created a mock gmail account from which access the free $300/270€ credits available and used the default project *My First Project* 

The steps that needed to be followed after having the online console available:

* Authentified and verified in local: 

  ```
  gcloud auth login
  gclod auth logic list
  ```

  

* Created env variables for project_id, region, image repository; set gcloud config and created a main bucket to work with. We also need to enable some APIs.

  ```
  export PROJECT=graphite-bliss-388109
  export REGION='europe-southwest1'  
  export DATASETNAME='yellow_tripdata'
  
  gcloud config set project ${PROJECT}
  gcloud config set compute/region ${REGION}
  
  gcloud services enable bigquery.googleapis.com
  gcloud services enable dataflow.googleapis.com
  
  gcloud storage buckets create gs://${PROJECT}
  bq mk -d ${DATASETNAME} --location=${REGION}
  ```

* Verified that I can access the raw data: 

  ```
  gcloud storage ls --recursive gs://data_eng_test/
  ```

* Creation of custom apache-beam image with the repository code. This is something that needs to be done since dataflow doesn't allow to copy files, it only executes code from a .py file.

  For this step I have followed [this](!https://cloud.google.com/dataflow/docs/guides/using-custom-containers?hl=es-419#create_and_build_the_container_image) documentation, that explain step-by-step how to create a custom image with google build.

  ```
  gcloud artifacts repositories create {$REPOSITORY}\
     --repository-format=docker \
     --location=${REGION} \
     --async
     
  gcloud auth configure-docker ${REGION}-docker.pkg.dev
  
  gcloud builds submit --tag ${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/dataflow/${CUSTOM_IMG} .
  
  europe-southwest1-docker.pkg.dev/graphite-bliss-388109/repository/dataflow/beam_nyc_taxis
  ```

  The is closed to the max recommended (4.39 < 5GiB) and needs about 15minutes to build in the artifact registry.

----

## Dataflow jobs

Steps followed to make the ETL:

* Unzip files from source: the files can be processed from a DirectRunner launched locally, but when launched in the cloud it throughs a `FileNotFound`. I have used a [public dataflow template](!https://stackoverflow.com/questions/49541026/how-do-i-unzip-a-zip-file-in-google-cloud-storage) that allows to unzip files from a gcp gs:

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



* Process data to BigQuery: all the code has been developed in a script called `main.py` that can be launched into a dataflow job locally with the following code:

  ```
  python all_joined.py \
    --project=${PROJECT} \
    --region=${REGION} \
    --runner=DataflowRunner\
    --setup_file=./setup.py\
    --input=gs://${PROJECT}/${DATASETNAME}/${DATASETNAME}_{date}_00*.csv\
    --output=${PROJECT}:${DATASETNAME}.raw_{date}\
    --num_workers=5\
    --worker_harness_container_image=apache/beam_python3.9_sdk:2.47.0 
  ```

  

This dataflow has the following steps:

* Paralellises by each line from the input passed.

* Parses and validates the geometry data types: 

  * Drop row if lat, lon = 0, 0. If no coords, then geospatial analysis is not possible.

  * Str2Float and round digits: GEOGRAPHY data precision depends on the len of decimals. In this case, I verified that the projection of the raw data is `EPSG:4326`-which is the most usual (web compatibility)- and because it is a degree-based projection, it is more sensible to precision errors.

    In this dataset, most points come with a precision of 15 digits, which is very precise (less than mm, see link in code). I decided to round to a 7 decimal float, which a bit below the meter precision, since for this use-case we are locating cars (3mx5m) in a very big territory.

    

    There are other issues with the geometrical data that haven't been taken into account:

    	* Some points, when plotted, are not even in America. In the data discovery notebook I checked that filtering outliers in the 0.2-0.98 IQR range would work. In order to do this in the dataflow job, we would need to create a PCollection for that, CoGroupByKey (full join) and Filter by inequalities.
    	* There are other points that come with less than 5 decimals, in this case a analysis of how many data is too imprecise to add value is needed.

* Parses and validates datetime data: 

  * Drops datetimes that do not follow the expected string format
  * if `tpep_pickup` is later than `tpep_dropoff`, then it is also filtered, since this is an error of the sensor.

​	

Other things that could be done in this stage, but haven't been implemented:

* Mapping the IDs columns, for `vendor_id` and `rate_code_id` we know the mapping for theirs values and since they are not very extensive (6) it is better for readability to map those values into the dataset.
* Drop columns based on validation rules: if `passenger_count` and `trip_distance` are zero; it could be considered human errors when doing the data entry.
* Creation of an unique id



In relation to the job optimization and scaling:

* The provided format .csv.zip is very tricky to use in a dataflow job; when developing in local, I was able to decompress everything, but somehow it threw a `FileNotFoundError` in the cloud job.
* The current design has a bottleneck at the 'ReadCSVLines', but haven't found a better solution.
* The dataflow job is designed to be launch for every monthly collection of data, since it is a monthly-updated dataset. It can be scaled with airflow to launch a single DAG for every new monthly set of data.
* GCloud Dataflow has a very important limitation: it only copies the main.py file into the apache instances; therefore programming extended ETLs in various files is a bit of a pain. To solve this limitation, one has to build a custom apache-beam image with the code inside and store it in the artifacts registry, creating before hand a repository. This is something that I explored, but apache images are very big and it is a time-consuming way to work on such a small ETL.

----

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
 | .gitgignore
 | README.md
 
 
```

---

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
python all_joined.py \
  --project=${PROJECT} \
  --region=${REGION} \
  --runner=DataflowRunner\
  --input=gs://${PROJECT}/${DATASETNAME}/${DATASETNAME}_{date}_00*.csv\
  --output=gs://${PROJECT}/dataflow/${DATASETNAME}_{date}\
  --worker_harness_container_image=apache/beam_python3.9_sdk:2.47.0
  
python all_joined.py \
  --project=${PROJECT} \
  --region=${REGION} \
  --runner=DataflowRunner\
  --setup_file=./setup.py\
  --input=gs://${PROJECT}/${DATASETNAME}/${DATASETNAME}_{date}_00*.csv\
  --output=gs://${PROJECT}/dataflow/${DATASETNAME}_{date}\
  --num_workers=5\
  --worker_harness_container_image=apache/beam_python3.9_sdk:2.47.0
  
 
python all_joined.py \
  --project=${PROJECT} \
  --region=${REGION} \
  --runner=DataflowRunner\
  --setup_file=./setup.py\
  --input=gs://${PROJECT}/${DATASETNAME}/${DATASETNAME}_{date}_00*.csv\
  --output=${PROJECT}:${DATASETNAME}.raw_{date}\
  --num_workers=10\
  --worker_harness_container_image=apache/beam_python3.9_sdk:2.47.0
 
  
python all_joined.py \
  --project=${PROJECT} \
  --region=${REGION} \
  --runner=DirectRunner\
  --input=gs://${PROJECT}/${DATASETNAME}/${DATASETNAME}_{date}_00*.csv\
  --output=gs://${PROJECT}/dataflow/${DATASETNAME}_{date}\
  --date=2015-01\
  --num_workers=3
```

