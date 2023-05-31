![](https://purewows3.imgix.net/images/articles/2018_05/nyc_yellow_taxi_in_times_square_hero.jpg?auto=format,compress&cs=strip)

# NYC Taxi ETL

---

Every month New York City releases data about trips taken in NYC taxis. A new ride sharing company is your client.

They are looking to start up a rival service to Uber or Lyft in NYC and are interested in using the Taxi Data to get insight into the transportation sector in New York.

They have tasked you to prepare the datasets and perform an initial exploratory analysis to get to know the data at hand.

The client is  launching a new ride sharing program in New York similar to Uber or  Lyft.

There are three tasks to do in this project:

1. **Write an ETL process able to do the following:**

   **a.** Download Taxi data from our own Google Cloud Storage bucket. The data is stored in this public bucket:   `gs://data_eng_test/ `


   **b**. Transform the data (if applicable) in a way that Google BigQuery is able to ingest it. And upload it to a lake in Google BigQuery.

   c. Design a DB by splitting the resulting table into data and geometries (data and geometries should be joinable by a common key)
2. **Data quality**:

   a. Assess the quality of the data. Identify and document any issues with the data.
3. **Write the SQL queries that help you answer the following questions**:

   a. What is the average fare per mile?

   b. Which are the 10 pickup taxi zones with the highest average tip?

---

## ETL Design and Stack

This ETL has been made using **Apache Beam Python SDK**, **Aiflow** and **BQ SQL**, all using a personal Google Cloud Platform free account in order to have access to the **DataFlow, Google Storage and BigQuery API**s.

![](https://i.ibb.co/3Fvx0N4/untitled-1.png)


As shown in the diagram, there are three main steps, which the final hasn't been developed as a DAG do to time constrains:

1. A monthly-triggered event that looks for new data, or data within the current month; since all the files have a YYYY-MM string.
2. If there are new files, this DAG then triggers another one in which contains the ETL as is. This event is made as scalable as possible following a BATCH PROCESSING with one worker per file.
3. The ETL has three steps, my main idea was to divide them into: loading modeling (applying schema) and verification (filtering data). At the end I found a DataFlow template that unzips data, therefore there are mainly two steps :

   1. data-transform.py that does all the heavy work: checking zeros, null, filtering data without geometries..
   2. data-load.py that fetches the data divided into each part and loads it into BigQuery.

---

## Development and Report on data cuality

About findings that I have check on the data:

* **Geometric data**:

  When approaching the assesment, I first check about its projection (usually data is on `EPSG:4326` which is the only accepted crs by BQ and the common one for webs) and the occurrence of unvalid points.

  Aprox about 10% of the data (by the files that I used for exploration) have points that may be wrong.

  When plotting the Pts with geopandas and contextualy there are two issues:

  * POINT(0 0) : Something when wrong during the data capture phase and no data was collected. In this case my decision has been to filter all raw that do not have lat, lon.
  * POINT(-86 50) : Valid geography, but it is not near NYC; maybe the sensor data was exposed to something that altered its precision. This hasn't been addressed, although filtering by (0.02 - 0.98) could be a solution, because of the nature of the ETL: it transforms the data row by row and  within a single file, therefore each quantile in earch DAG would be different.
  * GPS precision: points come in 15 decimals, which corresponds with a precision of less than cm. For this use case it doesn't make sense to save such big numerics, a precision of 5 (about 2m) should be enough to draw quality insights, although in the process I have chose to save 6 digits (1m precision).
* **Timestamp data**:

  The checks that I made in the ETL are the following:

  * Drops datetimes that do not follow the expected string format
  * if `tpep_pickup` is later than `tpep_dropoff`, then it is also filtered, since this is an error of the sensor.
* **Numeric data**:

  There are rows that couldn't be parsed as numeric due to bad data entry, pe: `0.5.1`or rows that had missing fields and during its ingestion the dataflow-load would fail with a `RuntimeError,  reason 'invalid'`. During the ingestion, Beam creates a JSON in a tmp/ file, therefore the dtype may not be valid for JSON. This is the main reason why I choose saving the transformed data into a .parquet format with an associated schema, that is not the final one, but allows to filter cases.

  Despite that, I still had some errors through the ingestion.
* **Ordinal and bool data**  :

  There are a few unknown classes in the '*ID' columns that where mapped with a '9999' as a string nan value.

New columns that I ADD

Development and bottleneck trough this assigment:

* Although I was confident enough to have everything working, setting up Airflow and GCP was time consuming and I even if I could make work every piece of the ETL, the design presented above is not implemented.

  * I had lots of trouble trying to set up Airflow with GCP, and launching locally (saving into folders and using Beam with DirectRunner) is not an option due to my computer limitations.
  * The final BigQuery ingestion into a single table instead of making all the modeling in a single step was a direct decision of these difficulties.
* Batch processing a single file has a bottleneck on ReadCSVLines, it could be probably better if each file could be read on batches.
* File compression on source
* DataFlow limitation with project organization : it only allows for a single file launch
* There are a few columns through the ETL that are missing:

---

## QUERY RESULT

Here are the result for the data that I could process (2015-04 EXCEPT FILE 18):

    **a**. What is the average fare per mile? **6.169 fare/mile**

    **b**. Which are the 10 pickup taxi zones with the highest average tip?

| 1  | dr5r5vb |  |
| -- | ------- | - |
| 2  | dr5rjn1 |  |
| 3  | dr5quvx |  |
| 4  | dr5xmqs |  |
| 5  | dr5r5v1 |  |
| 6  | dr5jzh5 |  |
| 7  | dr73p56 |  |
| 8  | dr5pnc9 |  |
| 9  | dr7014u |  |
| 10 | dr5ptmt |  |
