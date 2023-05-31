
/*
 * CREATING THE FINAL SCEMA TO BE UPLOAD EVYTHING
*/
CREATE SCHEMA IF NOT EXISTS  `graphite-bliss-388109.db_nyc_taxi_data` OPTIONS (
    description = 'FINAL TABLE',
    location = 'US'
);

CREATE TABLE IF NOT EXISTS  `graphite-bliss-388109.db_nyc_taxi_data.trips_pickup`
(
  custom_id STRING NOT NULL,
  geohash   STRING NOT NULL,
  geom      GEOGRAPHY,
  timestmp  TIMESTAMP,
  created_at TIMESTAMP
)
PARTITION BY date(timestmp)
CLUSTER BY geohash
;

CREATE TABLE IF NOT EXISTS  `graphite-bliss-388109.db_nyc_taxi_data.trips_dropdown` 
(
  custom_id STRING NOT NULL,
  geohash   STRING NOT NULL,
  geom      GEOGRAPHY,
  timestmp  TIMESTAMP,
  created_at TIMESTAMP
)
PARTITION BY date(timestmp)
CLUSTER BY geohash
;


CREATE TABLE IF NOT EXISTS  `graphite-bliss-388109.db_nyc_taxi_data.trips_data` 
(
    custom_id   STRING NOT NULL,
    VendorID    STRING,
    RateCodeID  STRING,
    store_and_fwd_flag BOOLEAN,
    payment_type STRING,
    passenger_count INTEGER,
    trip_distance   FLOAT64,
    amount_details STRUCT<fare FLOAT64, extra FLOAT64, mta FLOAT64, tip FLOAT64,tolls FLOAT64, improvement_surcharge FLOAT64>,
    amount_total FLOAT64,
    trip_minutes INTEGER,
    pickup_timestmp TIMESTAMP,
    dropoff_timestmp TIMESTAMP,
    created_at TIMESTAMP
)
PARTITION BY date(pickup_timestmp)
CLUSTER BY passenger_count, trip_minutes
;



/*
 * GEOMETRIES TABLE
*/
INSERT INTO `db_nyc_taxi_data.trips_pickup`(
  custom_id, geohash, geom,timestmp, created_at 
)SELECT  
    id as custom_id,
    ST_GEOHASH(pickup_geom, 7) as geohash,
    pickup_geom       as geom,
    TIMESTAMP(tpep_pickup_datetime) as timestmp,    
    TIMESTAMP(CURRENT_DATE('00:00')) as created_at
  FROM `graphite-bliss-388109.yellow_tripdata.tripdata_tmp` 
;


INSERT INTO `db_nyc_taxi_data.trips_dropdown`(
  custom_id, geohash, geom,timestmp, created_at 
) SELECT  
    id as custom_id,
    ST_GEOHASH(pickup_geom, 7) as geohash,
    pickup_geom       as geom,
    TIMESTAMP(tpep_dropoff_datetime) as timestmp,
    TIMESTAMP(CURRENT_DATE('00:00')) as created_at, 
FROM `graphite-bliss-388109.yellow_tripdata.tripdata_tmp` 
;

INSERT INTO `db_nyc_taxi_data.trips_data` (
    custom_id, VendorID, RateCodeID, store_and_fwd_flag, payment_type, passenger_count, trip_distance, 
    amount_details, 
    amount_total, trip_minutes ,pickup_timestmp ,dropoff_timestmp ,created_at 
) SELECT  
      id as custom_id,
      VendorID,
      RateCodeID,
      store_and_fwd_flag,
      payment_type,
      cast(passenger_count as INTEGER) as passenger_count,
      ROUND(cast( trip_distance as FLOAT64), 2) as trip_distance,
      STRUCT (
        cast(round(fare_amount, 2) as FLOAT64) as fare,
        cast(round(extra, 2) as FLOAT64) as extra,
        cast(round(mta_tax, 2) as FLOAT64) as mta,
        cast(round(tip_amount, 2) as FLOAT64) as tip,
        cast(round(tolls_amount, 2) as FLOAT64) as tolls,	
        cast(round(improvement_surcharge, 2) AS FLOAT64) as improvement_surcharge
      ) as amount_details,
      total_amount as amount_total,
      CAST (TIMESTAMP_DIFF(TIMESTAMP(tpep_dropoff_datetime), TIMESTAMP(tpep_pickup_datetime), MINUTE) AS INTEGER) as trip_minutes,
      TIMESTAMP(tpep_pickup_datetime) as pickup_timestmp,
      TIMESTAMP(tpep_dropoff_datetime) as dropoff_timestmp,
      TIMESTAMP(CURRENT_DATE('00:00')) as created_at
  FROM `graphite-bliss-388109.yellow_tripdata.tripdata_tmp` 
;
