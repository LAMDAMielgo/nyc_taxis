WITH
/* AVR FARE PER MILE
  FARE : amount.fare in usd/fare --> The time-and-distance fare calculated by the mete
  Miles : trip_distance  --> The elapsed trip distance in miles reported by the taximeter
*/

fare_per_mile as (
  SELECT round(avg(amount_details.fare / trip_distance), 3) as _avg
  FROM `graphite-bliss-388109.db_nyc_taxi_data.trips_data` 
  WHERE amount_details.fare != 0 and
        trip_distance != 0
)

SELECT * FROM fare_per_mile