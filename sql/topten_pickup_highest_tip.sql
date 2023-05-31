WITH

/* Which are the 10 pickup taxi zones with the highest average tip?
  #   (maximum X axis error, in km)     
1   ± 2500
2   ± 630
3   ± 78
4   ± 20
5   ± 2.4
6   ± 0.61
7   ± 0.076
8   ± 0.019
9   ± 0.0024
10  ± 0.00060
11  ± 0.000074
*/

avg_tip_by_gh7 as (
    SELECT 
        round(avg(amount_details.tip), 3) as avg_tip,
        geohash
    FROM `db_nyc_taxi_data.trips_data` d
    JOIN `db_nyc_taxi_data.trips_pickup` p on
      d.custom_id = p.custom_id and
      d.pickup_timestmp = p.timestmp
    GROUP BY p.geohash
),

top_ten_gh7 as (
  SELECT geohash FROM avg_tip_by_gh7
  ORDER BY avg_tip DESC
  LIMIT 10
)

SELECT * FROM top_ten_gh7 

