-- Cleans the notoriously dirty TLC data (negative fares, 1970/2098 timestamps,
-- 500-mile "trips") and derives the analysis columns.
--
-- Column-name caveat: dlt snake_cases source columns
-- (PULocationID -> pu_location_id, VendorID -> vendor_id, ...). If the first
-- real load shows different names, adjust here.
select
    tpep_pickup_datetime                                        as pickup_at,
    tpep_dropoff_datetime                                       as dropoff_at,
    cast(tpep_pickup_datetime as date)                          as pickup_date,
    extract(hour from tpep_pickup_datetime)                     as pickup_hour,
    dayname(tpep_pickup_datetime)                               as pickup_weekday,
    pu_location_id,
    do_location_id,
    passenger_count,
    trip_distance,
    date_diff('second', tpep_pickup_datetime,
              tpep_dropoff_datetime) / 60.0                     as trip_minutes,
    payment_type,
    case payment_type
        when 1 then 'Credit card'
        when 2 then 'Cash'
        when 3 then 'No charge'
        when 4 then 'Dispute'
        else 'Other'
    end                                                         as payment_type_name,
    fare_amount,
    tip_amount,
    tolls_amount,
    congestion_surcharge,
    total_amount,
    case when fare_amount > 0
         then tip_amount / fare_amount end                      as tip_ratio
from {{ source('nyc_taxi', 'yellow_trips') }}
-- The date bounds are a garbage filter, not a tier window: TLC ships stray
-- 1970 and 2098 timestamps. They used to be '2022-01-01'..'2025-01-01', which
-- silently threw away everything the wider tiers load — keep the lower bound
-- at or below the earliest month any tier mirrors (2019), and the upper bound
-- open-ended, so extending the demo to newer months needs no edit here. A
-- customer who widens their pipeline to TLC's pre-2019 archive should lower
-- the 2019 bound to match.
where tpep_pickup_datetime >= '2019-01-01'
  and tpep_pickup_datetime <  current_date + interval 1 day
  and tpep_dropoff_datetime > tpep_pickup_datetime
  and date_diff('second', tpep_pickup_datetime, tpep_dropoff_datetime) between 30 and 4 * 3600
  and trip_distance between 0.05 and 200
  and fare_amount between 0 and 2000
  and total_amount >= 0
