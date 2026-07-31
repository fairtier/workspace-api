-- The dashboard workhorse: pre-aggregated so Rill slices sub-second.
select
    pickup_date,
    pickup_borough,
    pickup_zone,
    count(*)                    as trips,
    sum(total_amount)           as revenue,
    avg(fare_amount)            as avg_fare,
    avg(tip_ratio)              as avg_tip_ratio,
    avg(trip_distance)          as avg_distance,
    avg(avg_mph)                as avg_speed_mph
from {{ ref('fct_trips') }}
group by 1, 2, 3
