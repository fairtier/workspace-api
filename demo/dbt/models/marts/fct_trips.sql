-- The DuckDB muscle: a ~110M-row double join materialized back into Iceberg.
-- Proves the engine isn't slacking.
select
    t.*,
    pu.borough  as pickup_borough,
    pu.zone     as pickup_zone,
    dz.borough  as dropoff_borough,
    dz.zone     as dropoff_zone,
    case when t.trip_minutes > 0
         then t.trip_distance / (t.trip_minutes / 60.0) end as avg_mph
from {{ ref('stg_trips') }} t
left join {{ ref('stg_zones') }} pu on t.pu_location_id = pu.location_id
left join {{ ref('stg_zones') }} dz on t.do_location_id = dz.location_id
