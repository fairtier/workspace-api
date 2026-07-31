select
    location_id,
    borough,
    zone,
    service_zone
from {{ source('nyc_taxi', 'taxi_zones') }}
