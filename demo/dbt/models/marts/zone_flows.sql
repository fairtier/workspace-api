-- Top pickup->dropoff corridors (for a "where do airport riders go" panel).
select
    pickup_zone,
    dropoff_zone,
    count(*)          as trips,
    avg(total_amount) as avg_total
from {{ ref('fct_trips') }}
where pickup_zone is not null and dropoff_zone is not null
group by 1, 2
having count(*) >= 1000
