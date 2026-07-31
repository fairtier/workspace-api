-- Hour x weekday matrix for the rush-hour heatmap panel.
select
    pickup_weekday,
    pickup_hour,
    count(*)          as trips,
    avg(total_amount) as avg_total,
    avg(avg_mph)      as avg_speed_mph
from {{ ref('fct_trips') }}
group by 1, 2
