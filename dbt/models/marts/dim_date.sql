with spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2024-01-01' as date)",
        end_date="dateadd(year, 2, current_date())"
    ) }}
)

select
    cast(date_day as date) as date_day,
    year(date_day) as year_number,
    quarter(date_day) as quarter_number,
    month(date_day) as month_number,
    monthname(date_day) as month_name,
    date_trunc('week', date_day)::date as week_start_date,
    date_trunc('month', date_day)::date as month_start_date,
    dayofweekiso(date_day) as day_of_week_iso,
    dayname(date_day) as day_name,
    dayofweekiso(date_day) in (6, 7) as is_weekend
from spine
