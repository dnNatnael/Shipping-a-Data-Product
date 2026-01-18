{{--
    Date dimension table
    Contains calendar information for time-based analysis
--}}

with date_range as (

    select 
        min(message_date_only) as min_date,
        max(message_date_only) as max_date
    from {{ ref('stg_telegram_messages') }}

),

date_series as (

    select 
        generate_series(
            min_date, 
            max_date, 
            interval '1 day'
        )::date as full_date
    from date_range

),

date_attributes as (

    select 
        full_date,
        extract(year from full_date) as year,
        extract(month from full_date) as month,
        extract(day from full_date) as day,
        extract(quarter from full_date) as quarter,
        extract(dow from full_date) as day_of_week_num,
        to_char(full_date, 'Day') as day_name,
        to_char(full_date, 'Month') as month_name,
        extract(week from full_date) as week_of_year,
        extract(doy from full_date) as day_of_year,
        case 
            when extract(dow from full_date) in (0, 6) then true
            else false
        end as is_weekend,
        case 
            when extract(dow from full_date) in (1, 2, 3, 4, 5) then true
            else false
        end as is_weekday,
        to_char(full_date, 'YYYY-MM') as year_month,
        to_char(full_date, 'YYYY-"Q"Q') as year_quarter,
        full_date - date_trunc('week', full_date)::date as day_of_week_offset
    from date_series

),

surrogate_keys as (

    select 
        row_number() over (order by full_date) as date_key,
        *
    from date_attributes

)

select 
    date_key,
    full_date,
    year,
    month,
    day,
    quarter,
    day_of_week_num,
    day_name,
    month_name,
    week_of_year,
    day_of_year,
    is_weekend,
    is_weekday,
    year_month,
    year_quarter,
    day_of_week_offset
from surrogate_keys
