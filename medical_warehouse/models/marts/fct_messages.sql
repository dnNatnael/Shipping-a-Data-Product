{{--
    Messages fact table
    One row per message with foreign keys to dimension tables
--}}

with messages as (

    select 
        message_id,
        channel_name,
        message_date,
        message_text,
        message_length,
        view_count,
        forward_count,
        has_image,
        has_media,
        scraped_at,
        message_date_only
    from {{ ref('stg_telegram_messages') }}

),

join_channels as (

    select 
        messages.*,
        dim_channels.channel_key
    from messages
    left join {{ ref('dim_channels') }} dim_channels
        on messages.channel_name = dim_channels.channel_name

),

join_dates as (

    select 
        join_channels.*,
        dim_dates.date_key
    from join_channels
    left join {{ ref('dim_dates') }} dim_dates
        on join_channels.message_date_only = dim_dates.full_date

)

select 
    message_id,
    channel_key,
    date_key,
    message_text,
    message_length,
    view_count,
    forward_count,
    has_image,
    has_media,
    scraped_at,
    message_date_only as message_date
from join_dates
