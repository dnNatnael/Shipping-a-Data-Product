{{--
    Custom test to ensure no duplicate message_id within the same channel
    This test should return 0 rows to pass
--}}

with duplicates as (

    select 
        message_id,
        channel_name,
        count(*) as duplicate_count
    from {{ ref('stg_telegram_messages') }}
    group by message_id, channel_name
    having count(*) > 1

)

select 
    message_id,
    channel_name,
    duplicate_count
from duplicates
