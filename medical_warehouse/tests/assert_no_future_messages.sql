{{--
    Custom test to ensure no messages have future dates
    This test should return 0 rows to pass
--}}

select 
    message_id,
    channel_name,
    message_date
from {{ ref('stg_telegram_messages') }}
where message_date > current_timestamp
