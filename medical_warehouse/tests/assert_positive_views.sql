{{--
    Custom test to ensure view counts are non-negative
    This test should return 0 rows to pass
--}}

select 
    message_id,
    channel_name,
    view_count
from {{ ref('stg_telegram_messages') }}
where view_count < 0
