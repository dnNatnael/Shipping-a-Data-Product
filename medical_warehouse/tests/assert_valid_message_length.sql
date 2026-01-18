{{--
    Custom test to ensure message lengths are reasonable
    Messages should be between 0 and 4000 characters (Telegram limit)
    This test should return 0 rows to pass
--}}

select 
    message_id,
    channel_name,
    message_length
from {{ ref('stg_telegram_messages') }}
where message_length < 0 or message_length > 4000
