{{--
    Channel dimension table
    Contains information about each Telegram channel
--}}

with channel_stats as (

    select 
        channel_name,
        min(message_date) as first_post_date,
        max(message_date) as last_post_date,
        count(*) as total_posts,
        avg(view_count) as avg_views,
        sum(view_count) as total_views,
        count(case when has_image = true then 1 end) as posts_with_images,
        count(case when has_media = true then 1 end) as posts_with_media,
        avg(message_length) as avg_message_length
    from {{ ref('stg_telegram_messages') }}
    group by channel_name

),

channel_classification as (

    select 
        *,
        case 
            when channel_name like '%pharma%' or channel_name like '%med%' then 'Pharmaceutical'
            when channel_name like '%cosmetic%' or channel_name like '%beauty%' then 'Cosmetics'
            when channel_name like '%chemed%' or channel_name like '%medical%' then 'Medical'
            else 'Other'
        end as channel_type
    from channel_stats

),

surrogate_keys as (

    select 
        row_number() over (order by channel_name) as channel_key,
        *
    from channel_classification

)

select 
    channel_key,
    channel_name,
    channel_type,
    first_post_date,
    last_post_date,
    total_posts,
    avg_views,
    total_views,
    posts_with_images,
    posts_with_media,
    avg_message_length,
    round(posts_with_images::decimal / nullif(total_posts, 0) * 100, 2) as image_percentage,
    round(posts_with_media::decimal / nullif(total_posts, 0) * 100, 2) as media_percentage
from surrogate_keys
