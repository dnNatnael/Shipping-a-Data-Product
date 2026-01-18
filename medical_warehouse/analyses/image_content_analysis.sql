-- Analysis of image content patterns and engagement
-- This analysis answers key questions about visual content performance

-- 1. Do "promotional" posts (with people) get more views than "product_display" posts?
with image_engagement as (
    select 
        img.calculated_category,
        fm.view_count,
        fm.forward_count,
        fm.message_length,
        dc.channel_name,
        dd.full_date
    from {{ ref('fct_image_detections') }} img
    join {{ ref('fct_messages') }} fm on img.message_id = fm.message_id
    join {{ ref('dim_channels') }} dc on img.channel_key = dc.channel_key
    join {{ ref('dim_dates') }} dd on img.date_key = dd.date_key
    where img.calculated_category in ('promotional', 'product_display', 'lifestyle', 'other')
),

category_stats as (
    select 
        calculated_category,
        count(*) as post_count,
        avg(view_count) as avg_views,
        median(view_count) as median_views,
        avg(forward_count) as avg_forwards,
        median(forward_count) as median_forwards,
        avg(message_length) as avg_message_length,
        stddev(view_count) as views_stddev
    from image_engagement
    group by calculated_category
)

select 
    calculated_category,
    post_count,
    round(avg_views, 2) as avg_views,
    round(median_views, 2) as median_views,
    round(avg_forwards, 2) as avg_forwards,
    round(median_forwards, 2) as median_forwards,
    round(avg_message_length, 2) as avg_message_length,
    round(views_stddev, 2) as views_stddev
from category_stats
order by avg_views desc;


-- 2. Which channels use more visual content?
with channel_visual_content as (
    select 
        dc.channel_name,
        dc.channel_type,
        count(*) as total_messages,
        count(img.message_id) as messages_with_images,
        count(case when img.calculated_category is not null then 1 end) as images_analyzed,
        round(count(img.message_id) * 100.0 / count(*), 2) as image_percentage,
        round(count(case when img.calculated_category is not null then 1 end) * 100.0 / count(*), 2) as analyzed_percentage
    from {{ ref('dim_channels') }} dc
    left join {{ ref('fct_messages') }} fm on dc.channel_key = fm.channel_key
    left join {{ ref('fct_image_detections') }} img on fm.message_id = img.message_id
    group by dc.channel_name, dc.channel_type
)

select 
    channel_name,
    channel_type,
    total_messages,
    messages_with_images,
    images_analyzed,
    image_percentage,
    analyzed_percentage
from channel_visual_content
order by image_percentage desc;


-- 3. Image category distribution by channel
with channel_categories as (
    select 
        dc.channel_name,
        img.calculated_category,
        count(*) as category_count,
        round(count(*) * 100.0 / sum(count(*)) over (partition by dc.channel_name), 2) as category_percentage
    from {{ ref('dim_channels') }} dc
    join {{ ref('fct_image_detections') }} img on dc.channel_key = img.channel_key
    where img.calculated_category is not null
    group by dc.channel_name, img.calculated_category
)

select 
    channel_name,
    calculated_category,
    category_count,
    category_percentage
from channel_categories
order by channel_name, category_percentage desc;


-- 4. Detection quality analysis
with detection_quality as (
    select 
        img.confidence_level,
        img.detection_density,
        img.total_detections,
        img.max_confidence,
        img.avg_confidence,
        fm.view_count,
        dc.channel_name
    from {{ ref('fct_image_detections') }} img
    join {{ ref('fct_messages') }} fm on img.message_id = fm.message_id
    join {{ ref('dim_channels') }} dc on img.channel_key = dc.channel_key
)

select 
    confidence_level,
    detection_density,
    count(*) as image_count,
    round(avg(total_detections), 2) as avg_detections,
    round(avg(max_confidence), 4) as avg_max_confidence,
    round(avg(avg_confidence), 4) as avg_confidence,
    round(avg(view_count), 2) as avg_views
from detection_quality
group by confidence_level, detection_density
order by confidence_level, detection_density;


-- 5. Temporal trends in visual content
with temporal_visual_trends as (
    select 
        dd.year,
        dd.month,
        dd.month_name,
        img.calculated_category,
        count(*) as post_count,
        avg(fm.view_count) as avg_views,
        avg(fm.forward_count) as avg_forwards
    from {{ ref('fct_image_detections') }} img
    join {{ ref('fct_messages') }} fm on img.message_id = fm.message_id
    join {{ ref('dim_dates') }} dd on img.date_key = dd.date_key
    where img.calculated_category is not null
    group by dd.year, dd.month, dd.month_name, img.calculated_category
)

select 
    year,
    month_name,
    calculated_category,
    post_count,
    round(avg_views, 2) as avg_views,
    round(avg_forwards, 2) as avg_forwards
from temporal_visual_trends
order by year, month, avg_views desc;


-- 6. Top detected objects by channel
with top_objects as (
    select 
        dc.channel_name,
        img.top_class,
        img.top_confidence,
        img.calculated_category,
        fm.view_count
    from {{ ref('fct_image_detections') }} img
    join {{ ref('fct_messages') }} fm on img.message_id = fm.message_id
    join {{ ref('dim_channels') }} dc on img.channel_key = dc.channel_key
    where img.top_class is not null
),

object_stats as (
    select 
        channel_name,
        top_class,
        count(*) as detection_count,
        round(avg(top_confidence), 4) as avg_confidence,
        round(avg(view_count), 2) as avg_views
    from top_objects
    group by channel_name, top_class
)

select 
    channel_name,
    top_class,
    detection_count,
    avg_confidence,
    avg_views
from object_stats
order by channel_name, detection_count desc;
