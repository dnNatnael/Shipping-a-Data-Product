{{--
    Image detections fact table
    Integrates YOLO object detection results with message data
--}}

with yolo_results as (

    select 
        message_id,
        channel_name,
        image_path,
        image_category,
        total_detections,
        person_count,
        product_count,
        max_confidence,
        avg_confidence,
        top_class,
        top_confidence,
        processing_timestamp
    from {{ source('raw', 'yolo_detections') }}

),

join_messages as (

    select 
        yolo_results.*,
        dim_channels.channel_key
    from yolo_results
    left join {{ ref('dim_channels') }} dim_channels
        on yolo_results.channel_name = dim_channels.channel_name

),

join_dates as (

    select 
        join_messages.*,
        dim_dates.date_key
    from join_messages
    left join {{ ref('fct_messages') }} fct_messages
        on join_messages.message_id = fct_messages.message_id
    left join {{ ref('dim_dates') }} dim_dates
        on fct_messages.date_key = dim_dates.date_key

),

enriched_detections as (

    select 
        message_id,
        channel_key,
        date_key,
        image_path,
        image_category,
        total_detections,
        person_count,
        product_count,
        max_confidence,
        avg_confidence,
        top_class,
        top_confidence,
        processing_timestamp,
        -- Additional calculated fields
        case 
            when person_count > 0 and product_count > 0 then 'promotional'
            when product_count > 0 and person_count = 0 then 'product_display'
            when person_count > 0 and product_count = 0 then 'lifestyle'
            else 'other'
        end as calculated_category,
        case 
            when total_detections = 0 then 'no_objects'
            when total_detections <= 2 then 'few_objects'
            when total_detections <= 5 then 'moderate_objects'
            else 'many_objects'
        end as detection_density,
        case 
            when max_confidence >= 0.8 then 'high_confidence'
            when max_confidence >= 0.5 then 'medium_confidence'
            when max_confidence > 0 then 'low_confidence'
            else 'no_confidence'
        end as confidence_level
    from join_dates

)

select 
    message_id,
    channel_key,
    date_key,
    image_path,
    image_category,
    calculated_category,
    total_detections,
    person_count,
    product_count,
    max_confidence,
    avg_confidence,
    top_class,
    top_confidence,
    detection_density,
    confidence_level,
    processing_timestamp
from enriched_detections
where channel_key is not null
order by message_id
