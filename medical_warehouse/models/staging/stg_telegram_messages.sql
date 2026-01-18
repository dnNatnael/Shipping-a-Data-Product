{{--
    Staging model for telegram messages
    Cleans and standardizes raw telegram data
--}}

with source as (

    select * from {{ source('raw', 'telegram_messages') }}

),

renamed as (

    select
        -- Convert message_id to integer
        message_id,

        -- Clean channel name
        trim(lower(channel_name)) as channel_name,

        -- Parse and standardize message_date
        message_date,

        -- Clean message text
        case 
            when message_text is null or trim(message_text) = '' then null
            else trim(message_text)
        end as message_text,

        -- Convert boolean fields
        coalesce(has_media, false) as has_media,

        -- Clean image path
        case 
            when image_path is null or trim(image_path) = '' then null
            else trim(image_path)
        end as image_path,

        -- Convert numeric fields with defaults
        coalesce(views, 0) as view_count,
        coalesce(forwards, 0) as forward_count,

        -- Parse scraped_at
        scraped_at,

        -- Add calculated fields
        length(coalesce(message_text, '')) as message_length,
        case 
            when image_path is not null and trim(image_path) != '' then true
            else false
        end as has_image,

        -- Extract date parts for easier analysis
        date(message_date) as message_date_only,
        extract(year from message_date) as message_year,
        extract(month from message_date) as message_month,
        extract(day from message_date) as message_day,
        extract(dow from message_date) as day_of_week,

        -- Metadata
        file_path,
        loaded_at

    from source

),

filtered as (

    select *
    from renamed
    where 
        -- Filter out invalid records
        message_id is not null 
        and channel_name is not null
        and message_date is not null
        and message_date <= current_timestamp  -- No future messages
        and message_date >= '2020-01-01'      -- Reasonable date range

)

select * from filtered
