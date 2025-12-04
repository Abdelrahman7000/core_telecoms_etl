select 
    complaint_id,
    customer_id,
    complaint_category,
    agent_id,
    resolution_status,
    request_date,
    resolution_date,
    media_channel,
    media_complaint_generation_date,
    ingestion_timestamp,
    source_system,
    file_name

from {{ source('source', 'media_complaints') }}