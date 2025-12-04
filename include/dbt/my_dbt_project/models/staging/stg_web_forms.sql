select 
    request_id,
    customer_id,
    complaint_category,
    agent_id,
    resolution_status,
    request_date,
    resolution_date,
    web_form_generation_date,
    ingestion_timestamp,
    source_system,
    source_table
from {{ source('source', 'web_forms') }}