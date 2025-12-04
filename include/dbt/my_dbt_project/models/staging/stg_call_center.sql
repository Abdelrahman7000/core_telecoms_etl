select 
    call_id,
    customer_id,
    complaint_category,
    agent_id,
    call_start_time ::timestamp,
    call_end_time ::timestamp,
    resolution_status,
    call_logs_generation_date,
    ingestion_timestamp ::timestamp,
    source_system,
    file_name
from {{ source('source', 'call_center_logs') }}