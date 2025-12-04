select 
    call_id AS complaint_id,
    customer_id,
    agent_id,
    'call_logs' AS interaction_type,
    call_end_time AS resolution_date,
    call_start_time ::timestamp AS start_date,
    resolution_status,
    complaint_category,
    Null as media_channel
    --EXTRACT(EPOCH FROM (call_end_time - call_start_time)) AS call_duration_seconds,
    --CASE 
    --    WHEN resolution_status = 'Resolved' THEN 1
    --    ELSE 0
    ---END AS is_resolved

from {{ ref('stg_call_center') }} 

UNION ALL

select 
    complaint_id,
    customer_id,
    agent_id AS agent_id,
    'social_media' AS interaction_type,
    NULLIF(resolution_date, '')::timestamp as resolution_date,
    request_date::timestamp as start_date,
    resolution_status,
    complaint_category,
    media_channel
    --Null AS call_duration_seconds,
    --CASE 
    --    WHEN resolution_status = 'Resolved' THEN 1
    --    ELSE 0
    --END AS is_resolve
from {{ ref('stg_media_complaints') }}

UNION ALL

select 
    request_id as complaint_id,
    customer_id,
    agent_id AS agent_id,
    'web_forms' AS interaction_type,
    NULLIF(resolution_date, '')::timestamp as resolution_date,
    request_date::timestamp as start_date,
    resolution_status,
    complaint_category,
    Null As media_channel
    --Null AS call_duration_seconds,
    --CASE 
    --    WHEN resolution_status = 'Resolved' THEN 1
    --    ELSE 0
    --END AS is_resolve
from {{ ref('stg_web_forms') }} 

