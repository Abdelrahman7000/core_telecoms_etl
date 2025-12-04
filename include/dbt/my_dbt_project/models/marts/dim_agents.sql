select 
    agent_id,
    agent_name,
    experience,
    state
from {{ ref('stg_agents') }}