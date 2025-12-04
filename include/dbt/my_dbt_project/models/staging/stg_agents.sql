select 
      id as agent_id,
      name as agent_name,
      experience,
      state 
from {{ source('source', 'agents_dataset') }}