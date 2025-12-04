select 
    customer_id,
    name,
    gender,
    date_of_birth,
    signup_date,
    email,
    address
from {{ ref('stg_customers') }}