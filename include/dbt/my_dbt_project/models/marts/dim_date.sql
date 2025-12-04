with cte as (
select
    signup_date :: date AS date_key
from {{ ref('stg_customers') }}

union 

select 
    request_date :: date AS date_key
from {{ ref('stg_media_complaints') }}

union

select 
    NULLIF(resolution_date, ''):: date AS date_key
from {{ ref('stg_media_complaints') }}

union 

select
    media_complaint_generation_date:: date AS date_key
from {{ ref('stg_media_complaints') }}

union

select 
    call_start_time::date AS date_key
from {{ ref('stg_call_center') }}

union 

select 
    call_end_time::date AS date_key 
from {{ ref('stg_call_center') }}

union 

select 
    call_logs_generation_date::date AS date_key
from {{ ref('stg_call_center') }}

union 

select 
    request_date:: date AS date_key
from {{ ref('stg_web_forms') }}

union 
select 
    NULLIF(resolution_date, ''):: date AS date_key 
from {{ ref('stg_web_forms') }}

union 

select 
    web_form_generation_date:: date AS date_key   
from {{ ref('stg_web_forms') }}
)

select 
    date_key,
    EXTRACT(DAY FROM date_key) AS day,
    EXTRACT(MONTH FROM date_key) AS month,
    EXTRACT(YEAR FROM date_key) AS year,
    EXTRACT(DOW FROM date_key) AS day_of_week,
    EXTRACT(DOY FROM date_key) AS day_of_year,
    CASE 
        WHEN EXTRACT(MONTH FROM date_key) IN (1,2,3) THEN 'Q1'
        WHEN EXTRACT(MONTH FROM date_key) IN (4,5,6) THEN 'Q2'
        WHEN EXTRACT(MONTH FROM date_key) IN (7,8,9) THEN 'Q3'
        WHEN EXTRACT(MONTH FROM date_key) IN (10,11,12) THEN 'Q4'
    END AS quarter
from cte