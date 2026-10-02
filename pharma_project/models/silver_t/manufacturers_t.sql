{{
  config(
    materialized = 'incremental',
    unique_key = 'manufacturer_id'
    )
}}
select *
    , current_timestamp() as processed_at
from {{source('pharma_databricks','manufacturers')}} 

{%if is_incremental() %}
    Where updated_timestamp  > (select COALESCE(max(updated_timestamp), '1900-01-01') from {{ this }})
{%endif%}