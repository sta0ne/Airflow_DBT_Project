select
    manufacturer_id,
    manufacturer_name,
    country,
    founded_year,
    is_active,
    created_timestamp,
    updated_timestamp
from {{ ref('manufacturers_t') }}