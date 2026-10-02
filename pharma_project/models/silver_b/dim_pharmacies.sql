select
    pharmacy_id,
    pharmacy_name,
    city,
    state,
    region,
    created_timestamp,
    updated_timestamp,  
    is_active
from {{ ref('pharmacies_t') }}