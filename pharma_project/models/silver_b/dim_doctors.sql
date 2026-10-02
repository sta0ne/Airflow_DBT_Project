select
    doctor_id,
    doctor_name,
    specialization,
    city,
    years_experience,
    created_timestamp,
    updated_timestamp,
    is_active
from {{ ref('doctors_t') }}