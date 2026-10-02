select
    patient_id,
    age,
    gender,
    city,
    state,
    insurance_type,
    created_timestamp,
    updated_timestamp,
    is_active
from {{ ref('patients_t') }}

      