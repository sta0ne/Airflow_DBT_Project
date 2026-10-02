select 
    prescription_id,
    patient_id,
    doctor_id,
    drug_id,
    pharmacy_id,
    prescription_date,
    quantity,
    days_supply,
    created_timestamp,
    updated_timestamp,
    status
from {{ ref('prescriptions_t') }}