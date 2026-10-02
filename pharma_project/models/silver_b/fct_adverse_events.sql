select
    event_id,
    drug_id,
    patient_id,
    event_date,
    reaction,
    severity,
    outcome,
    status,
    created_timestamp,
    updated_timestamp
from {{ ref('adverse_events_t') }}