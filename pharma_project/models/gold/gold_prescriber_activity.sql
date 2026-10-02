select
    doc.doctor_id,
    doc.doctor_name,
    doc.specialization,
    doc.city,
    count(*)                                                       as total_prescriptions_written,
    count(distinct rx.patient_id)                                  as unique_patients,
    count(distinct rx.drug_id)                                     as unique_drugs_prescribed,
    sum(case when rx.status = 'Cancelled' then 1 else 0 end)       as cancelled_prescriptions
from {{ ref('fct_prescriptions') }} rx
join {{ ref('dim_doctors') }} doc on rx.doctor_id = doc.doctor_id
group by doc.doctor_id, doc.doctor_name, doc.specialization, doc.city