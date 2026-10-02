select
    d.drug_id,
    d.drug_name,
    d.generic_name,
    d.atc_category,
    d.category_description,
    d.dosage_form,
    d.strength_mg,
    d.prescription_status,
    d.unit_price,
    m.manufacturer_id,
    m.manufacturer_name,
    m.country           as manufacturer_country,
    d.is_active,
    d.created_timestamp,
    d.updated_timestamp
from {{ ref('drugs_t') }} d
left join {{ ref('manufacturers_t') }} m
       on d.manufacturer_id = m.manufacturer_id