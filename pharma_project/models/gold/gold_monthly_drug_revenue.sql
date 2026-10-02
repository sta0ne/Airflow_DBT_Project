select
    date_trunc('month', s.sale_date)   as sale_month,
    d.drug_id,
    d.drug_name,
    d.manufacturer_name,
    d.atc_category,
    ph.region,
    count(*)                            as transaction_count,
    sum(s.quantity_sold)                as total_units_sold,
    sum(s.total_amount)                 as total_revenue,
    avg(s.total_amount)                 as avg_transaction_value
from {{ ref('fct_sales') }} s
join {{ ref('dim_drugs') }} d      on s.drug_id = d.drug_id
join {{ ref('dim_pharmacies') }} ph on s.pharmacy_id = ph.pharmacy_id
where s.status = 'Completed'
group by
    date_trunc('month', s.sale_date),
    d.drug_id, d.drug_name, d.manufacturer_name, d.atc_category, ph.region