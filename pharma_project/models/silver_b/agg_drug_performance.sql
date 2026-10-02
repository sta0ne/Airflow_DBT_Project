with rx as (
    select drug_id,
           count(*)                                              as total_prescriptions,
           sum(quantity)                                         as total_qty_prescribed,
           sum(case when status = 'Active' then 1 else 0 end)    as active_prescriptions
    from {{ ref('fct_prescriptions') }}
    group by drug_id
),

sales as (
    select drug_id,
           count(*)             as total_sales,
           sum(total_amount)    as total_revenue
    from {{ ref('fct_sales') }}
    where status = 'Completed'
    group by drug_id
),

ae as (
    select drug_id,
           count(*)                                              as total_adverse_events,
           sum(case when severity = 'Severe' then 1 else 0 end)  as severe_events
    from {{ ref('fct_adverse_events') }}
    group by drug_id
)

select
    d.drug_id,
    d.drug_name,
    d.manufacturer_name,
    coalesce(rx.total_prescriptions, 0)    as total_prescriptions,
    coalesce(sales.total_sales, 0)         as total_sales,
    coalesce(sales.total_revenue, 0)       as total_revenue,
    coalesce(ae.total_adverse_events, 0)   as total_adverse_events,
    coalesce(ae.severe_events, 0)          as severe_events
from {{ ref('dim_drugs') }} d
left join rx     on d.drug_id = rx.drug_id
left join sales  on d.drug_id = sales.drug_id
left join ae     on d.drug_id = ae.drug_id