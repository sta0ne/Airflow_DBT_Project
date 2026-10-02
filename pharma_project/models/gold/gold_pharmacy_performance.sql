select
    ph.pharmacy_id,
    ph.pharmacy_name,
    ph.city,
    ph.region,
    count(*)                            as total_sales,
    sum(s.total_amount)                 as total_revenue,
    sum(case when s.status = 'Refunded' then 1 else 0 end)  as refund_count,
    round(
        sum(case when s.status = 'Refunded' then 1 else 0 end) * 100.0 / count(*), 2
    )                                    as refund_rate_pct
from {{ ref('fct_sales') }} s
join {{ ref('dim_pharmacies') }} ph on s.pharmacy_id = ph.pharmacy_id
group by ph.pharmacy_id, ph.pharmacy_name, ph.city, ph.region