select 
    sale_id,
    sale_date,
    drug_id,
    pharmacy_id,
    quantity_sold,
    unit_price,
    total_amount,
    payment_mode,
    status,
    created_timestamp,
    updated_timestamp
from {{ ref('sales_t') }}