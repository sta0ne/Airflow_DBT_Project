{% snapshot sales_snapshot %}
{{ config(target_schema='snapshots', unique_key='sale_id',
          strategy='check', check_cols=['status'],
          invalidate_hard_deletes=true) }}
select * from {{ source('pharma_databricks', 'sales') }}
{% endsnapshot %}