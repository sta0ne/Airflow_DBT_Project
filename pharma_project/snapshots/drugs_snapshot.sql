{% snapshot drugs_snapshot %}
{{ config(target_schema='snapshots', unique_key='drug_id',
          strategy='timestamp', updated_at='updated_timestamp',
          invalidate_hard_deletes=true) }}
select * from {{ source('pharma_databricks', 'drugs') }}
{% endsnapshot %}
