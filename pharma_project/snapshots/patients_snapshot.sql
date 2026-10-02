{% snapshot patients_snapshot %}
{{ config(target_schema='snapshots', unique_key='patient_id',
          strategy='timestamp', updated_at='updated_timestamp',
          invalidate_hard_deletes=true) }}
select * from {{ source('pharma_databricks', 'patients') }}
{% endsnapshot %}