{% snapshot adverse_events_snapshot %}
{{ config(target_schema='snapshots', unique_key='event_id',
          strategy='check', check_cols=['status', 'outcome'],
          invalidate_hard_deletes=true) }}
select * from {{ source('pharma_databricks', 'adverse_events') }}
{% endsnapshot %}