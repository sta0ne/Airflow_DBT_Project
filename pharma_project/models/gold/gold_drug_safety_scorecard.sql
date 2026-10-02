with prescribed as (
    select drug_id, count(*) as total_prescriptions
    from {{ ref('fct_prescriptions') }}
    group by drug_id
),

events as (
    select
        drug_id,
        count(*)                                                     as total_adverse_events,
        sum(case when severity = 'Severe' then 1 else 0 end)         as severe_events,
        sum(case when outcome = 'Fatal' then 1 else 0 end)           as fatal_events
    from {{ ref('fct_adverse_events') }}
    group by drug_id
)

select
    d.drug_id,
    d.drug_name,
    d.manufacturer_name,
    d.atc_category,
    coalesce(p.total_prescriptions, 0)   as total_prescriptions,
    coalesce(e.total_adverse_events, 0)  as total_adverse_events,
    coalesce(e.severe_events, 0)         as severe_events,
    coalesce(e.fatal_events, 0)          as fatal_events,
    case when coalesce(p.total_prescriptions, 0) > 0
         then round(coalesce(e.total_adverse_events, 0) * 100.0 / p.total_prescriptions, 2)
         else null
    end                                   as adverse_event_rate_pct
from {{ ref('dim_drugs') }} d
left join prescribed p on d.drug_id = p.drug_id
left join events     e on d.drug_id = e.drug_id