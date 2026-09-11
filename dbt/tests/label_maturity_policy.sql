{{ config(tags=['temporal'], severity='error') }}

{# Python owns this operational registry; it is not a dbt model. Compile sets
   execute=true even without a connection, so inspect only during test/build.
   Snowflake has no registry. #}
{% set maturity_registry = none %}
{% if execute and target.type == 'postgres' and flags.WHICH in ['test', 'build'] %}
    {% set maturity_registry = adapter.get_relation(
        database=target.database,
        schema='modeled',
        identifier='label_maturity_watermarks'
    ) %}
{% endif %}

{% if maturity_registry is not none %}

with watermarks as (

    select
        *,
        count(*) filter (where is_current) over () as current_count
    from {{ maturity_registry }}

)

select watermark_version
from watermarks
where current_count <> 1
   or quantile is distinct from 0.995
   or confidence is distinct from 0.95
   or cohort_count is distinct from 12
   or watermark_version is distinct from metadata->>'watermark_version'
   or label_definition is distinct from metadata->>'label_definition'
   or calculation_version is distinct from metadata->>'calculation_version'
   or quantile is distinct from (metadata->>'quantile')::numeric
   or confidence is distinct from (metadata->>'confidence')::numeric
   or data_as_of is distinct from (metadata->>'data_as_of')::date
   or computed_at is distinct from (metadata->>'computed_at')::timestamptz
   or grace_days is distinct from (metadata->>'grace_days')::integer
   or sample_size is distinct from (metadata->>'sample_size')::bigint
   or cohort_count is distinct from jsonb_array_length(metadata->'cohorts')
   or cohort_start is distinct from
       (metadata->'cohorts'->0->>'event_month')::date
   or cohort_end is distinct from
       (metadata->'cohorts'->11->>'event_month')::date
   or cohort_start is distinct from date_trunc('month', cohort_start)::date
   or cohort_end is distinct from date_trunc('month', cohort_end)::date
   or cohort_start is distinct from (cohort_end - interval '11 months')::date
   or (
       cohort_end + interval '1 month' - interval '1 day' + interval '9 months'
   )::date > data_as_of
   or (
       cohort_end + interval '2 months' - interval '1 day' + interval '9 months'
   )::date <= data_as_of
   or sample_size is distinct from (
       select sum((cohort->>'incident_count')::bigint)
       from jsonb_array_elements(metadata->'cohorts') as entries(cohort)
   )
   or exists (
       select 1
       from jsonb_array_elements(metadata->'cohorts')
            with ordinality as entries(cohort, position)
       where (cohort->>'event_month')::date is distinct from
           (cohort_start + (position - 1) * interval '1 month')::date
   )

{# A measured grace over nine months is valid evidence that training must stop.
   This guard checks measurement consistency, not training eligibility. #}
{% else %}

-- No registry validation outside PostgreSQL test/build or without a registry.
-- This does not authorize labels.
select cast(null as {{ dbt.type_string() }}) as watermark_version
where false

{% endif %}
