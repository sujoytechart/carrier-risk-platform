{{ config(enabled=var('learning_demo', false)) }}

select * from {{ ref('demo_training_features') }}
where inspections_4m < 1 or violations_4m < 0 or oos_violations_4m < 0
    or crashes_24m < 0 or days_since_last_inspection < 1
    or (violations_4m = 0 and oos_violation_rate is not null)
    or (violations_4m > 0 and oos_violation_rate is null)
    or (scoring_date < date '2026-09-01' and label is null)
    or (scoring_date = date '2026-09-01' and label is not null)
