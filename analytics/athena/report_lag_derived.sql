-- Queries validated Parquet derivatives; original CSV snapshots remain unchanged.
-- Describe source-row report lag for one complete snapshot per feed.
-- Replace only the two sentinel dates below with cataloged acquisition partitions.
-- This is historical source-proxy profiling, not the Phase 3 incident watermark:
-- it uses ADD_DATE/MCMIS_ADD_DATE plus one publication day, never observation time.
with selected_snapshots (feed, acquisition_date) as (
    values
        ('crashes', date '1970-01-01'),
        ('inspections', date '1970-01-01')
),

manifest_rows as (
    select
        m.feed,
        m.acquisition_date,
        arbitrary(m.observed_at) as manifest_observed_at,
        arbitrary(m.row_count) as manifest_row_count
    from derived_snapshot_metadata m
    join selected_snapshots s
      on m.feed = s.feed
     and m.acquisition_date = cast(s.acquisition_date as varchar)
    group by m.feed, m.acquisition_date
),

source_rows as (
    select
        'crashes' as feed,
        r.acquisition_date,
        r.report_date as raw_event_date,
        r.add_date as raw_source_add_at
    from derived_crashes r
    join selected_snapshots s
      on s.feed = 'crashes'
     and r.acquisition_date = cast(s.acquisition_date as varchar)

    union all

    select
        'inspections' as feed,
        r.acquisition_date,
        r.insp_date as raw_event_date,
        r.mcmis_add_date as raw_source_add_at
    from derived_inspections r
    join selected_snapshots s
      on s.feed = 'inspections'
     and r.acquisition_date = cast(s.acquisition_date as varchar)
),

parsed_rows as (
    select
        *,
        case
            when regexp_like(trim(raw_event_date), '^[0-9]{8}$')
                then case
                    when cast(substr(trim(raw_event_date), 1, 4) as integer)
                             between 1 and 9999
                        then try(cast(
                            date_parse(trim(raw_event_date), '%Y%m%d') as date
                        ))
                end
        end as event_date,
        case
            when regexp_like(trim(raw_source_add_at), '^[0-9]{8} [0-9]{4}$')
                then case
                    when cast(substr(trim(raw_source_add_at), 1, 4) as integer)
                             between 1 and 9999
                        then try(date_parse(
                            trim(raw_source_add_at), '%Y%m%d %H%i'
                        ))
                end
        end as source_add_at
    from source_rows
),

lag_rows as (
    select
        feed,
        acquisition_date,
        date_diff(
            'day',
            event_date,
            date(source_add_at + interval '1' day)
        ) as report_lag_days
    from parsed_rows
    where event_date is not null
      and source_add_at is not null
      and date(source_add_at + interval '1' day) >= event_date
),

lag_summaries as (
    select
        feed,
        acquisition_date,
        count(*) as lag_sample_size,
        min(report_lag_days) as minimum_lag_days,
        max(report_lag_days) as maximum_lag_days,
        approx_percentile(report_lag_days, 0.50) as p50_lag_days,
        approx_percentile(report_lag_days, 0.90) as p90_lag_days,
        approx_percentile(report_lag_days, 0.95) as p95_lag_days,
        approx_percentile(report_lag_days, 0.99) as p99_lag_days,
        approx_percentile(report_lag_days, 0.995) as p995_lag_days
    from lag_rows
    group by feed, acquisition_date
),

lag_histogram as (
    select
        feed,
        acquisition_date,
        case
            when report_lag_days = 0 then '00 same day'
            when report_lag_days <= 2 then '01 1-2 days'
            when report_lag_days <= 7 then '02 3-7 days'
            when report_lag_days <= 14 then '03 8-14 days'
            when report_lag_days <= 30 then '04 15-30 days'
            when report_lag_days <= 60 then '05 31-60 days'
            when report_lag_days <= 90 then '06 61-90 days'
            when report_lag_days <= 180 then '07 91-180 days'
            when report_lag_days <= 270 then '08 181-270 days'
            else '09 271+ days'
        end as lag_bucket,
        count(*) as lag_bucket_row_count
    from lag_rows
    group by
        feed,
        acquisition_date,
        case
            when report_lag_days = 0 then '00 same day'
            when report_lag_days <= 2 then '01 1-2 days'
            when report_lag_days <= 7 then '02 3-7 days'
            when report_lag_days <= 14 then '03 8-14 days'
            when report_lag_days <= 30 then '04 15-30 days'
            when report_lag_days <= 60 then '05 31-60 days'
            when report_lag_days <= 90 then '06 61-90 days'
            when report_lag_days <= 180 then '07 91-180 days'
            when report_lag_days <= 270 then '08 181-270 days'
            else '09 271+ days'
        end
)

select
    h.feed,
    h.acquisition_date,
    m.manifest_observed_at,
    m.manifest_row_count,
    'source_proxy' as availability_quality,
    s.lag_sample_size,
    s.minimum_lag_days,
    s.maximum_lag_days,
    s.p50_lag_days,
    s.p90_lag_days,
    s.p95_lag_days,
    s.p99_lag_days,
    s.p995_lag_days,
    h.lag_bucket,
    h.lag_bucket_row_count,
    cast(h.lag_bucket_row_count as double) / s.lag_sample_size
        as lag_bucket_fraction
from lag_histogram h
join lag_summaries s
  on s.feed = h.feed
 and s.acquisition_date = h.acquisition_date
join manifest_rows m
  on m.feed = h.feed
 and m.acquisition_date = h.acquisition_date
order by h.feed, h.lag_bucket;
