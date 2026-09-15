-- Reconcile one explicitly selected complete snapshot per feed with its manifest.
-- Replace only the two sentinel dates below with cataloged acquisition partitions.
-- The query runs in the configured Athena catalog database; no bucket is embedded.
with selected_snapshots (feed, acquisition_date) as (
    values
        ('crashes', date '1970-01-01'),
        ('inspections', date '1970-01-01')
),

manifest_rows as (
    select
        m.feed,
        m.acquisition_date,
        count(*) as metadata_row_count,
        arbitrary(m.batch_id) as batch_id,
        arbitrary(m.observed_at) as manifest_observed_at,
        arbitrary(m.row_count) as manifest_row_count,
        arbitrary(m.content_sha256) as content_sha256,
        arbitrary(m.object_sha256) as object_sha256,
        arbitrary(m.schema_fingerprint) as schema_fingerprint
    from raw_snapshot_metadata m
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
    from raw_crashes r
    join selected_snapshots s
      on s.feed = 'crashes'
     and r.acquisition_date = cast(s.acquisition_date as varchar)

    union all

    select
        'inspections' as feed,
        r.acquisition_date,
        r.insp_date as raw_event_date,
        r.mcmis_add_date as raw_source_add_at
    from raw_inspections r
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

classified_rows as (
    select
        *,
        date(source_add_at + interval '1' day) as source_proxy_reported_date
    from parsed_rows
),

catalog_counts as (
    select
        feed,
        acquisition_date,
        count(*) as catalog_row_count,
        count_if(coalesce(trim(raw_event_date), '') = '')
            as missing_event_date_count,
        count_if(
            coalesce(trim(raw_event_date), '') <> '' and event_date is null
        ) as invalid_event_date_count,
        count_if(coalesce(trim(raw_source_add_at), '') = '')
            as missing_source_add_at_count,
        count_if(
            coalesce(trim(raw_source_add_at), '') <> '' and source_add_at is null
        ) as invalid_source_add_at_count,
        count_if(
            event_date is null
            or source_add_at is null
            or source_proxy_reported_date < event_date
        ) as excluded_from_lag_count,
        count_if(
            event_date is not null
            and source_add_at is not null
            and source_proxy_reported_date < event_date
        ) as negative_lag_count,
        count_if(
            event_date is not null
            and source_add_at is not null
            and source_proxy_reported_date >= event_date
        ) as lag_sample_size
    from classified_rows
    group by feed, acquisition_date
)

select
    s.feed,
    cast(s.acquisition_date as varchar) as acquisition_date,
    coalesce(m.metadata_row_count, 0) as metadata_row_count,
    m.batch_id,
    m.manifest_observed_at,
    'source_proxy' as availability_quality,
    m.content_sha256,
    m.object_sha256,
    m.schema_fingerprint,
    m.manifest_row_count,
    coalesce(c.catalog_row_count, 0) as catalog_row_count,
    coalesce(c.catalog_row_count, 0) - coalesce(m.manifest_row_count, 0)
        as row_count_delta,
    coalesce(c.missing_event_date_count, 0) as missing_event_date_count,
    coalesce(c.invalid_event_date_count, 0) as invalid_event_date_count,
    coalesce(c.missing_source_add_at_count, 0) as missing_source_add_at_count,
    coalesce(c.invalid_source_add_at_count, 0) as invalid_source_add_at_count,
    coalesce(c.excluded_from_lag_count, 0) as excluded_from_lag_count,
    coalesce(c.negative_lag_count, 0) as negative_lag_count,
    coalesce(c.lag_sample_size, 0) as lag_sample_size
from selected_snapshots s
left join manifest_rows m
  on m.feed = s.feed
 and m.acquisition_date = cast(s.acquisition_date as varchar)
left join catalog_counts c
  on c.feed = s.feed
 and c.acquisition_date = cast(s.acquisition_date as varchar)
order by s.feed;
