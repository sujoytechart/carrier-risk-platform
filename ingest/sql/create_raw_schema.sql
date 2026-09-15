create schema if not exists raw;

create table if not exists raw.snapshot_batches (
    batch_id text primary key,
    feed_name text not null check (feed_name in ('crashes', 'inspections')),
    dataset_id text not null,
    source_url text not null,
    observed_at timestamptz not null,
    object_key text not null,
    row_count bigint not null check (row_count >= 0),
    uncompressed_bytes bigint not null check (uncompressed_bytes >= 0),
    compressed_bytes bigint not null check (compressed_bytes >= 0),
    content_sha256 text not null,
    object_sha256 text not null,
    schema_fingerprint text not null,
    source_columns jsonb not null,
    status text not null check (status in ('loading', 'loaded')),
    loaded_at timestamptz
);

create table if not exists raw.row_quarantine (
    batch_id text not null,
    source_row_number bigint not null,
    feed_name text not null,
    source_payload jsonb not null,
    parse_reasons text[] not null check (cardinality(parse_reasons) > 0),
    primary key (batch_id, source_row_number)
);
