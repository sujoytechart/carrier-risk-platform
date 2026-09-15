# 0006. Derive Parquet while preserving raw snapshot evidence

Date: 2026-09-11
Status: Accepted

## Context

Athena can query the immutable gzip CSV snapshots in place only while every
source record fits its CSV reader's narrower dialect. FMCSA exports may contain
embedded record separators and other valid CSV values that must not be discarded
or normalized. Reading large CSV snapshots repeatedly also scans more data than
the same values stored in a columnar format.

The acquisition timestamp is the authoritative knowledge clock. Changing it
during a format conversion would weaken the point-in-time guarantee even if all
row values remained intact.

## Decision

Keep the original gzip CSV object and manifest as immutable authoritative
evidence. Locally derive a Parquet copy only after validating the manifest,
compressed and uncompressed byte counts and checksums, exact header, row widths,
and row count. Every source column remains a required Parquet string with its
original uppercase name; empty strings, leading zeros, backslashes, Unicode, and
embedded CR/LF remain values rather than nulls or normalized text.

Process a configured number of rows at a time. Re-read the completed Parquet in
batches and require equal record counts, value counts, zero nulls, and equal
boundary-safe hashes of every source and Parquet value. Publish the Parquet file
into an exclusively reserved directory, then atomically publish `lineage.json`
as its completion marker. A crash can leave an incomplete directory, which a
retry rejects for explicit recovery; it cannot replace another publication. Lineage
contains the complete original manifest, unchanged `observed_at`, source schema,
converter version, output checksum, and reconciliation evidence.

## Consequences

- Athena may use the derived copy for compatible analytics while the raw gzip
  object remains the recovery and audit source.
- A retry validates an existing publication and returns an idempotent result.
  Missing, corrupt, or inconsistent files are conflicts and are never replaced.
- Local conversion needs optional PyArrow installation and temporary disk space
  for one complete Parquet file, with row batches limiting working memory; Parquet footer metadata also grows
  with the number of row groups.
- Schema order is fixed and validated. Consumers that lowercase field names can
  map by position without changing the stored source headers.

## Cloud publication

Cloud publication uses conditional immutable object creation with a final metadata
completion marker. Consumers reconcile exactly one metadata row per selected feed
and acquisition date before interpreting data. An interrupted data-only upload
is incomplete and can be completed by an exact retry; conflicting objects fail.
