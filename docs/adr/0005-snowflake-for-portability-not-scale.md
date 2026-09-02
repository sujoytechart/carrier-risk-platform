# 0005. Snowflake as a portability target, not a scale decision

Date: 2026-09-02
Status: Accepted

## Context

The expected data volume is in the tens of millions of source rows. Postgres can
handle that workload comfortably. The project does not need Snowflake for scale,
high concurrency, or streaming ingestion.

Using Snowflake as a performance solution would add cost and complexity without
solving a real problem at this volume.

## Decision

Postgres on RDS is the primary warehouse. Snowflake is a second dbt target used
to verify that the transformation layer is not tied to one SQL engine.

Every model compiles and every test passes against both, verified by running the
suite against each. Adapter divergences are recorded in
`docs/adapter-differences.md`.

Snowflake validation uses a trial account, and the README states that limit.

## Consequences

- Real adapter work: incremental merge strategies, `qualify` support, and date
  arithmetic differ between the two, and each difference is documented rather
  than worked around silently.
- Two targets to keep green, which is ongoing cost on every model change.
- Passing on both targets demonstrates SQL portability, not equal performance.
  Each warehouse would still need its own tuning at larger scale.

## Revisit if

Maintaining the second target starts costing more than the portability
demonstrates, or a warehouse-specific feature becomes worth the lock-in.
