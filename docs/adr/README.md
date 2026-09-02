# Architecture decision records

Short records of decisions that were not obvious, and that a future reader would
otherwise have to reverse-engineer from the code.

Each one states what was decided, what it cost, and what would have to change for
the decision to be revisited.

| # | Decision | Status |
|---|---|---|
| [0001](0001-event-versions-over-dbt-snapshots.md) | Incremental `event_versions` instead of dbt snapshots | Accepted |
| [0002](0002-exclude-historical-carrier-attributes.md) | Historical carrier attributes excluded from features | Accepted |
| [0003](0003-six-month-feature-and-label-windows.md) | Six-month inspection and label windows | Accepted |
| [0004](0004-crash-eligibility-and-deduplication.md) | Crash eligibility rules and the carrier incident key | Accepted |
| [0005](0005-snowflake-for-portability-not-scale.md) | Snowflake as a portability target, not a scale decision | Accepted |
