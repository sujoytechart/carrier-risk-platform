# carrier-risk-platform

Risk scoring for US motor carriers, built so that every prediction uses only the
records that were actually available on the date the decision would have been made.

## Background

Freight brokers can face negligent hiring liability when a carrier they dispatch
is involved in a crash. The Federal Motor Carrier Safety Administration (FMCSA)
publishes carrier inspection and crash histories. Inspection rows already include
violation and out-of-service totals. This project turns that public data into a
carrier risk score, helping brokers assess safety risk before dispatching a load.

## The problem it solves

Federal safety scores are recalculated each month using the previous two years of
inspections and crashes. That creates a leakage problem when those scores are used
to train a model.

Say you're trying to predict whether a carrier would crash last March.
If you train on the carrier's current safety score, that score already includes
the March crash. The model learns that carriers with bad scores tend to crash,
tests well, and isn't much help with the question that matters: which carriers
are risky before they crash?

Fixing this requires two date filters. Every event has a date when it happened,
and a date when it showed up in federal data. Those dates can be weeks apart:

```sql
where event_date    < scoring_date   -- it had happened
  and reported_date < scoring_date   -- it was visible
```

Checking only the event date still leaks information.
An inspection or crash may have already happened but not yet been reported.
At the scoring date, the model couldn't have known about it.

So each historical score has to be built from only the information that was
actually available on that date. Most implementations get the first filter
right and miss the second.

## Approach

* Ingest daily inspection and crash feeds as immutable snapshots
* Preserve corrected event versions so historical scoring uses the version that
  was knowable at the time
* Exclude present-day carrier attributes from historical features because no
  confirmed public archive provides their past versions
* Generate features for each carrier and scoring date using both the occurrence and
  reporting-date filters above
* Build labels from a forward-looking window, and only admit a row into training once
  that window has fully elapsed
* Enforce temporal correctness with tests that fail the build if future information
  leaks into a historical row
* Make every load idempotent, since carriers can dispute records and federal history
  can change after publication

## Architecture

![Carrier Risk Platform architecture](docs/architecture.svg)

## Data

V0 uses FMCSA's Vehicle Inspection File and Crash File from the DOT open data
portal. Violation and out-of-service features come from totals on each inspection
row, so a separate violation feed is not ingested. US government work, public
domain. Raw data is not committed; tests use generated fixtures instead.

## Landing a snapshot

Authenticate to AWS and create an account-level budget before provisioning the
Phase 0 resources. Then:

```bash
python3.12 -m venv .venv
.venv/bin/pip install -e '.[dev]'
terraform -chdir=infra/base init
terraform -chdir=infra/base apply
export CARRIER_RISK_RAW_BUCKET="$(terraform -chdir=infra/base output -raw raw_bucket_name)"
export CARRIER_RISK_INGEST_ROLE_ARN="$(terraform -chdir=infra/base output -raw ingest_role_arn)"
.venv/bin/python -m ingest.landing --feed inspections
.venv/bin/python -m ingest.landing --feed crashes
```

Each feed is written to a deterministic UTC-day partition. The data object is
checksummed and validated before its manifest is published as the commit marker;
rerunning a completed partition is a no-op.

## Stack

Airflow, dbt-core, Postgres, MLflow, FastAPI, Terraform, AWS (S3, SQS, RDS, Glue,
Athena).

## Status

Phase 0 is complete. The tested landing workflow and Terraform baseline have
landed both full event feeds in AWS and passed a completed-partition no-op rerun.
Warehouse loading and temporal models begin in Phase 1.

## License

MIT
