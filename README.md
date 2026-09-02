# carrier-risk-platform

Risk scoring for US motor carriers, built so that every prediction uses only the
records that were actually available on the date the decision would have been made.

## Background

Freight brokers can face negligent hiring liability when a carrier they dispatch
is involved in a crash. The Federal Motor Carrier Safety Administration (FMCSA)
publishes carrier inspection, violation, and crash histories. This project turns
that public data into a carrier risk score, helping brokers assess safety risk
before dispatching a load.

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

* Ingest three federal data feeds on their native schedules, ranging from daily to monthly
* Maintain carrier attributes as Type 2 history, so any scoring date resolves to the
  carrier record that was current at the time
* Generate features for each carrier and scoring date using both the occurrence and
  reporting-date filters above
* Build labels from a forward-looking window, and only admit a row into training once
  that window has fully elapsed
* Enforce temporal correctness with tests that fail the build if future information
  leaks into a historical row
* Make every load idempotent, since carriers can dispute records and federal history
  can change after publication

## Data

FMCSA publishes carrier census, inspection, violation, and crash files through the
DOT open data portal. US government work, public domain. Raw data is not committed;
`ingest/` downloads it and a small sample is kept for offline tests.

## Stack

Airflow, dbt-core, Postgres, MLflow, FastAPI, Terraform, AWS (S3, SQS, RDS, Glue,
Athena).

## Status

Early. Nothing here runs yet.

## License

MIT
