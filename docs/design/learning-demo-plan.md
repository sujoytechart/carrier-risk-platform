# Real-data learning demonstration

Approved scope: a retrospective experiment that proves the platform can train,
register and serve a real-data model. This does not replace the v0 maturity policy.

1. Freeze February 2024 training and September 2024 testing before fitting. Use
   four months of inspections, 24 months of prior crashes and six months of
   subsequent recorded federal crashes. Exclude reporting dates at/after scoring
   from features. Keep snapshot ascertainment explicit for labels.
2. Extract reconciled local Parquet into an isolated PostgreSQL demo schema;
   retain fingerprints and aggregate exclusion counts. Build contracted dbt
   features for those dates and the acquisition month's scoring date.
3. Fit the fixed estimator once. Choose a classification threshold using training
   rows only. Report accuracy, positive recall, precision, confusion counts,
   average precision and the prior-crash baseline on the untouched later cohort.
4. Register under a separate experimental MLflow identity. Add a manual Airflow
   DAG and an explicitly experimental scoring endpoint using the four-month
   feature contract. Preserve all production and synthetic isolation checks.
5. Run behavioral tests and repository quality checks. Publish aggregate evidence
   and the measured limitations in the README. Show the changes before pushing.

The retained snapshot cannot establish eventual label completeness, historical
public availability or prospective deployment performance. Repeated carriers
across periods are allowed; the holdout measures a later period, not new carriers.
