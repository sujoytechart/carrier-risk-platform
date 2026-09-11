# Phase 3 evidence

These files contain aggregate measurements and synthetic validation results.
They contain no federal source rows, carrier-level training dataset, credentials,
or private process instructions.

- [Empirical maturity](maturity-september-3-source-proxy.json): full retained
  snapshot scan, twelve cohort estimates, exclusions, provenance, deterministic
  bootstrap configuration, and repeat-run evidence. The effective grace is 495
  days.
- [Training gate](training-gate.json): actual MLflow skip before fitting, with no
  registered real-data model.
- [Monthly replay](monthly-skip-replay.json): the measured policy replayed through
  PostgreSQL and MLflow, retaining one current immutable registry version.
- [Independent dbt guard](maturity-dbt-negative-test.json): a deliberately changed
  summary fails; restoring the measured value passes.
- [Production-profile response](production-model-unavailable.json): actual
  PostgreSQL/MLflow lookups return `model_unavailable`, without a score.
- [Quality record](quality.json): full and final component test counts,
  exact coverage, source fingerprint, installed versions and scan results.
- [Local cleanup](local-cleanup.json): temporary API and PostgreSQL teardown,
  with original snapshots and existing services untouched.
- [Latency evidence and investigation](latency-investigation.md): the complete
  failed throughput curve, raw timings and hashes, and bounded follow-up
  diagnostics. The 200-rps latency target remains unmet.
- [ASGI timing correlation](asgi-probe/correlation-summary.json): client and
  framework boundary timings from a bounded instrumented diagnostic, with the
  [failed recording attempt](asgi-probe/attempt-1/summary.json),
  [recovery summary](asgi-probe/recovery/summary.json), and
  [compressed traces, exact sources and SHA-256 manifest](asgi-probe/manifest.json).

See the [verification report](../../phase-3-verification.md) for commands,
coverage provenance, latency scope, and limitations. Synthetic scores establish
the serving path only; they do not establish predictive quality on federal data.

The [two-worker configuration trial](latency-two-worker-trial.json), its
[complete request records and resource evidence](latency-two-worker-trial-files.json),
and the [investigation notes](latency-investigation.md) retain the rejected worker
isolation experiment. Its required 200-rps stage also failed; it does not replace
the original complete curve.
