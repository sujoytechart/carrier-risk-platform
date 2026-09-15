# Phase 4 chart provenance

The figures present preserved Phase 2 and Phase 3 measurements. Rendering does
not download raw data, query cloud services, retrain a model, or run a load test.
The [renderer](render_charts.py) verifies the five committed input-file hashes
before drawing. [Machine-readable provenance](chart-provenance.json) records
those hashes, the renderer hash, output hashes, runtime versions, bucket counts,
quantiles, and benchmark stages.

## Reproduce

From the repository root, use Python 3.12 with Matplotlib 3.11.2, NumPy 2.5.3,
and Pillow 12.3.0. These are documentation-rendering dependencies, independent
of the application runtime. An isolated environment can be created with:

```bash
python3.12 -m venv /tmp/carrier-risk-chart-venv
/tmp/carrier-risk-chart-venv/bin/python -m pip install \
  matplotlib==3.11.2 numpy==2.5.3 Pillow==12.3.0
MPLCONFIGDIR=/tmp/carrier-risk-chart-mpl \
  XDG_CACHE_HOME=/tmp/carrier-risk-chart-cache \
  /tmp/carrier-risk-chart-venv/bin/python \
  docs/evidence/phase-4/render_charts.py
```

The command writes `docs/report_lag.png`, `docs/latency.png`, and
`docs/evidence/phase-4/chart-provenance.json`. To render a comparison without
overwriting them, append `--output-root /tmp/carrier-risk-chart-reproduction`.
No path to a local environment enters the generated provenance. The renderer
uses the noninteractive Agg backend, bundled DejaVu Sans font, 180 dpi, and
fixed PNG metadata. Two independent renders in the recorded environment
produced byte-identical images and provenance. Different rendering-library or
font environments may change pixels; numerical provenance remains reviewable.

## Report lag

[report_lag.png](../../report_lag.png) uses the `report_lag_derived` query rows
in [Athena results](../phase-2/athena-results.json), joined by feed to
`validate_derived`. The [query](../../../analytics/athena/report_lag_derived.sql)
defines every bin and the lag formula. Its sentinel acquisition dates were
substituted with the recorded `2026-09-03` partition during Phase 2 execution;
the chart does not execute or modify that SQL. The
[Parquet reconciliation](../phase-2/parquet-reconciliation.json) verifies source
row counts, conversion value hashes, and acquisition lineage; it is not itself
the histogram source.

| Source snapshot | Rows | Approximate p50 | Approximate p95 | Approximate p99 |
| --- | ---: | ---: | ---: | ---: |
| Crash file | 4,986,413 | 39 days | 897 days | 1,891 days |
| Inspection file | 8,281,794 | 3 days | 11 days | 61 days |

These are current rows in the preserved source snapshots acquired September 3,
2026. They are labeled `availability_quality = 'source_proxy'`. The proxy is
`date(ADD_DATE + 1 publication day) - REPORT_DATE` for crashes and
`date(MCMIS_ADD_DATE + 1 publication day) - INSP_DATE` for inspections. It is
not the time between the event and this project's acquisition. Original source
fields are preserved in the validated Parquet derivatives.

Each feed's ten bucket counts sum exactly to its lag sample size and to the
Athena, manifest, source-CSV, and Parquet row counts. Recorded fractions agree
with count divided by sample size. Both feeds have zero excluded or negative-lag
rows. The SQL emits only populated groups; the renderer inserts the defined
same-day bucket with count zero, consistent with both recorded minimum lags of
one day. All ten bins are visible. Both axes are linear, and the bars show the
fraction in each categorical interval, not probability density: the intervals
have unequal widths and the final interval is open-ended. Percentiles come
directly from Athena's recorded `approx_percentile` output; they are not inferred
from the histogram. The maxima are 13,609 days for crashes and 1,089 for
inspections, both contained in the final `271+` bucket.

Crash-file rows are not distinct crash incidents or first incident versions.
This descriptive plot does not calculate the incident-based p99.5 maturity
watermark, its bootstrapped confidence bound, or a training grace period.

## Latency

[latency.png](../../latency.png) uses the final
[32-session summary](../phase-3/latency-session-32/summary.json) and
[manifest](../phase-3/latency-session-32/manifest.json). It depicts the preserved
acceptance run completed September 11, 2026, with Locust 2.46.5, independent
scheduled arrivals, five seconds of warmup and 30 measured seconds per stage.
All scheduled completions were drained. The original investigation and failed
trials remain in Phase 3 evidence.

The left panel plots HTTP p50, p95, and p99 from Locust's rounded response-time
histogram, including failures. The right panel plots achieved throughput
against requested arrival rate; the reference line is the requested rate.
Achieved throughput includes drain time and equals completed requests divided
by elapsed seconds. The table displays three decimal places; provenance retains
the complete recorded values. Every stage completed all scheduled requests with
zero failures.

The committed criterion is **HTTP p99 <= 120 ms at 200 requested rps**. That
stage completed 6,000 requests at 199.998 achieved rps with 14 ms HTTP p99.
The horizontal line is a visual reference for that criterion, not a separately
committed criterion at each other load. At 200 rps, scheduled-arrival-to-response
p99 was 14.30 ms and maximum scheduler lag was 78.85 ms. HTTP timings begin when
the request is sent, so scheduler delays are disclosed separately. The chart
does not claim that maximum response time stayed below 120 ms at every stage.

The run used 512 fictional carriers and a synthetic MLflow model, PostgreSQL
lookups, loopback HTTP, and 32 persistent client sessions on local macOS. It
validates this local serving path. It establishes neither real-data model
quality nor production/cloud latency.
