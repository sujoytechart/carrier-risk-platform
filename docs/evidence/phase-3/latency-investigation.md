# Local latency evidence and bounded investigation

The representative complete 50/100/200/300-rps curve **did not meet** the
200-rps acceptance target. Its 200-rps stage completed all 6,000 scheduled
requests, with 516 failures, HTTP p99 of 5,600 ms, and achieved throughput of
166.689 rps including completion drain. The complete curve retains all 19,500
request outcomes and 517 failures. No stage was selected or rerun to replace a
failed result.

The fixture uses 512 fictional carriers in a dedicated local PostgreSQL database
and an explicitly tagged MLflow model, `carrier-risk-fixture` version 2. The
classifier uses the frozen v0 configuration: 100 boosting trees, depth 3,
learning rate 0.1, seed 20260911. It validates the HTTP, warehouse, and registered
model inference path; it does not establish real-data predictive quality or
production latency. Repository verification had stopped before this complete
curve, but ordinary desktop applications remained active and uncontrolled.

- [Complete failed curve](latency-before-investigation.json) and
  [all compressed request records and SHA-256 hashes](latency-before-investigation-files.json).
- [Earlier concurrent-workload diagnostic](latency-diagnostic.json) and
  [its request records and hashes](latency-diagnostic-files.json). Its fixture
  version 1 used fewer trees, so it is not representative inference-cost evidence.
- [One bounded follow-up diagnostic](latency-bounded-diagnostic.json) and
  [its request records, resources, server metrics, and hashes](latency-bounded-diagnostic-files.json).

The full-curve harness overwrote Locust's underlying transport exception with a
generic `HTTP 0` failure. This reporting defect was corrected and covered by
regression tests. The corrected harness also records request start timestamps.
The original failure causes cannot be reconstructed from the old CSV; its
durations, failure flags, summary, and original harness hash remain unchanged.
The [exact original harness source](benchmark-before-diagnostics.py.txt) was
retained in the clean-export validation copy and its SHA-256 was verified against
the complete curve's recorded hash. The current harness hash matches the bounded
diagnostic's recorded hash.

The one follow-up diagnostic measured 2,000 requests at 200 rps for ten seconds,
after a two-second warmup. All measured requests returned valid synthetic scores,
but HTTP p99 was 300 ms and the gate still failed. The 5-second transport failures
did not recur. Server Prometheus counter differences show that all 2,400 handlers,
including warmup, completed within 120 ms. The excess HTTP tail therefore lies
outside the instrumented handler interval, which excludes request dispatch and
client/transport time. This observation does not isolate a causal bottleneck.

An independent observer recorded host/API/client resources once per second during
that diagnostic: system CPU peaked at 80.1%, API CPU at 58.1% of one core, and
client CPU at 86.5% of one core; minimum available memory was 1.77 GB. System-wide
swap-in increased by 29.2 MB and swap-out by 3.76 MB. These are concurrent host
measurements, not proof that memory pressure caused the observed latency. No
desktop applications were stopped. Post-run API logs contained no exception
tracebacks; the warehouse pool had twelve idle connections and no observed
blocked application query. These snapshots do not rule out transient stalls.

The open-arrival client catches up after scheduling delays and uses 256 persistent
sessions, so delayed arrivals can form bursts and long requests can overlap a
session's reuse. These are investigation leads, not established explanations for
the failed run. No speculative performance change or threshold adjustment was
made, and the complete failed baseline remains the acceptance result.

## ASGI boundary diagnostic

A temporary wrapper then measured ASGI entry, response start and final body send,
correlating synthetic request IDs with client timestamps from the unchanged
open-arrival harness. Each attempt used 200 rps for ten measured seconds after
five seconds of warmup, with the same synthetic model version 2 and default
single-worker Uvicorn. Repository checks paused during the load; ordinary desktop
activity remained uncontrolled. The added timing headers and buffered timestamps
make these instrumented diagnostics, not replacement acceptance measurements.

The [first attempt](asgi-probe/attempt-1/summary.json) completed all 2,000 measured
requests without transport failures, with HTTP p99 of 2,000 ms. Its temporary
`atexit` recording hook did not flush server records when Uvicorn re-raised
SIGTERM after graceful shutdown. The failed recording attempt, its exact source
and client timings are retained. The recovery changed only temporary recording:
flush at ASGI lifespan shutdown completion and copy timing headers into the
client trace.

The [recovery attempt](asgi-probe/recovery/summary.json) completed all 2,000
measured requests without transport failures, with native histogram p99 of
400 ms and exact client p99 of 385.04 ms. All 3,000 warmup and measured request
IDs matched server records, and client timing headers matched the server trace.
[Correlated aggregate measurements](asgi-probe/correlation-summary.json) show
ASGI entry-to-response-start p99 of 258.55 ms and maximum of 338.92 ms; combined
time before ASGI entry and after response start had p99 of 144.53 ms. These
component percentiles describe different request distributions and are not
additive. ASGI time dominated 136 of the 208 measured responses exceeding
120 ms; outside-ASGI time dominated the other 72. The slowest response took
483.17 ms: 56.42 ms before ASGI entry, 328.86 ms inside ASGI before response
start, and 97.88 ms afterward. Final body send took at most 0.801 ms.

This bounds the observed tail to include substantial delay inside the
API/framework interval and outside ASGI; final body write time alone does not
explain it. The ASGI interval includes framework scheduling, the synchronous
endpoint thread queue, handler execution, validation and response construction.
This probe does not separate those stages. Its handler histogram includes slow
warmup requests, so it cannot establish a handler-only bound for the measured
stage. Neither attempt reproduced the earlier 516 transport failures or explains
their cause. The latency target remains unmet.

[Compressed client/server traces, native CSVs, metrics, exact probe sources and
SHA-256 manifest](asgi-probe/manifest.json) preserve both attempts. The first
attempt has no server trace because of the recording failure. Every published
file hash was verified, compressed content was checked after decompression, and
source hashes match the recorded configurations. No private environment contents,
credentials, source-data rows or response bodies are included. Both temporary
servers were stopped after their respective stages.

## Two-worker configuration trial

The ASGI measurements supported one controlled test of worker isolation. A new
temporary API ran with `--workers 2`, using the same fixture model version 2 and
the unchanged harness, five-second warmups, and thirty-second measured stages.
Repository tests and other load runs were stopped. Ordinary desktop applications
remained active. All 19,500 request outcomes are retained in the
[complete trial](latency-two-worker-trial.json) and
[raw records, resource samples, and hash manifest](latency-two-worker-trial-files.json).

| Offered rps | Achieved rps including drain | HTTP p99, ms | Failures | Stage gate |
| ---: | ---: | ---: | ---: | :--- |
| 50 | 49.999 | 17 | 0 | Passed |
| 100 | 99.963 | 260 | 0 | Failed |
| 200 | 197.038 | 200 | 0 | Failed |
| 300 | 299.768 | 76 | 0 | Passed |

The required 200-rps stage still failed: its scheduled-arrival p99 was 462.784 ms
and maximum scheduler lag was 435.830 ms. The faster 300-rps stage does not
replace that failed result. This configuration was rejected as an acceptance
fix; no worker or performance setting was adopted and no thresholds changed.

The independent resource observer collected 142 samples. Aggregate API process
CPU peaked at 112.2% of one core; system CPU peaked at 82.6%, and minimum available
memory was 1.45 GB. System-wide swap-in increased by 362.2 MB and swap-out by
22.7 MB. These observations do not establish the cause of the latency variation.
The existing Prometheus registry is per process, so this temporary trial did not
claim aggregated server metrics. Supporting multiple workers would also require
correct metric aggregation. Both workers, their supervisor, and the load process
were stopped after the trial.
