"""Reproducible local Locust benchmark with open arrivals at 50/100/200/300 rps.

Arrivals follow a wall-clock schedule independent of response completion. Raw
Locust request timings, failures, scheduler lag, and drained throughput are saved;
unavailable/error responses cannot count as successful synthetic scores.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import importlib
import json
import math
import platform
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter, time
from typing import Any
from urllib.parse import urlparse

CLIENT_SESSION_COUNT = 32


def valid_fixture_score(
    payload: object, *, status_code: int, month: str, usdot: str
) -> bool:
    """Require an actual probability for the requested synthetic carrier."""
    if not isinstance(payload, dict):
        return False
    probability = payload.get("risk_score")
    version = payload.get("model_version")
    return (
        status_code == 200
        and payload.get("status") == "scored"
        and payload.get("usdot_number") == usdot
        and payload.get("validation_fixture") is True
        and payload.get("features_as_of") == month
        and isinstance(version, str)
        and version.isdigit()
        and int(version) > 0
        and isinstance(probability, (int, float))
        and not isinstance(probability, bool)
        and 0 <= probability <= 1
        and math.isfinite(probability)
    )


def score_failure_reason(
    status_code: int, payload: object, transport_error: Exception | None
) -> Exception | str:
    """Preserve Locust's actual transport exception instead of masking its cause."""
    if transport_error is not None:
        return transport_error
    status = payload.get("status") if isinstance(payload, dict) else "invalid_json"
    return f"Expected synthetic score; HTTP {status_code}; status={status}"


def nearest_rank_percentile(values: list[float], quantile: float) -> float:
    """Calculate a percentile of raw arrival-to-response timings without rounding."""
    return sorted(values)[max(0, math.ceil(quantile * len(values)) - 1)]


def run_load(
    host: str,
    output: Path,
    *,
    seconds: int = 30,
    warmup: int = 5,
    rates: tuple[int, ...] = (50, 100, 200, 300),
) -> dict[str, object]:
    """Benchmark synthetic scores on loopback using actual Locust request events."""
    if urlparse(host).hostname not in {"localhost", "127.0.0.1", "::1"}:
        raise ValueError("This validation benchmark only accepts a loopback API")
    if seconds < 10 or warmup < 1:
        raise ValueError("Use at least ten measured seconds and one warmup second")
    if not rates or any(
        type(rate) is not int or not 0 < rate <= 1000 for rate in rates
    ):
        raise ValueError("Arrival rates must be whole numbers between 1 and 1000")
    # Locust owns cooperative socket patching before any HTTP client is created.
    locust = importlib.import_module("locust")
    environment_module = importlib.import_module("locust.env")
    clients_module = importlib.import_module("locust.clients")
    gevent = importlib.import_module("gevent")
    pool_module = importlib.import_module("gevent.pool")
    output.mkdir(parents=True, exist_ok=True)

    def run_stage(rate: int) -> dict[str, object]:
        environment = environment_module.Environment()
        runner = environment.create_local_runner()
        # Thirty-two persistent sessions keep connections warm without turning a
        # 200-rps loopback test into hundreds of idle server sockets. At the
        # acceptance rate, each session receives one arrival every 160 ms and
        # still permits overlapping requests when a response is slow.
        clients = [
            clients_module.HttpSession(host, environment.events.request, None)
            for _ in range(CLIENT_SESSION_COUNT)
        ]
        for client in clients:
            client.trust_env = False
        samples: list[tuple[float, float, bool, str, float]] = []
        model_versions: set[str] = set()

        def record_request(
            response_time: float,
            exception: Exception | None,
            context: dict[str, float],
            **details: object,
        ) -> None:
            scheduled_latency = response_time + context["scheduler_lag_ms"]
            samples.append(
                (
                    response_time,
                    scheduled_latency,
                    exception is not None,
                    f"{type(exception).__name__}: {str(exception)[:500]}"
                    if exception is not None
                    else "",
                    context["request_started_at_unix_seconds"],
                )
            )

        environment.events.request.add_listener(record_request)
        pool = pool_module.Group()
        lag_samples: list[float] = []
        expected_month = datetime.now(UTC).date().replace(day=1).isoformat()

        def request_score(client: Any, number: int, deadline: float) -> None:
            usdot = str(1 + number % 512)
            lag = max(0, perf_counter() - deadline) * 1000
            lag_samples.append(lag)
            with client.get(
                f"/score/{usdot}",
                name="/score/{usdot_number}",
                catch_response=True,
                timeout=5,
                context={
                    "scheduler_lag_ms": lag,
                    "request_started_at_unix_seconds": time(),
                },
            ) as response:
                payload = None
                try:
                    payload = response.json()
                    valid = valid_fixture_score(
                        payload,
                        status_code=response.status_code,
                        month=expected_month,
                        usdot=usdot,
                    )
                except (ValueError, AttributeError):
                    valid = False
                if not valid:
                    response.failure(
                        score_failure_reason(
                            response.status_code, payload, response.error
                        )
                    )
                elif isinstance(payload, dict):
                    model_versions.add(str(payload["model_version"]))

        def arrivals(duration: int) -> float:
            started = perf_counter()
            for number in range(rate * duration):
                deadline = started + number / rate
                gevent.sleep(max(0, deadline - perf_counter()))
                pool.spawn(
                    request_score, clients[number % len(clients)], number, deadline
                )
            gevent.sleep(max(0, started + duration - perf_counter()))
            pool.join()
            return perf_counter() - started

        arrivals(warmup)
        environment.stats.reset_all()
        samples.clear()
        lag_samples.clear()
        elapsed = arrivals(seconds)
        totals = environment.stats.total
        requests = int(totals.num_requests)
        failures = int(totals.num_failures)
        scheduled_p99 = nearest_rank_percentile([sample[1] for sample in samples], 0.99)
        record: dict[str, object] = {
            "target_rps": rate,
            "scheduled_requests": rate * seconds,
            "completed_requests": requests,
            "failures": failures,
            "elapsed_seconds_including_drain": elapsed,
            "achieved_rps": requests / elapsed,
            "p50_ms": float(totals.get_response_time_percentile(0.50)),
            "p95_ms": float(totals.get_response_time_percentile(0.95)),
            "p99_ms": float(totals.get_response_time_percentile(0.99)),
            "max_ms": float(totals.max_response_time),
            "maximum_scheduler_lag_ms": max(lag_samples),
            "scheduled_arrival_to_response_p99_ms": scheduled_p99,
            "model_versions": sorted(model_versions),
            "failure_details": sorted({sample[3] for sample in samples if sample[2]}),
            "all_scheduled_requests_completed": requests == rate * seconds,
        }
        record["latency_target_met"] = (
            failures == 0
            and requests == rate * seconds
            and float(totals.get_response_time_percentile(0.99)) <= 120
            and max(lag_samples) <= 120
            and scheduled_p99 <= 120
        )
        with (output / f"locust-{rate}rps.csv").open("w", newline="") as destination:
            writer = csv.writer(destination)
            writer.writerow(
                (
                    "request_duration_ms",
                    "scheduled_arrival_to_response_ms",
                    "failed",
                    "failure_details",
                    "request_started_at_unix_seconds",
                )
            )
            writer.writerows(samples)
        for client in clients:
            client.close()
        runner.quit()
        print(json.dumps(record), flush=True)
        return record

    stages = [run_stage(rate) for rate in rates]
    report: dict[str, object] = {
        "validation_fixture": True,
        "scope": "Synthetic model loaded from MLflow; PostgreSQL lookup and HTTP score",
        "not_evidence_of": ["real model quality", "production/cloud latency"],
        "load_generator": f"Locust {locust.__version__}",
        "arrival_model": "independent scheduled open arrivals; all completions drained",
        "percentiles": "Locust's rounded response-time histogram, including failures",
        "measured_seconds_per_stage": seconds,
        "warmup_seconds_per_stage": warmup,
        "completed_at": datetime.now(UTC).isoformat(),
        "client_platform": platform.platform(),
        "python_version": platform.python_version(),
        "harness_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "stages": stages,
    }
    (output / "summary.json").write_text(json.dumps(report, indent=2) + "\n")
    return report


def main() -> None:
    """Run and retain the complete local throughput curve, including failing stages."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="http://127.0.0.1:8000")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--seconds", type=int, default=30)
    parser.add_argument("--warmup", type=int, default=5)
    parser.add_argument("--rates", type=int, nargs="+", default=[50, 100, 200, 300])
    arguments = parser.parse_args()
    run_load(
        arguments.host,
        arguments.output_dir,
        seconds=arguments.seconds,
        warmup=arguments.warmup,
        rates=tuple(arguments.rates),
    )


if __name__ == "__main__":
    main()
