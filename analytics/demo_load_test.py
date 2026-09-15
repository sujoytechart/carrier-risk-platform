"""Measure a loopback demo example at a fixed offered request rate.

Run after starting the experimental API with a real example configured. This is
a short local smoke load test, separate from the production latency acceptance
sweep. Individual carrier identifiers never enter the output.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from collections import Counter
from pathlib import Path
from time import perf_counter

import httpx
import numpy as np


async def measure(port: int, rate: int, seconds: int) -> dict[str, object]:
    """Schedule arrivals against a monotonic clock and include queueing latency."""
    latencies: list[float] = []
    outcomes: Counter[str] = Counter()
    limit = asyncio.Semaphore(200)
    async with httpx.AsyncClient(
        base_url=f"http://127.0.0.1:{port}",
        timeout=5,
        limits=httpx.Limits(max_connections=200, max_keepalive_connections=200),
    ) as client:
        readiness = await client.get("/readyz")
        readiness.raise_for_status()

        async def request() -> None:
            started = perf_counter()
            try:
                async with limit:
                    response = await client.get("/demo/example")
                result = response.json()
                if (
                    response.status_code == 200
                    and result.get("status") == "scored"
                    and result.get("experimental") is True
                ):
                    outcomes["scored"] += 1
                else:
                    outcomes[f"http_{response.status_code}"] += 1
            except (httpx.HTTPError, ValueError):
                outcomes["client_error"] += 1
            finally:
                latencies.append((perf_counter() - started) * 1000)

        started = perf_counter()
        tasks = []
        for number in range(rate * seconds):
            await asyncio.sleep(max(0, started + number / rate - perf_counter()))
            tasks.append(asyncio.create_task(request()))
        await asyncio.gather(*tasks)
        elapsed = perf_counter() - started
        metrics = (await client.get("/metrics")).text
    return {
        "scope": "short_local_real_data_experimental_api_smoke",
        "offered_rps": rate,
        "offered_duration_seconds": seconds,
        "elapsed_seconds": elapsed,
        "achieved_rps": len(latencies) / elapsed,
        "requests": len(latencies),
        "outcomes": dict(outcomes),
        "p50_ms": float(np.percentile(latencies, 50)),
        "p95_ms": float(np.percentile(latencies, 95)),
        "p99_ms": float(np.percentile(latencies, 99)),
        "metrics_after": metrics,
    }


def main() -> None:
    """Write aggregate latency evidence for an already-running local API."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8003)
    parser.add_argument("--rate", type=int, default=200)
    parser.add_argument("--seconds", type=int, default=30)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if min(args.rate, args.seconds) < 1:
        parser.error("Rate and duration must be positive")
    result = asyncio.run(measure(args.port, args.rate, args.seconds))
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(
        json.dumps(
            {key: value for key, value in result.items() if key != "metrics_after"},
            indent=2,
        )
    )
    if result["outcomes"] != {"scored": result["requests"]}:
        raise SystemExit("Some requests failed; see the aggregate evidence")


if __name__ == "__main__":
    main()
