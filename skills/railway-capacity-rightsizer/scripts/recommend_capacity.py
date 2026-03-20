#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import re
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Recommend Railway CPU/memory from benchmark evidence")
    parser.add_argument(
        "--benchmark-report",
        action="append",
        default=[],
        type=Path,
        help="Benchmark JSON report (repeatable)",
    )
    parser.add_argument("--target-duration-sec", type=float, help="Target stage duration in seconds")
    parser.add_argument("--memory-headroom-pct", type=float, default=35.0)
    parser.add_argument("--current-cpu", type=float, help="Current worker CPU tier")
    parser.add_argument("--current-memory-bytes", type=int, help="Current worker memoryBytes")
    parser.add_argument(
        "--worker-railway-toml",
        type=Path,
        default=Path("apps/worker/railway.toml"),
        help="Path to worker railway.toml",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("artifacts/capacity/rightsize_recommendation.json"),
        help="Recommendation output JSON",
    )
    return parser.parse_args()


def parse_worker_limits(path: Path) -> tuple[float | None, int | None]:
    if not path.exists():
        return None, None
    text = path.read_text(encoding="utf-8")

    cpu_match = re.search(r"^cpu\s*=\s*([0-9]+(?:\.[0-9]+)?)\s*$", text, flags=re.MULTILINE)
    memory_match = re.search(r"^memoryBytes\s*=\s*([0-9]+)\s*$", text, flags=re.MULTILINE)

    cpu = float(cpu_match.group(1)) if cpu_match else None
    memory = int(memory_match.group(1)) if memory_match else None
    return cpu, memory


def extract_metrics(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    if "metrics" in payload and "runs" in payload:
        metrics = payload.get("metrics", {})
        duration = metrics.get("duration_sec", {}).get("mean")
        rss = metrics.get("max_rss_kb", {}).get("mean") if metrics.get("max_rss_kb") else None
        rows.append(
            {
                "label": payload.get("label") or payload.get("command") or "hotpath",
                "duration_sec_mean": float(duration) if duration is not None else None,
                "max_rss_kb_mean": float(rss) if rss is not None else None,
            }
        )

    for item in payload.get("perf", []):
        rows.append(
            {
                "label": item.get("command", "perf_command"),
                "duration_sec_mean": float(item.get("duration_sec", {}).get("mean", 0.0)),
                "max_rss_kb_mean": (
                    float(item.get("max_rss_kb", {}).get("mean"))
                    if item.get("max_rss_kb") and item.get("max_rss_kb", {}).get("mean") is not None
                    else None
                ),
            }
        )

    return rows


def nearest_2gib(bytes_value: int) -> int:
    two_gib = 2 * 1024 * 1024 * 1024
    return int(math.ceil(bytes_value / two_gib) * two_gib)


def recommend_cpu(current_cpu: float, observed_max_duration: float, target_duration: float | None) -> tuple[float, str]:
    if target_duration is None or target_duration <= 0:
        return current_cpu, "No target duration provided; keep current CPU tier."

    ratio = observed_max_duration / target_duration if target_duration > 0 else 1.0

    if ratio >= 1.6:
        recommended = min(16.0, max(current_cpu, 2.0) * 2.0)
        return recommended, f"Observed duration is {ratio:.2f}x target; recommend doubling CPU tier."
    if ratio >= 1.2:
        recommended = min(16.0, max(current_cpu, 2.0) * 1.5)
        return recommended, f"Observed duration is {ratio:.2f}x target; recommend moderate CPU increase."
    if ratio <= 0.6 and current_cpu > 2.0:
        recommended = max(2.0, current_cpu / 1.5)
        return recommended, f"Observed duration is {ratio:.2f}x target; CPU may be reduced safely after validation."

    return current_cpu, f"Observed duration is {ratio:.2f}x target; keep CPU tier."


def main() -> int:
    args = parse_args()

    cfg_cpu, cfg_memory = parse_worker_limits(args.worker_railway_toml)
    current_cpu = args.current_cpu if args.current_cpu is not None else (cfg_cpu if cfg_cpu is not None else 8.0)
    current_memory = (
        args.current_memory_bytes
        if args.current_memory_bytes is not None
        else (cfg_memory if cfg_memory is not None else 16 * 1024 * 1024 * 1024)
    )

    metric_rows: list[dict[str, Any]] = []
    for report_path in args.benchmark_report:
        if not report_path.exists():
            print(f"warning: missing report {report_path}")
            continue
        payload = json.loads(report_path.read_text(encoding="utf-8"))
        metric_rows.extend(extract_metrics(payload))

    if not metric_rows:
        print("No benchmark metrics found. Provide at least one --benchmark-report.")
        return 2

    duration_values = [row["duration_sec_mean"] for row in metric_rows if row.get("duration_sec_mean") is not None]
    rss_values = [row["max_rss_kb_mean"] for row in metric_rows if row.get("max_rss_kb_mean") is not None]

    observed_max_duration = max(duration_values) if duration_values else 0.0
    observed_max_rss_kb = max(rss_values) if rss_values else None

    recommended_cpu, cpu_reason = recommend_cpu(current_cpu, observed_max_duration, args.target_duration_sec)

    if observed_max_rss_kb is not None:
        raw_bytes = int((observed_max_rss_kb * 1024.0) * (1.0 + args.memory_headroom_pct / 100.0))
        recommended_memory = nearest_2gib(raw_bytes)
        memory_reason = (
            f"Peak observed RSS={observed_max_rss_kb:.0f}KB with headroom={args.memory_headroom_pct:.1f}% "
            f"rounded to nearest 2GiB."
        )
    else:
        recommended_memory = current_memory
        memory_reason = "RSS data unavailable in reports; keep current memory setting."

    recommendation = {
        "current": {
            "cpu": current_cpu,
            "memory_bytes": current_memory,
        },
        "recommended": {
            "cpu": recommended_cpu,
            "memory_bytes": recommended_memory,
        },
        "observed": {
            "max_duration_sec": observed_max_duration,
            "max_rss_kb": observed_max_rss_kb,
            "sample_count": len(metric_rows),
        },
        "reasons": {
            "cpu": cpu_reason,
            "memory": memory_reason,
        },
        "inputs": {
            "reports": [str(path) for path in args.benchmark_report],
            "target_duration_sec": args.target_duration_sec,
            "memory_headroom_pct": args.memory_headroom_pct,
        },
        "railway_toml_snippet": {
            "cpu": recommended_cpu,
            "memoryBytes": recommended_memory,
        },
    }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(recommendation, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    snippet_path = args.output.with_suffix(".toml")
    snippet_path.write_text(
        "[deploy.limitOverride.containers]\n"
        f"cpu = {recommended_cpu}\n"
        f"memoryBytes = {recommended_memory}\n",
        encoding="utf-8",
    )

    print(f"recommendation={args.output}")
    print(f"snippet={snippet_path}")
    print(f"recommended_cpu={recommended_cpu}")
    print(f"recommended_memory_bytes={recommended_memory}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
