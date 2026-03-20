#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import platform
import re
import shutil
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class CommandRun:
    command: str
    exit_code: int
    duration_sec: float
    max_rss_kb: int | None
    stdout_tail: str
    stderr_tail: str


TIME_BINARY = "/usr/bin/time"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run performance + parity gate checks")
    parser.add_argument("--perf-command", action="append", default=[], help="Perf command (repeatable)")
    parser.add_argument("--parity-command", action="append", default=[], help="Parity command (repeatable)")
    parser.add_argument("--runs", type=int, default=1, help="Repetitions for each perf command")
    parser.add_argument("--baseline", type=Path, help="Optional baseline JSON file")
    parser.add_argument("--update-baseline", action="store_true", help="Write current metrics into baseline")
    parser.add_argument("--max-duration-regression-pct", type=float, default=10.0)
    parser.add_argument("--max-rss-regression-pct", type=float, default=15.0)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("artifacts/perf_parity/perf_parity_report.json"),
        help="Report output path",
    )
    return parser.parse_args()


def parse_rss_kb(stderr_text: str) -> int | None:
    linux_match = re.search(r"Maximum resident set size \(kbytes\):\s*(\d+)", stderr_text)
    if linux_match:
        return int(linux_match.group(1))
    darwin_match = re.search(r"maximum resident set size\s*(\d+)", stderr_text, re.IGNORECASE)
    if darwin_match:
        return int(darwin_match.group(1))
    return None


def run_command(command: str) -> CommandRun:
    start = time.perf_counter()
    use_time = Path(TIME_BINARY).exists() and shutil.which("bash") is not None

    if use_time:
        if platform.system() == "Darwin":
            wrapped = [TIME_BINARY, "-l", "bash", "-lc", command]
        else:
            wrapped = [TIME_BINARY, "-v", "bash", "-lc", command]
    else:
        wrapped = ["bash", "-lc", command]

    completed = subprocess.run(wrapped, text=True, capture_output=True, check=False)
    if use_time and completed.returncode != 0 and "Operation not permitted" in completed.stderr:
        # macOS sandbox can block /usr/bin/time sysctl reads; rerun without it.
        completed = subprocess.run(["bash", "-lc", command], text=True, capture_output=True, check=False)
        use_time = False
    duration = time.perf_counter() - start

    max_rss_kb = parse_rss_kb(completed.stderr) if use_time else None

    return CommandRun(
        command=command,
        exit_code=completed.returncode,
        duration_sec=duration,
        max_rss_kb=max_rss_kb,
        stdout_tail="\n".join(completed.stdout.splitlines()[-30:]),
        stderr_tail="\n".join(completed.stderr.splitlines()[-40:]),
    )


def summarize_runs(runs: list[CommandRun]) -> dict[str, Any]:
    durations = [item.duration_sec for item in runs]
    rss_values = [item.max_rss_kb for item in runs if item.max_rss_kb is not None]

    return {
        "command": runs[0].command,
        "runs": len(runs),
        "exit_codes": [item.exit_code for item in runs],
        "duration_sec": {
            "min": min(durations),
            "max": max(durations),
            "mean": statistics.mean(durations),
            "median": statistics.median(durations),
        },
        "max_rss_kb": {
            "min": min(rss_values),
            "max": max(rss_values),
            "mean": statistics.mean(rss_values),
            "median": statistics.median(rss_values),
        }
        if rss_values
        else None,
        "failed": any(item.exit_code != 0 for item in runs),
        "stdout_tail": runs[-1].stdout_tail,
        "stderr_tail": runs[-1].stderr_tail,
    }


def load_baseline(path: Path | None) -> dict[str, Any]:
    if not path or not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def index_baseline_by_command(baseline: dict[str, Any]) -> dict[str, dict[str, Any]]:
    indexed: dict[str, dict[str, Any]] = {}
    for item in baseline.get("perf", []):
        command = item.get("command")
        if isinstance(command, str):
            indexed[command] = item
    return indexed


def compare_against_baseline(
    current_perf: list[dict[str, Any]],
    baseline: dict[str, Any],
    *,
    max_duration_regression_pct: float,
    max_rss_regression_pct: float,
) -> list[str]:
    failures: list[str] = []
    base_index = index_baseline_by_command(baseline)

    for current in current_perf:
        command = current["command"]
        base = base_index.get(command)
        if not base:
            continue

        curr_mean = float(current["duration_sec"]["mean"])
        base_mean = float(base.get("duration_sec", {}).get("mean", 0.0))
        if base_mean > 0:
            limit = base_mean * (1.0 + max_duration_regression_pct / 100.0)
            if curr_mean > limit:
                failures.append(
                    f"duration regression command={command!r} baseline_mean={base_mean:.3f}s current_mean={curr_mean:.3f}s limit={limit:.3f}s"
                )

        curr_rss = current.get("max_rss_kb", {})
        base_rss = base.get("max_rss_kb", {})
        if curr_rss and base_rss:
            curr_rss_mean = float(curr_rss["mean"])
            base_rss_mean = float(base_rss.get("mean", 0.0))
            if base_rss_mean > 0:
                rss_limit = base_rss_mean * (1.0 + max_rss_regression_pct / 100.0)
                if curr_rss_mean > rss_limit:
                    failures.append(
                        f"rss regression command={command!r} baseline_mean={base_rss_mean:.0f}KB current_mean={curr_rss_mean:.0f}KB limit={rss_limit:.0f}KB"
                    )

    return failures


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()

    if not args.perf_command:
        print("At least one --perf-command is required", file=sys.stderr)
        return 2
    if args.runs < 1:
        print("--runs must be >= 1", file=sys.stderr)
        return 2

    parity_runs: list[CommandRun] = []
    for command in args.parity_command:
        print(f"[parity] {command}")
        run = run_command(command)
        parity_runs.append(run)

    perf_runs_grouped: dict[str, list[CommandRun]] = {command: [] for command in args.perf_command}
    for command in args.perf_command:
        for run_index in range(args.runs):
            print(f"[perf {run_index + 1}/{args.runs}] {command}")
            perf_runs_grouped[command].append(run_command(command))

    parity_summary = [summarize_runs([run]) for run in parity_runs]
    perf_summary = [summarize_runs(runs) for runs in perf_runs_grouped.values()]

    baseline = load_baseline(args.baseline)
    regressions = compare_against_baseline(
        perf_summary,
        baseline,
        max_duration_regression_pct=args.max_duration_regression_pct,
        max_rss_regression_pct=args.max_rss_regression_pct,
    )

    parity_failures = [item for item in parity_summary if item["failed"]]
    perf_failures = [item for item in perf_summary if item["failed"]]

    report = {
        "ok": not parity_failures and not perf_failures and not regressions,
        "parity": parity_summary,
        "perf": perf_summary,
        "regressions": regressions,
        "thresholds": {
            "max_duration_regression_pct": args.max_duration_regression_pct,
            "max_rss_regression_pct": args.max_rss_regression_pct,
        },
        "baseline_path": str(args.baseline) if args.baseline else None,
    }

    write_json(args.output, report)
    print(f"report={args.output}")

    if args.update_baseline:
        if not args.baseline:
            print("--update-baseline requires --baseline", file=sys.stderr)
            return 2
        baseline_payload = {"perf": perf_summary}
        write_json(args.baseline, baseline_payload)
        print(f"baseline_updated={args.baseline}")

    if parity_failures:
        print("parity failures detected", file=sys.stderr)
    if perf_failures:
        print("perf command failures detected", file=sys.stderr)
    for regression in regressions:
        print(f"regression: {regression}", file=sys.stderr)

    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
