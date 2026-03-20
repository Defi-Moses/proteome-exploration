#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import platform
import re
import shlex
import shutil
import statistics
import subprocess
import time
from pathlib import Path
from pstats import Stats
from typing import Any

TIME_BINARY = "/usr/bin/time"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Profile and benchmark hotpath commands")
    parser.add_argument("--label", required=True, help="Session label used for artifact path")
    parser.add_argument("--command", required=True, help="Command to execute")
    parser.add_argument("--runs", type=int, default=3, help="Number of repetitions")
    parser.add_argument("--top", type=int, default=25, help="Top functions to print from cProfile")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("artifacts/hotpath_profiles"),
        help="Artifact output root",
    )
    parser.add_argument("--skip-cprofile", action="store_true", help="Skip cProfile wrapping")
    return parser.parse_args()


def parse_rss_kb(stderr_text: str) -> int | None:
    linux_match = re.search(r"Maximum resident set size \(kbytes\):\s*(\d+)", stderr_text)
    if linux_match:
        return int(linux_match.group(1))
    darwin_match = re.search(r"maximum resident set size\s*(\d+)", stderr_text, re.IGNORECASE)
    if darwin_match:
        return int(darwin_match.group(1))
    return None


def build_profiled_command(raw_command: str, profile_path: Path, skip_cprofile: bool) -> list[str]:
    parts = shlex.split(raw_command)
    if not parts:
        raise ValueError("Command is empty")

    if skip_cprofile:
        return ["bash", "-lc", raw_command]

    exe = Path(parts[0]).name
    if exe.startswith("python"):
        if len(parts) >= 2 and parts[1] == "-c":
            return ["bash", "-lc", raw_command]
        return [parts[0], "-m", "cProfile", "-o", str(profile_path), *parts[1:]]

    return ["bash", "-lc", raw_command]


def run_once(raw_command: str, profiled_command: list[str]) -> dict[str, Any]:
    start = time.perf_counter()
    use_time = Path(TIME_BINARY).exists() and shutil.which("bash") is not None

    if use_time:
        if platform.system() == "Darwin":
            wrapped = [TIME_BINARY, "-l", *profiled_command]
        else:
            wrapped = [TIME_BINARY, "-v", *profiled_command]
    else:
        wrapped = profiled_command

    completed = subprocess.run(wrapped, capture_output=True, text=True, check=False)
    if use_time and completed.returncode != 0 and "Operation not permitted" in completed.stderr:
        # macOS sandbox can block /usr/bin/time sysctl reads; rerun without it.
        completed = subprocess.run(profiled_command, capture_output=True, text=True, check=False)
        use_time = False
    duration = time.perf_counter() - start

    return {
        "command": raw_command,
        "exit_code": completed.returncode,
        "duration_sec": duration,
        "max_rss_kb": parse_rss_kb(completed.stderr) if use_time else None,
        "stdout_tail": "\n".join(completed.stdout.splitlines()[-30:]),
        "stderr_tail": "\n".join(completed.stderr.splitlines()[-40:]),
    }


def summarize_metrics(runs: list[dict[str, Any]]) -> dict[str, Any]:
    durations = [float(item["duration_sec"]) for item in runs]
    rss_values = [int(item["max_rss_kb"]) for item in runs if item.get("max_rss_kb") is not None]

    return {
        "runs": len(runs),
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
        "failed_runs": [index for index, item in enumerate(runs, start=1) if int(item["exit_code"]) != 0],
    }


def render_top_stats(profile_path: Path, top_n: int, output_path: Path) -> None:
    with output_path.open("w", encoding="utf-8") as handle:
        stats = Stats(str(profile_path), stream=handle)
        stats.strip_dirs().sort_stats("cumulative").print_stats(top_n)


def main() -> int:
    args = parse_args()
    if args.runs < 1:
        raise SystemExit("--runs must be >= 1")

    session_dir = args.output_dir / args.label
    session_dir.mkdir(parents=True, exist_ok=True)

    run_rows: list[dict[str, Any]] = []
    cprofile_files: list[Path] = []

    for run_index in range(1, args.runs + 1):
        profile_path = session_dir / f"run_{run_index}.pstats"
        command = build_profiled_command(args.command, profile_path, args.skip_cprofile)
        print(f"[run {run_index}/{args.runs}] {args.command}")
        row = run_once(args.command, command)
        row["run_index"] = run_index
        row["profile_path"] = str(profile_path) if profile_path.exists() else None
        run_rows.append(row)
        if profile_path.exists():
            cprofile_files.append(profile_path)

    summary = {
        "label": args.label,
        "command": args.command,
        "metrics": summarize_metrics(run_rows),
        "runs": run_rows,
        "cprofile_files": [str(path) for path in cprofile_files],
    }

    top_stats_files: list[str] = []
    for profile_path in cprofile_files:
        top_path = profile_path.with_suffix(".top.txt")
        render_top_stats(profile_path, args.top, top_path)
        top_stats_files.append(str(top_path))

    summary["cprofile_top_reports"] = top_stats_files

    summary_json = session_dir / "summary.json"
    summary_txt = session_dir / "summary.txt"

    summary_json.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_txt.write_text(
        "\n".join(
            [
                f"label={args.label}",
                f"command={args.command}",
                f"runs={args.runs}",
                f"duration_mean_sec={summary['metrics']['duration_sec']['mean']:.3f}",
                f"duration_median_sec={summary['metrics']['duration_sec']['median']:.3f}",
                (
                    f"max_rss_mean_kb={summary['metrics']['max_rss_kb']['mean']:.0f}"
                    if summary["metrics"]["max_rss_kb"]
                    else "max_rss_mean_kb=<unavailable>"
                ),
                f"failed_runs={summary['metrics']['failed_runs']}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    print(f"summary_json={summary_json}")
    print(f"summary_txt={summary_txt}")

    return 0 if not summary["metrics"]["failed_runs"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
