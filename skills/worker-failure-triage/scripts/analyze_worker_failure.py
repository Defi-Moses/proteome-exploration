#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

STAGE_NAMES = {
    "smoke-ingest",
    "ingest-ccre",
    "project-fixture",
    "project-vcf",
    "call-states",
    "discover-candidates",
    "featurize",
    "build-validation-link",
    "build-holdouts",
    "evaluate-ranking",
    "shortlist",
    "score-fanout",
    "compute-disagreement",
    "run-ablations",
    "build-registry",
    "freeze-evaluation",
    "build-phase1-report",
}


@dataclass
class Signature:
    category: str
    confidence: float
    pattern: re.Pattern[str]
    summary: str
    actions: list[str]


SIGNATURES = [
    Signature(
        category="memory_oom",
        confidence=0.95,
        pattern=re.compile(r"(exit=-9|Killed 137|out of memory|\bOOM\b)", re.IGNORECASE),
        summary="Worker likely exceeded memory during stage execution.",
        actions=[
            "Run hot-path profiling for the failing stage.",
            "Apply streaming/chunking or reduce in-memory materialization.",
            "Use capacity-rightsizer recommendations after collecting metrics.",
        ],
    ),
    Signature(
        category="missing_required_env",
        confidence=0.98,
        pattern=re.compile(r"PANCCRE_[A-Z0-9_]+ must be set", re.IGNORECASE),
        summary="Required PANCCRE environment variable is missing.",
        actions=[
            "Set the missing env var on the worker service.",
            "Re-run pipeline readiness gate before retrying.",
        ],
    ),
    Signature(
        category="invalid_env_value",
        confidence=0.95,
        pattern=re.compile(r"must be one of|Unsupported PANCCRE_WORKER_MODE", re.IGNORECASE),
        summary="Environment variable value is invalid for current worker logic.",
        actions=[
            "Correct enum-style env values to allowed options.",
            "Re-run pipeline readiness gate to validate combinations.",
        ],
    ),
    Signature(
        category="missing_path",
        confidence=0.9,
        pattern=re.compile(r"(FileNotFoundError|No such file or directory|missing files)", re.IGNORECASE),
        summary="Required input/output file path is missing.",
        actions=[
            "Verify mounted volume paths and configured file locations.",
            "Re-run from the failing stage or its direct upstream dependency.",
        ],
    ),
    Signature(
        category="api_sync_auth_or_endpoint",
        confidence=0.97,
        pattern=re.compile(r"(PANCCRE_REGISTRY_SYNC_TOKEN|PANCCRE_API_SYNC_URL|registry API sync failed status=)", re.IGNORECASE),
        summary="Registry API sync configuration/authentication failed.",
        actions=[
            "Validate publish mode and sync token on both worker and API services.",
            "Validate PANCCRE_API_SYNC_URL or linked Railway API domain wiring.",
        ],
    ),
    Signature(
        category="generic_nonzero_exit",
        confidence=0.6,
        pattern=re.compile(r"pipeline command failed \(exit=", re.IGNORECASE),
        summary="Pipeline command returned non-zero exit without a known specialized signature.",
        actions=[
            "Inspect failing stage logs around stack trace and stderr payload.",
            "Add signature coverage after root cause is confirmed.",
        ],
    ),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze PANCCRE worker logs and suggest root-cause actions")
    parser.add_argument("--log-file", type=Path, help="Path to worker log file (default: read stdin)")
    parser.add_argument("--json-out", type=Path, help="Optional JSON report output path")
    parser.add_argument("--tail-lines", type=int, default=600, help="Only analyze last N lines (default: 600)")
    return parser.parse_args()


def read_lines(args: argparse.Namespace) -> list[str]:
    if args.log_file:
        content = args.log_file.read_text(encoding="utf-8", errors="replace")
    else:
        content = sys.stdin.read()
    lines = content.splitlines()
    if args.tail_lines > 0 and len(lines) > args.tail_lines:
        return lines[-args.tail_lines :]
    return lines


def extract_stage_from_command(command_text: str) -> str | None:
    parts = command_text.strip().split()
    for index, token in enumerate(parts):
        if token.endswith("run_phase1.py") and index + 1 < len(parts):
            maybe_stage = parts[index + 1].strip()
            if maybe_stage in STAGE_NAMES:
                return maybe_stage
    return None


def find_last_stage(lines: list[str]) -> str | None:
    stage: str | None = None
    for line in lines:
        match = re.search(r"worker_pipeline_command\s+.*command=(.+)$", line)
        if match:
            parsed = extract_stage_from_command(match.group(1))
            if parsed:
                stage = parsed
    return stage


def classify(lines: list[str]) -> tuple[Signature, list[str]]:
    joined = "\n".join(lines)
    for signature in SIGNATURES:
        if signature.pattern.search(joined):
            evidence = [line for line in lines if signature.pattern.search(line)]
            return signature, evidence[-6:]
    fallback = Signature(
        category="unknown",
        confidence=0.4,
        pattern=re.compile(r"$^"),
        summary="No known signature matched. Manual triage required.",
        actions=[
            "Collect broader log window and inspect stack trace boundaries.",
            "Capture reproducible command and input set for deeper debugging.",
        ],
    )
    return fallback, lines[-6:]


def build_next_commands(stage: str | None, category: str) -> list[str]:
    commands: list[str] = []
    commands.append("python3 skills/pipeline-readiness-gate/scripts/check_pipeline_readiness.py")

    if category == "memory_oom":
        target = stage or "call-states"
        commands.append(
            f"python3 skills/hotpath-profiler-lab/scripts/profile_hotpath.py --label {target} --command \"python3 scripts/run_phase1.py {target} ...\" --runs 2"
        )
    if category in {"api_sync_auth_or_endpoint", "missing_required_env", "invalid_env_value"}:
        commands.append("python3 skills/pipeline-readiness-gate/scripts/check_pipeline_readiness.py --strict-railway-link")

    if stage:
        commands.append(
            f"python3 skills/incremental-pipeline-runner/scripts/run_incremental_pipeline.py --run-dir /data/runs/<run_tag> --start-stage {stage} --execute"
        )

    return commands


def print_text_report(report: dict[str, Any]) -> None:
    print(f"category={report['category']} confidence={report['confidence']:.2f}")
    print(f"stage={report['stage'] or '<unknown>'}")
    print(f"summary={report['summary']}")

    if report["evidence"]:
        print("evidence:")
        for line in report["evidence"]:
            print(f"  {line}")

    if report["actions"]:
        print("actions:")
        for action in report["actions"]:
            print(f"  - {action}")

    if report["next_commands"]:
        print("next_commands:")
        for command in report["next_commands"]:
            print(f"  - {command}")


def main() -> int:
    args = parse_args()
    lines = read_lines(args)

    if not lines:
        print("no log content provided", file=sys.stderr)
        return 2

    stage = find_last_stage(lines)
    signature, evidence = classify(lines)

    report = {
        "category": signature.category,
        "confidence": signature.confidence,
        "stage": stage,
        "summary": signature.summary,
        "actions": signature.actions,
        "evidence": evidence,
        "next_commands": build_next_commands(stage, signature.category),
    }

    print_text_report(report)

    if args.json_out:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(f"json_report={args.json_out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
