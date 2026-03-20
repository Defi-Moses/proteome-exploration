#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

VALID_WORKER_MODES = {"heartbeat", "once", "pipeline_once", "pipeline_loop"}
VALID_PROJECTION_MODES = {"fixture", "vcf"}
VALID_PUBLISH_MODES = {"local", "api_sync", "dual"}
VALID_ASSAY_FORMATS = {"csv", "jsonl", "parquet"}


@dataclass
class CheckResult:
    name: str
    level: str
    ok: bool
    details: str


def load_env_file(path: Path) -> dict[str, str]:
    loaded: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        loaded[key] = value
    return loaded


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fail-fast preflight checks for PANCCRE pipeline runs")
    parser.add_argument("--env-file", type=Path, help="Optional env file to load before checks")
    parser.add_argument(
        "--strict-railway-link",
        action="store_true",
        help="Treat Railway linkage/status issues as hard errors",
    )
    parser.add_argument(
        "--skip-railway",
        action="store_true",
        help="Skip Railway CLI/link checks",
    )
    parser.add_argument("--json-out", type=Path, help="Optional JSON report output path")
    return parser.parse_args()


def nearest_existing_parent(path: Path) -> Path:
    current = path
    while True:
        if current.exists():
            return current
        if current.parent == current:
            return current
        current = current.parent


def check_target_writable(path: Path) -> tuple[bool, str]:
    parent = nearest_existing_parent(path)
    if not parent.exists():
        return False, f"no existing parent for target={path}"
    if not os.access(parent, os.W_OK):
        return False, f"parent not writable parent={parent} target={path}"
    return True, f"writable parent={parent} target={path}"


def validate_url(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def add(results: list[CheckResult], name: str, ok: bool, details: str, *, level: str = "error") -> None:
    effective_level = "info" if ok else level
    results.append(CheckResult(name=name, level=effective_level, ok=ok, details=details))


def run_checks(args: argparse.Namespace) -> list[CheckResult]:
    env = dict(os.environ)
    if args.env_file:
        if not args.env_file.exists():
            return [CheckResult(name="env_file", level="error", ok=False, details=f"env file missing: {args.env_file}")]
        env.update(load_env_file(args.env_file))

    results: list[CheckResult] = []

    run_script = Path("scripts/run_phase1.py")
    add(results, "pipeline_cli", run_script.exists(), f"expected {run_script}")

    worker_mode = env.get("PANCCRE_WORKER_MODE", "heartbeat").strip()
    add(
        results,
        "worker_mode",
        worker_mode in VALID_WORKER_MODES,
        f"PANCCRE_WORKER_MODE={worker_mode} valid={sorted(VALID_WORKER_MODES)}",
    )

    projection_mode = env.get("PANCCRE_PIPELINE_PROJECTION_MODE", "fixture").strip().lower() or "fixture"
    add(
        results,
        "projection_mode",
        projection_mode in VALID_PROJECTION_MODES,
        f"PANCCRE_PIPELINE_PROJECTION_MODE={projection_mode} valid={sorted(VALID_PROJECTION_MODES)}",
    )

    publish_mode = env.get("PANCCRE_REGISTRY_PUBLISH_MODE", "local").strip().lower() or "local"
    add(
        results,
        "publish_mode",
        publish_mode in VALID_PUBLISH_MODES,
        f"PANCCRE_REGISTRY_PUBLISH_MODE={publish_mode} valid={sorted(VALID_PUBLISH_MODES)}",
    )

    assay_format = env.get("PANCCRE_PIPELINE_ASSAY_SOURCE_FORMAT", "csv").strip().lower() or "csv"
    add(
        results,
        "assay_format",
        assay_format in VALID_ASSAY_FORMATS,
        f"PANCCRE_PIPELINE_ASSAY_SOURCE_FORMAT={assay_format} valid={sorted(VALID_ASSAY_FORMATS)}",
    )

    if projection_mode == "vcf":
        variants = env.get("PANCCRE_PIPELINE_VARIANTS", "").strip()
        add(results, "vcf_variants_env", bool(variants), "PANCCRE_PIPELINE_VARIANTS required when projection_mode=vcf")
        if variants:
            variants_path = Path(variants)
            add(results, "vcf_variants_path", variants_path.exists(), f"variants path exists: {variants_path}")

    haplotypes = env.get("PANCCRE_PIPELINE_HAPLOTYPES", "").strip()
    if haplotypes:
        hap_path = Path(haplotypes)
        add(results, "haplotypes_path", hap_path.exists(), f"haplotypes path exists: {hap_path}")

    ccre_bed = env.get("PANCCRE_PIPELINE_CCRE_BED", "").strip()
    if ccre_bed:
        ccre_path = Path(ccre_bed)
        add(results, "ccre_bed_path", ccre_path.exists(), f"cCRE BED path exists: {ccre_path}")

    assay_source = env.get("PANCCRE_PIPELINE_ASSAY_SOURCE", "").strip()
    if assay_source:
        assay_path = Path(assay_source)
        add(results, "assay_source_path", assay_path.exists(), f"assay source path exists: {assay_path}")

    if publish_mode in {"api_sync", "dual"}:
        sync_token = env.get("PANCCRE_REGISTRY_SYNC_TOKEN", "").strip()
        add(results, "registry_sync_token", bool(sync_token), "PANCCRE_REGISTRY_SYNC_TOKEN required for api_sync/dual")

        api_sync_url = env.get("PANCCRE_API_SYNC_URL", "").strip()
        linked_domain = env.get("RAILWAY_SERVICE__PANCCRE_API_URL", "").strip()
        url_ok = validate_url(api_sync_url) if api_sync_url else bool(linked_domain)
        add(
            results,
            "api_sync_endpoint",
            url_ok,
            (
                f"PANCCRE_API_SYNC_URL={api_sync_url or '<unset>'} "
                f"RAILWAY_SERVICE__PANCCRE_API_URL={linked_domain or '<unset>'}"
            ),
        )

    for key, default in [
        ("PANCCRE_PIPELINE_OUTPUT_ROOT", "/data/runs"),
        ("PANCCRE_PUBLISH_REGISTRY_DIR", "/data/registry"),
        ("PANCCRE_REPORT_OUTPUT_ROOT", "/data/reports"),
        ("PANCCRE_FREEZE_OUTPUT_ROOT", "/data/processed"),
    ]:
        target = Path(env.get(key, default))
        ok, details = check_target_writable(target)
        add(results, f"writable::{key}", ok, details, level="warning")

    if not args.skip_railway:
        railway_path = shutil.which("railway")
        add(results, "railway_cli", bool(railway_path), f"railway path={railway_path or '<missing>'}", level="warning")
        if railway_path:
            try:
                completed = subprocess.run(
                    ["railway", "status"],
                    capture_output=True,
                    text=True,
                    check=False,
                    timeout=8,
                )
                status_ok = completed.returncode == 0
                detail = f"railway status exit={completed.returncode}"
                add(
                    results,
                    "railway_link",
                    status_ok,
                    detail,
                    level="error" if args.strict_railway_link else "warning",
                )
            except Exception as exc:  # pragma: no cover - defensive guard
                add(
                    results,
                    "railway_link",
                    False,
                    f"railway status failed: {exc}",
                    level="error" if args.strict_railway_link else "warning",
                )

    return results


def print_text_report(results: list[CheckResult]) -> None:
    symbols = {"info": "PASS", "warning": "WARN", "error": "FAIL"}
    for result in results:
        symbol = symbols.get(result.level, "INFO")
        print(f"[{symbol}] {result.name}: {result.details}")

    errors = sum(1 for item in results if item.level == "error" and not item.ok)
    warnings = sum(1 for item in results if item.level == "warning" and not item.ok)
    print(f"summary checks={len(results)} errors={errors} warnings={warnings}")


def serialize(results: list[CheckResult]) -> dict[str, Any]:
    errors = [r for r in results if r.level == "error" and not r.ok]
    warnings = [r for r in results if r.level == "warning" and not r.ok]
    return {
        "ok": not errors,
        "error_count": len(errors),
        "warning_count": len(warnings),
        "checks": [
            {
                "name": r.name,
                "level": r.level,
                "ok": r.ok,
                "details": r.details,
            }
            for r in results
        ],
    }


def main() -> int:
    args = parse_args()
    results = run_checks(args)
    report = serialize(results)
    print_text_report(results)

    if args.json_out:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(f"json_report={args.json_out}")

    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
