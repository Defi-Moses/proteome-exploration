"""Railway worker entrypoint for heartbeat and pipeline orchestration."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import subprocess
import time


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _log(event: str, **fields: object) -> None:
    details = " ".join(f"{k}={v}" for k, v in fields.items())
    if details:
        print(f"{event} timestamp_utc={_utc_now()} {details}")
        return
    print(f"{event} timestamp_utc={_utc_now()}")


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _artifact_extension(output_format: str) -> str:
    if output_format == "jsonl":
        return "jsonl"
    if output_format == "csv":
        return "csv"
    return "parquet"


def _int_env(name: str, default_value: int, *, minimum: int = 1) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default_value
    value = int(raw)
    return max(value, minimum)


def _run_command(args: list[str], *, env: dict[str, str]) -> None:
    _log("worker_pipeline_command", command=" ".join(args))
    completed = subprocess.run(args, env=env, check=False)
    if completed.returncode != 0:
        raise RuntimeError(f"pipeline command failed (exit={completed.returncode}): {' '.join(args)}")


def _required_registry_files(output_format: str) -> tuple[str, ...]:
    ext = _artifact_extension(output_format)
    return (
        f"polymorphic_ccre_registry.{ext}",
        f"replacement_candidates.{ext}",
        f"scorer_outputs.{ext}",
        f"validation_links.{ext}",
        "registry_manifest.json",
    )


def _validate_registry_dir(path: Path, *, output_format: str) -> None:
    missing = [name for name in _required_registry_files(output_format) if not (path / name).exists()]
    if missing:
        raise FileNotFoundError(f"registry publish candidate missing files: {missing}")


def _discover_registry_artifact(base_dir: Path, stem: str) -> Path:
    for ext in ("parquet", "jsonl", "csv"):
        candidate = base_dir / f"{stem}.{ext}"
        if candidate.exists():
            return candidate
    raise FileNotFoundError(f"registry artifact not found for stem={stem} in {base_dir}")


def _rewrite_registry_manifest_paths(registry_dir: Path) -> None:
    manifest_path = registry_dir / "registry_manifest.json"
    if not manifest_path.exists():
        return

    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    files = payload.get("files")
    if not isinstance(files, dict):
        return

    rewritten: dict[str, str] = {}
    for stem in ("polymorphic_ccre_registry", "replacement_candidates", "scorer_outputs", "validation_links"):
        path = _discover_registry_artifact(registry_dir, stem)
        rewritten[stem] = str(path.resolve())

    payload["files"] = rewritten
    manifest_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _remove_dir_if_exists(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)


def _publish_registry_atomically(*, source_registry_dir: Path, target_registry_dir: Path) -> None:
    target_registry_dir.parent.mkdir(parents=True, exist_ok=True)
    next_dir = target_registry_dir.parent / f"{target_registry_dir.name}.__next__"
    prev_dir = target_registry_dir.parent / f"{target_registry_dir.name}.__prev__"

    _remove_dir_if_exists(next_dir)
    _remove_dir_if_exists(prev_dir)
    shutil.copytree(source_registry_dir, next_dir)

    if target_registry_dir.exists():
        os.replace(target_registry_dir, prev_dir)
    os.replace(next_dir, target_registry_dir)
    _rewrite_registry_manifest_paths(target_registry_dir)

    _remove_dir_if_exists(prev_dir)


def _run_pipeline_once() -> int:
    repo_root = _repo_root()
    run_script = repo_root / "scripts" / "run_phase1.py"
    if not run_script.exists():
        raise FileNotFoundError(f"Pipeline CLI wrapper not found: {run_script}")

    run_tag = os.environ.get("PANCCRE_PIPELINE_RUN_TAG", "").strip()
    if not run_tag:
        run_tag = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    output_root = Path(os.environ.get("PANCCRE_PIPELINE_OUTPUT_ROOT", "/data/runs"))
    output_root.mkdir(parents=True, exist_ok=True)
    run_dir = output_root / run_tag
    if run_dir.exists():
        raise FileExistsError(f"pipeline run directory already exists: {run_dir}")
    run_dir.mkdir(parents=True, exist_ok=False)

    intermediate_format = os.environ.get("PANCCRE_PIPELINE_INTERMEDIATE_FORMAT", "jsonl")
    registry_format = os.environ.get("PANCCRE_PIPELINE_REGISTRY_FORMAT", "csv")
    context_group = os.environ.get("PANCCRE_PIPELINE_CONTEXT", "immune_hematopoietic")
    shortlist_top = _int_env("PANCCRE_PIPELINE_SHORTLIST_TOP", 10000, minimum=1)

    ext = _artifact_extension(intermediate_format)
    smoke_ccre = run_dir / "smoke" / f"ccre_ref.{ext}"
    projection_path = run_dir / "projection" / f"hap_projection.{ext}"
    state_path = run_dir / "state" / f"ccre_state.{ext}"
    candidates_path = run_dir / "candidates" / f"replacement_candidates.{ext}"
    feature_path = run_dir / "features" / f"feature_matrix.{ext}"
    validation_link_path = run_dir / "validation" / f"validation_link.{ext}"
    publication_holdout_path = run_dir / "validation" / f"validation_link_publication.{ext}"
    locus_holdout_path = run_dir / "validation" / f"validation_link_locus.{ext}"
    shortlist_path = run_dir / "scorers" / f"shortlist.{ext}"
    scorer_output_path = run_dir / "scorers" / f"scorer_outputs.{ext}"

    max_alpha_calls = os.environ.get("PANCCRE_MAX_ALPHAGENOME_CALLS", "").strip()
    publish_registry_dir = Path(os.environ.get("PANCCRE_PUBLISH_REGISTRY_DIR", "/data/registry"))
    build_registry_dir = run_dir / "registry_build"
    report_enabled = os.environ.get("PANCCRE_BUILD_REPORT_BUNDLE", "1") != "0"
    report_output_root = Path(os.environ.get("PANCCRE_REPORT_OUTPUT_ROOT", "/data/reports"))
    report_top_hits_k = _int_env("PANCCRE_REPORT_TOP_HITS_K", 100, minimum=1)
    report_case_study_count = _int_env("PANCCRE_REPORT_CASE_STUDY_COUNT", 3, minimum=1)

    command_env = os.environ.copy()
    command_env["PYTHONPATH"] = str((repo_root / "src").resolve())

    commands: list[list[str]] = [
        ["python3", str(run_script), "smoke-ingest", "--output-dir", str(run_dir / "smoke"), "--output-format", intermediate_format],
        [
            "python3",
            str(run_script),
            "project-fixture",
            "--ccre-ref",
            str(smoke_ccre),
            "--output-dir",
            str(run_dir / "projection"),
            "--output-format",
            intermediate_format,
        ],
        [
            "python3",
            str(run_script),
            "call-states",
            "--hap-projection",
            str(projection_path),
            "--output-dir",
            str(run_dir / "state"),
            "--output-format",
            intermediate_format,
        ],
        [
            "python3",
            str(run_script),
            "discover-candidates",
            "--ccre-state",
            str(state_path),
            "--output-dir",
            str(run_dir / "candidates"),
            "--output-format",
            intermediate_format,
        ],
        [
            "python3",
            str(run_script),
            "featurize",
            "--ccre-state",
            str(state_path),
            "--replacement-candidates",
            str(candidates_path),
            "--output-dir",
            str(run_dir / "features"),
            "--output-format",
            intermediate_format,
        ],
        [
            "python3",
            str(run_script),
            "build-validation-link",
            "--ccre-state",
            str(state_path),
            "--output-dir",
            str(run_dir / "validation"),
            "--output-format",
            intermediate_format,
        ],
        [
            "python3",
            str(run_script),
            "build-holdouts",
            "--validation-link",
            str(validation_link_path),
            "--output-dir",
            str(run_dir / "validation"),
            "--output-format",
            intermediate_format,
        ],
        [
            "python3",
            str(run_script),
            "evaluate-ranking",
            "--feature-matrix",
            str(feature_path),
            "--publication-validation",
            str(publication_holdout_path),
            "--locus-validation",
            str(locus_holdout_path),
            "--output-dir",
            str(run_dir / "ranking"),
        ],
        [
            "python3",
            str(run_script),
            "shortlist",
            "--feature-matrix",
            str(feature_path),
            "--top",
            str(shortlist_top),
            "--output-dir",
            str(run_dir / "scorers"),
            "--output-format",
            intermediate_format,
        ],
        [
            "python3",
            str(run_script),
            "score-fanout",
            "--feature-matrix",
            str(feature_path),
            "--shortlist",
            str(shortlist_path),
            "--context-group",
            context_group,
            "--output-dir",
            str(run_dir / "scorers"),
            "--output-format",
            intermediate_format,
        ],
        [
            "python3",
            str(run_script),
            "compute-disagreement",
            "--scorer-outputs",
            str(scorer_output_path),
            "--output-dir",
            str(run_dir / "scorers"),
            "--output-format",
            intermediate_format,
        ],
        [
            "python3",
            str(run_script),
            "run-ablations",
            "--feature-matrix",
            str(feature_path),
            "--disagreement-features",
            str(run_dir / "scorers" / f"disagreement_features.{ext}"),
            "--publication-validation",
            str(publication_holdout_path),
            "--locus-validation",
            str(locus_holdout_path),
            "--output-dir",
            str(run_dir / "ranking"),
        ],
        [
            "python3",
            str(run_script),
            "build-registry",
            "--ccre-state",
            str(state_path),
            "--replacement-candidates",
            str(candidates_path),
            "--scorer-outputs",
            str(scorer_output_path),
            "--validation-links",
            str(validation_link_path),
            "--output-dir",
            str(build_registry_dir),
            "--output-format",
            registry_format,
            "--context-group",
            context_group,
        ],
    ]

    if max_alpha_calls:
        for command in commands:
            if len(command) >= 3 and command[2] == "score-fanout":
                command.extend(["--max-alphagenome-calls", max_alpha_calls])
                break

    freeze_enabled = os.environ.get("PANCCRE_FREEZE_EVALUATION", "1") != "0"
    if freeze_enabled:
        freeze_label = os.environ.get("PANCCRE_FREEZE_LABEL", "").strip() or run_tag
        freeze_output_root = Path(os.environ.get("PANCCRE_FREEZE_OUTPUT_ROOT", "/data/processed"))
        commands.append(
            [
                "python3",
                str(run_script),
                "freeze-evaluation",
                "--label",
                freeze_label,
                "--validation-dir",
                str(run_dir / "validation"),
                "--ranking-dir",
                str(run_dir / "ranking"),
                "--output-root",
                str(freeze_output_root),
            ]
        )

    if report_enabled:
        report_dir = report_output_root / run_tag
        commands.append(
            [
                "python3",
                str(run_script),
                "build-phase1-report",
                "--registry-dir",
                str(build_registry_dir),
                "--publication-ranking-report",
                str(run_dir / "ranking" / "ranking_publication_report.json"),
                "--locus-ranking-report",
                str(run_dir / "ranking" / "ranking_locus_report.json"),
                "--disagreement-features",
                str(run_dir / "scorers" / f"disagreement_features.{ext}"),
                "--ablation-summary",
                str(run_dir / "ranking" / "disagreement_ablation_summary.json"),
                "--output-dir",
                str(report_dir),
                "--top-hits-k",
                str(report_top_hits_k),
                "--case-study-count",
                str(report_case_study_count),
            ]
        )

    _log("worker_pipeline_start", run_tag=run_tag, run_dir=run_dir)
    for args in commands:
        _run_command(args, env=command_env)

    _validate_registry_dir(build_registry_dir, output_format=registry_format)
    _publish_registry_atomically(source_registry_dir=build_registry_dir, target_registry_dir=publish_registry_dir)
    _log("worker_pipeline_publish_complete", target_registry_dir=publish_registry_dir)
    _log("worker_pipeline_complete", run_tag=run_tag)
    return 0


def main() -> int:
    mode = os.environ.get("PANCCRE_WORKER_MODE", "heartbeat").strip()
    interval = _int_env("PANCCRE_WORKER_INTERVAL_SEC", 30, minimum=5)

    if mode == "once":
        _log("worker_once")
        return 0

    if mode == "pipeline_once":
        return _run_pipeline_once()

    if mode == "pipeline_loop":
        while True:
            try:
                _run_pipeline_once()
            except Exception as exc:  # pragma: no cover - defensive runtime guard
                _log("worker_pipeline_failed", error=repr(exc))
            time.sleep(interval)
        return 0

    if mode != "heartbeat":
        raise ValueError(f"Unsupported PANCCRE_WORKER_MODE: {mode}")

    while True:
        _log("worker_heartbeat")
        time.sleep(interval)


if __name__ == "__main__":
    raise SystemExit(main())
