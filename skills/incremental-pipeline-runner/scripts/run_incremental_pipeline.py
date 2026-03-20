#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable


STAGE_ORDER = [
    "ingest",
    "project",
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
]


@dataclass
class StageContext:
    repo_root: Path
    run_script: Path
    run_dir: Path
    intermediate_format: str
    registry_format: str
    projection_mode: str
    context_group: str
    shortlist_top: int
    source_release: str
    variants: str
    haplotypes: str
    max_variants: str
    ccre_bed: str
    assay_source: str
    assay_source_format: str
    max_alpha_calls: str
    include_freeze: bool
    include_report: bool
    freeze_label: str
    freeze_output_root: str
    report_output_root: str
    report_top_hits_k: int
    report_case_study_count: int


@dataclass
class StageSpec:
    name: str
    build_command: Callable[[StageContext], list[str]]
    required_inputs: Callable[[StageContext], list[Path]]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run minimal stage windows of PANCCRE pipeline")
    parser.add_argument("--run-dir", required=True, type=Path, help="Existing or target run directory")
    parser.add_argument("--start-stage", default="ingest", choices=STAGE_ORDER)
    parser.add_argument("--end-stage", default="build-registry", choices=STAGE_ORDER)
    parser.add_argument("--execute", action="store_true", help="Execute commands (default: dry-run)")
    parser.add_argument("--intermediate-format", default=os.environ.get("PANCCRE_PIPELINE_INTERMEDIATE_FORMAT", "jsonl"))
    parser.add_argument("--registry-format", default=os.environ.get("PANCCRE_PIPELINE_REGISTRY_FORMAT", "csv"))
    parser.add_argument("--projection-mode", default=os.environ.get("PANCCRE_PIPELINE_PROJECTION_MODE", "fixture"))
    parser.add_argument("--context-group", default=os.environ.get("PANCCRE_PIPELINE_CONTEXT", "immune_hematopoietic"))
    parser.add_argument("--shortlist-top", type=int, default=int(os.environ.get("PANCCRE_PIPELINE_SHORTLIST_TOP", "10000")))
    parser.add_argument("--source-release", default=os.environ.get("PANCCRE_PIPELINE_SOURCE_RELEASE", "fixture-2026-03"))
    parser.add_argument("--variants", default=os.environ.get("PANCCRE_PIPELINE_VARIANTS", ""))
    parser.add_argument("--haplotypes", default=os.environ.get("PANCCRE_PIPELINE_HAPLOTYPES", ""))
    parser.add_argument("--max-variants", default=os.environ.get("PANCCRE_PIPELINE_MAX_VARIANTS", ""))
    parser.add_argument("--ccre-bed", default=os.environ.get("PANCCRE_PIPELINE_CCRE_BED", ""))
    parser.add_argument("--assay-source", default=os.environ.get("PANCCRE_PIPELINE_ASSAY_SOURCE", ""))
    parser.add_argument("--assay-source-format", default=os.environ.get("PANCCRE_PIPELINE_ASSAY_SOURCE_FORMAT", "csv"))
    parser.add_argument("--max-alpha-calls", default=os.environ.get("PANCCRE_MAX_ALPHAGENOME_CALLS", ""))
    parser.add_argument("--include-freeze", action="store_true")
    parser.add_argument("--freeze-label", default=os.environ.get("PANCCRE_FREEZE_LABEL", ""))
    parser.add_argument("--freeze-output-root", default=os.environ.get("PANCCRE_FREEZE_OUTPUT_ROOT", "/data/processed"))
    parser.add_argument("--include-report", action="store_true")
    parser.add_argument("--report-output-root", default=os.environ.get("PANCCRE_REPORT_OUTPUT_ROOT", "/data/reports"))
    parser.add_argument("--report-top-hits-k", type=int, default=int(os.environ.get("PANCCRE_REPORT_TOP_HITS_K", "100")))
    parser.add_argument("--report-case-study-count", type=int, default=int(os.environ.get("PANCCRE_REPORT_CASE_STUDY_COUNT", "3")))
    parser.add_argument("--allow-missing-inputs", action="store_true", help="Do not fail dry-run on missing inputs")
    return parser.parse_args()


def ext_for_format(fmt: str) -> str:
    if fmt == "jsonl":
        return "jsonl"
    if fmt == "csv":
        return "csv"
    return "parquet"


def build_paths(ctx: StageContext) -> dict[str, Path]:
    ext = ext_for_format(ctx.intermediate_format)
    paths = {
        "smoke_ccre": ctx.run_dir / "smoke" / f"ccre_ref.{ext}",
        "projection": ctx.run_dir / "projection" / f"hap_projection.{ext}",
        "state": ctx.run_dir / "state" / f"ccre_state.{ext}",
        "candidates": ctx.run_dir / "candidates" / f"replacement_candidates.{ext}",
        "features": ctx.run_dir / "features" / f"feature_matrix.{ext}",
        "validation": ctx.run_dir / "validation" / f"validation_link.{ext}",
        "holdout_publication": ctx.run_dir / "validation" / f"validation_link_publication.{ext}",
        "holdout_locus": ctx.run_dir / "validation" / f"validation_link_locus.{ext}",
        "shortlist": ctx.run_dir / "scorers" / f"shortlist.{ext}",
        "scorer_outputs": ctx.run_dir / "scorers" / f"scorer_outputs.{ext}",
        "disagreement": ctx.run_dir / "scorers" / f"disagreement_features.{ext}",
        "registry_build": ctx.run_dir / "registry_build",
    }
    return paths


def ensure_parent_dirs(command: list[str]) -> None:
    for token in command:
        if token.startswith("/") or token.startswith("."):
            path = Path(token)
            if path.suffix:
                path.parent.mkdir(parents=True, exist_ok=True)


def stage_specs(ctx: StageContext) -> dict[str, StageSpec]:
    p = build_paths(ctx)

    def ingest_command(_: StageContext) -> list[str]:
        if ctx.ccre_bed:
            return [
                "python3",
                str(ctx.run_script),
                "ingest-ccre",
                "--input-bed",
                ctx.ccre_bed,
                "--output-dir",
                str(ctx.run_dir / "smoke"),
                "--context-group",
                ctx.context_group,
                "--source-release",
                ctx.source_release,
                "--output-format",
                ctx.intermediate_format,
            ]
        return [
            "python3",
            str(ctx.run_script),
            "smoke-ingest",
            "--output-dir",
            str(ctx.run_dir / "smoke"),
            "--output-format",
            ctx.intermediate_format,
        ]

    def ingest_required(_: StageContext) -> list[Path]:
        return [Path(ctx.ccre_bed)] if ctx.ccre_bed else []

    def project_command(_: StageContext) -> list[str]:
        if ctx.projection_mode == "vcf":
            command = [
                "python3",
                str(ctx.run_script),
                "project-vcf",
                "--ccre-ref",
                str(p["smoke_ccre"]),
                "--ccre-ref-format",
                ctx.intermediate_format,
                "--variants",
                ctx.variants,
                "--output-dir",
                str(ctx.run_dir / "projection"),
                "--output-format",
                ctx.intermediate_format,
            ]
            if ctx.haplotypes:
                command.extend(["--haplotypes", ctx.haplotypes])
            if ctx.max_variants:
                command.extend(["--max-variants", ctx.max_variants])
            return command

        return [
            "python3",
            str(ctx.run_script),
            "project-fixture",
            "--ccre-ref",
            str(p["smoke_ccre"]),
            "--output-dir",
            str(ctx.run_dir / "projection"),
            "--output-format",
            ctx.intermediate_format,
        ]

    def project_required(_: StageContext) -> list[Path]:
        required = [p["smoke_ccre"]]
        if ctx.projection_mode == "vcf":
            required.append(Path(ctx.variants))
            if ctx.haplotypes:
                required.append(Path(ctx.haplotypes))
        return required

    def validation_command(_: StageContext) -> list[str]:
        command = [
            "python3",
            str(ctx.run_script),
            "build-validation-link",
            "--ccre-state",
            str(p["state"]),
            "--output-dir",
            str(ctx.run_dir / "validation"),
            "--output-format",
            ctx.intermediate_format,
        ]
        if ctx.assay_source:
            command.extend(["--assay-source", ctx.assay_source, "--assay-source-format", ctx.assay_source_format])
        return command

    def validation_required(_: StageContext) -> list[Path]:
        required = [p["state"]]
        if ctx.assay_source:
            required.append(Path(ctx.assay_source))
        return required

    specs = {
        "ingest": StageSpec("ingest", ingest_command, ingest_required),
        "project": StageSpec("project", project_command, project_required),
        "call-states": StageSpec(
            "call-states",
            lambda _: [
                "python3",
                str(ctx.run_script),
                "call-states",
                "--hap-projection",
                str(p["projection"]),
                "--output-dir",
                str(ctx.run_dir / "state"),
                "--output-format",
                ctx.intermediate_format,
            ],
            lambda _: [p["projection"]],
        ),
        "discover-candidates": StageSpec(
            "discover-candidates",
            lambda _: [
                "python3",
                str(ctx.run_script),
                "discover-candidates",
                "--ccre-state",
                str(p["state"]),
                "--output-dir",
                str(ctx.run_dir / "candidates"),
                "--output-format",
                ctx.intermediate_format,
            ],
            lambda _: [p["state"]],
        ),
        "featurize": StageSpec(
            "featurize",
            lambda _: [
                "python3",
                str(ctx.run_script),
                "featurize",
                "--ccre-state",
                str(p["state"]),
                "--replacement-candidates",
                str(p["candidates"]),
                "--output-dir",
                str(ctx.run_dir / "features"),
                "--output-format",
                ctx.intermediate_format,
            ],
            lambda _: [p["state"], p["candidates"]],
        ),
        "build-validation-link": StageSpec("build-validation-link", validation_command, validation_required),
        "build-holdouts": StageSpec(
            "build-holdouts",
            lambda _: [
                "python3",
                str(ctx.run_script),
                "build-holdouts",
                "--validation-link",
                str(p["validation"]),
                "--output-dir",
                str(ctx.run_dir / "validation"),
                "--output-format",
                ctx.intermediate_format,
            ],
            lambda _: [p["validation"]],
        ),
        "evaluate-ranking": StageSpec(
            "evaluate-ranking",
            lambda _: [
                "python3",
                str(ctx.run_script),
                "evaluate-ranking",
                "--feature-matrix",
                str(p["features"]),
                "--publication-validation",
                str(p["holdout_publication"]),
                "--locus-validation",
                str(p["holdout_locus"]),
                "--output-dir",
                str(ctx.run_dir / "ranking"),
            ],
            lambda _: [p["features"], p["holdout_publication"], p["holdout_locus"]],
        ),
        "shortlist": StageSpec(
            "shortlist",
            lambda _: [
                "python3",
                str(ctx.run_script),
                "shortlist",
                "--feature-matrix",
                str(p["features"]),
                "--top",
                str(ctx.shortlist_top),
                "--output-dir",
                str(ctx.run_dir / "scorers"),
                "--output-format",
                ctx.intermediate_format,
            ],
            lambda _: [p["features"]],
        ),
        "score-fanout": StageSpec(
            "score-fanout",
            lambda _: [
                "python3",
                str(ctx.run_script),
                "score-fanout",
                "--feature-matrix",
                str(p["features"]),
                "--shortlist",
                str(p["shortlist"]),
                "--context-group",
                ctx.context_group,
                "--output-dir",
                str(ctx.run_dir / "scorers"),
                "--output-format",
                ctx.intermediate_format,
            ]
            + (["--max-alphagenome-calls", ctx.max_alpha_calls] if ctx.max_alpha_calls else []),
            lambda _: [p["features"], p["shortlist"]],
        ),
        "compute-disagreement": StageSpec(
            "compute-disagreement",
            lambda _: [
                "python3",
                str(ctx.run_script),
                "compute-disagreement",
                "--scorer-outputs",
                str(p["scorer_outputs"]),
                "--output-dir",
                str(ctx.run_dir / "scorers"),
                "--output-format",
                ctx.intermediate_format,
            ],
            lambda _: [p["scorer_outputs"]],
        ),
        "run-ablations": StageSpec(
            "run-ablations",
            lambda _: [
                "python3",
                str(ctx.run_script),
                "run-ablations",
                "--feature-matrix",
                str(p["features"]),
                "--disagreement-features",
                str(p["disagreement"]),
                "--publication-validation",
                str(p["holdout_publication"]),
                "--locus-validation",
                str(p["holdout_locus"]),
                "--output-dir",
                str(ctx.run_dir / "ranking"),
            ],
            lambda _: [p["features"], p["disagreement"], p["holdout_publication"], p["holdout_locus"]],
        ),
        "build-registry": StageSpec(
            "build-registry",
            lambda _: [
                "python3",
                str(ctx.run_script),
                "build-registry",
                "--ccre-state",
                str(p["state"]),
                "--replacement-candidates",
                str(p["candidates"]),
                "--scorer-outputs",
                str(p["scorer_outputs"]),
                "--validation-links",
                str(p["validation"]),
                "--output-dir",
                str(p["registry_build"]),
                "--output-format",
                ctx.registry_format,
                "--context-group",
                ctx.context_group,
            ],
            lambda _: [p["state"], p["candidates"], p["scorer_outputs"], p["validation"]],
        ),
    }

    return specs


def extend_optional_stages(plan: list[tuple[str, list[str], list[Path]]], ctx: StageContext) -> list[tuple[str, list[str], list[Path]]]:
    p = build_paths(ctx)

    if ctx.include_freeze:
        freeze_label = ctx.freeze_label or ctx.run_dir.name
        plan.append(
            (
                "freeze-evaluation",
                [
                    "python3",
                    str(ctx.run_script),
                    "freeze-evaluation",
                    "--label",
                    freeze_label,
                    "--validation-dir",
                    str(ctx.run_dir / "validation"),
                    "--ranking-dir",
                    str(ctx.run_dir / "ranking"),
                    "--output-root",
                    ctx.freeze_output_root,
                ],
                [ctx.run_dir / "validation", ctx.run_dir / "ranking"],
            )
        )

    if ctx.include_report:
        report_dir = Path(ctx.report_output_root) / ctx.run_dir.name
        plan.append(
            (
                "build-phase1-report",
                [
                    "python3",
                    str(ctx.run_script),
                    "build-phase1-report",
                    "--registry-dir",
                    str(p["registry_build"]),
                    "--publication-ranking-report",
                    str(ctx.run_dir / "ranking" / "ranking_publication_report.json"),
                    "--locus-ranking-report",
                    str(ctx.run_dir / "ranking" / "ranking_locus_report.json"),
                    "--disagreement-features",
                    str(p["disagreement"]),
                    "--ablation-summary",
                    str(ctx.run_dir / "ranking" / "disagreement_ablation_summary.json"),
                    "--output-dir",
                    str(report_dir),
                    "--top-hits-k",
                    str(ctx.report_top_hits_k),
                    "--case-study-count",
                    str(ctx.report_case_study_count),
                ],
                [
                    p["registry_build"],
                    ctx.run_dir / "ranking" / "ranking_publication_report.json",
                    ctx.run_dir / "ranking" / "ranking_locus_report.json",
                    p["disagreement"],
                    ctx.run_dir / "ranking" / "disagreement_ablation_summary.json",
                ],
            )
        )

    return plan


def build_plan(ctx: StageContext, start_stage: str, end_stage: str) -> list[tuple[str, list[str], list[Path]]]:
    start_index = STAGE_ORDER.index(start_stage)
    end_index = STAGE_ORDER.index(end_stage)
    if end_index < start_index:
        raise ValueError("--end-stage must not come before --start-stage")

    specs = stage_specs(ctx)

    plan: list[tuple[str, list[str], list[Path]]] = []
    for stage_name in STAGE_ORDER[start_index : end_index + 1]:
        spec = specs[stage_name]
        plan.append((stage_name, spec.build_command(ctx), spec.required_inputs(ctx)))

    return extend_optional_stages(plan, ctx)


def validate_required_inputs(plan: list[tuple[str, list[str], list[Path]]], allow_missing: bool) -> int:
    missing_count = 0
    for stage_name, _, required_paths in plan:
        for path in required_paths:
            if str(path).strip() == "":
                print(f"[FAIL] stage={stage_name} required_path=<empty>")
                missing_count += 1
                continue
            if not path.exists():
                print(f"[FAIL] stage={stage_name} missing_input={path}")
                missing_count += 1

    if missing_count and not allow_missing:
        print(f"missing_inputs={missing_count}")
    return missing_count


def run_plan(plan: list[tuple[str, list[str], list[Path]]], env: dict[str, str]) -> int:
    for stage_name, command, _ in plan:
        ensure_parent_dirs(command)
        print(f"[RUN] stage={stage_name} command={' '.join(command)}")
        completed = subprocess.run(command, env=env, check=False)
        if completed.returncode != 0:
            print(f"[FAIL] stage={stage_name} exit={completed.returncode}")
            return completed.returncode
    print("[OK] incremental pipeline plan complete")
    return 0


def main() -> int:
    args = parse_args()

    if args.projection_mode not in {"fixture", "vcf"}:
        print("--projection-mode must be fixture|vcf", file=sys.stderr)
        return 2
    if args.assay_source_format not in {"csv", "jsonl", "parquet"}:
        print("--assay-source-format must be csv|jsonl|parquet", file=sys.stderr)
        return 2
    if args.projection_mode == "vcf" and not args.variants:
        print("VCF projection mode requires --variants (or PANCCRE_PIPELINE_VARIANTS)", file=sys.stderr)
        return 2

    repo_root = Path(__file__).resolve().parents[3]
    run_script = repo_root / "scripts" / "run_phase1.py"
    if not run_script.exists():
        print(f"missing run script: {run_script}", file=sys.stderr)
        return 2

    if args.execute:
        args.run_dir.mkdir(parents=True, exist_ok=True)

    ctx = StageContext(
        repo_root=repo_root,
        run_script=run_script,
        run_dir=args.run_dir,
        intermediate_format=args.intermediate_format,
        registry_format=args.registry_format,
        projection_mode=args.projection_mode,
        context_group=args.context_group,
        shortlist_top=args.shortlist_top,
        source_release=args.source_release,
        variants=args.variants,
        haplotypes=args.haplotypes,
        max_variants=args.max_variants,
        ccre_bed=args.ccre_bed,
        assay_source=args.assay_source,
        assay_source_format=args.assay_source_format,
        max_alpha_calls=args.max_alpha_calls,
        include_freeze=args.include_freeze,
        include_report=args.include_report,
        freeze_label=args.freeze_label,
        freeze_output_root=args.freeze_output_root,
        report_output_root=args.report_output_root,
        report_top_hits_k=args.report_top_hits_k,
        report_case_study_count=args.report_case_study_count,
    )

    plan = build_plan(ctx, args.start_stage, args.end_stage)

    print(f"run_dir={args.run_dir}")
    print(f"execute={args.execute}")
    print(f"stage_window={args.start_stage}->{args.end_stage}")
    for stage_name, command, _ in plan:
        print(f"[PLAN] stage={stage_name} command={' '.join(command)}")

    missing_inputs = validate_required_inputs(plan, args.allow_missing_inputs or not args.execute)
    if missing_inputs and args.execute and not args.allow_missing_inputs:
        return 1

    if not args.execute:
        print("dry_run_only=1")
        return 0

    command_env = dict(os.environ)
    command_env["PYTHONPATH"] = str((repo_root / "src").resolve())
    return run_plan(plan, command_env)


if __name__ == "__main__":
    raise SystemExit(main())
