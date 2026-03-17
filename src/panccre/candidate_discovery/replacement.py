"""Replacement candidate discovery from cCRE state rows."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path

import pandas as pd

from panccre.state_calling import CCRE_STATE_COLUMNS

REPLACEMENT_CANDIDATE_COLUMNS = [
    "candidate_id",
    "parent_ccre_id",
    "haplotype_id",
    "window_class",
    "alt_contig",
    "alt_start",
    "alt_end",
    "seq_len",
    "repeat_class",
    "te_family",
    "motif_count",
    "gc_content",
    "nearest_gene",
    "nearest_gene_distance",
]

_REPEAT_CLASSES = ["LINE", "SINE", "LTR", "DNA", "low_complexity"]
_TE_FAMILIES = ["L1", "Alu", "ERV", "hAT", "simple"]


@dataclass(frozen=True)
class CandidateDiscoveryResult:
    row_count: int
    output_path: Path
    qc_summary_path: Path
    output_format: str


def _parquet_available() -> bool:
    try:
        pd.io.parquet.get_engine("auto")
        return True
    except Exception:
        return False


def _infer_format_from_path(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return "parquet"
    if suffix == ".csv":
        return "csv"
    if suffix in {".jsonl", ".ndjson"}:
        return "jsonl"
    raise ValueError(f"Could not infer format from extension: {path}")


def read_ccre_state(path: str | Path, *, input_format: str | None = None) -> pd.DataFrame:
    file_path = Path(path)
    fmt = (input_format or _infer_format_from_path(file_path)).lower()

    if fmt == "parquet":
        if not _parquet_available():
            raise RuntimeError("Reading parquet requires pyarrow or fastparquet")
        frame = pd.read_parquet(file_path)
    elif fmt == "csv":
        frame = pd.read_csv(file_path)
    elif fmt == "jsonl":
        frame = pd.read_json(file_path, lines=True)
    else:
        raise ValueError("input_format must be one of: parquet, csv, jsonl")

    actual = list(frame.columns)
    if actual != CCRE_STATE_COLUMNS:
        raise ValueError(f"ccre_state column contract mismatch: expected={CCRE_STATE_COLUMNS} actual={actual}")
    if frame.empty:
        raise ValueError("ccre_state frame must not be empty")
    return frame


def _stable_int(value: str, modulo: int) -> int:
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
    return int(digest[:10], 16) % modulo


def _parse_state_reason(payload: str) -> dict[str, object]:
    obj = json.loads(payload)
    if not isinstance(obj, dict):
        raise ValueError("state_reason must decode to object")
    return obj


def discover_replacement_candidates(state: pd.DataFrame) -> pd.DataFrame:
    """Generate deterministic replacement candidates for altered states."""
    candidates: list[dict[str, object]] = []

    for _, row in state.iterrows():
        state_class = str(row["state_class"])
        if state_class not in {"absent", "fractured", "duplicated"}:
            continue

        reason = _parse_state_reason(str(row["state_reason"]))
        parent_ccre_id = str(row["ccre_id"])
        haplotype_id = str(row["haplotype_id"])
        token = f"{parent_ccre_id}|{haplotype_id}|{state_class}"

        ref_chr = str(reason.get("ref_chr"))
        ref_start = int(reason.get("ref_start"))
        ref_end = int(reason.get("ref_end"))

        alt_contig = reason.get("alt_contig")
        if alt_contig is None:
            alt_contig = ref_chr
        alt_contig = str(alt_contig)

        if state_class == "absent":
            window_class = "absent_window"
            alt_start = ref_start - 100
            alt_end = ref_end + 100
        elif state_class == "fractured":
            window_class = "fracture_gap"
            alt_start = int(reason.get("alt_start") or ref_start)
            alt_end = int(reason.get("alt_end") or (ref_start + (ref_end - ref_start) // 2))
        else:
            window_class = "duplicate_neighbor"
            alt_start = int(reason.get("alt_start") or (ref_start - 50))
            alt_end = int(reason.get("alt_end") or (ref_end - 50))

        if alt_end <= alt_start:
            alt_end = alt_start + 50

        seq_len = alt_end - alt_start
        repeat_class = _REPEAT_CLASSES[_stable_int(token + "|repeat", len(_REPEAT_CLASSES))]
        te_family = _TE_FAMILIES[_stable_int(token + "|te", len(_TE_FAMILIES))]
        motif_count = 1 + _stable_int(token + "|motif", 15)
        gc_content = round(0.30 + (_stable_int(token + "|gc", 35) / 100), 3)
        nearest_gene_distance = 100 + _stable_int(token + "|dist", 40000)
        nearest_gene = f"GENE{1000 + _stable_int(token + '|gene', 9000)}"

        candidate_id = f"cand_{parent_ccre_id}_{haplotype_id}".replace("|", "_")

        candidates.append(
            {
                "candidate_id": candidate_id,
                "parent_ccre_id": parent_ccre_id,
                "haplotype_id": haplotype_id,
                "window_class": window_class,
                "alt_contig": alt_contig,
                "alt_start": int(alt_start),
                "alt_end": int(alt_end),
                "seq_len": int(seq_len),
                "repeat_class": repeat_class,
                "te_family": te_family,
                "motif_count": int(motif_count),
                "gc_content": float(gc_content),
                "nearest_gene": nearest_gene,
                "nearest_gene_distance": int(nearest_gene_distance),
            }
        )

    frame = pd.DataFrame(candidates, columns=REPLACEMENT_CANDIDATE_COLUMNS)
    validate_replacement_candidates(frame)
    return frame


def validate_replacement_candidates(frame: pd.DataFrame) -> None:
    actual = list(frame.columns)
    if actual != REPLACEMENT_CANDIDATE_COLUMNS:
        raise ValueError(
            "replacement_candidate column contract mismatch: "
            f"expected={REPLACEMENT_CANDIDATE_COLUMNS} actual={actual}"
        )
    if frame.empty:
        raise ValueError("replacement_candidate frame must not be empty")
    if frame["candidate_id"].duplicated().any():
        raise ValueError("replacement_candidate contains duplicate candidate_id values")
    if (frame["alt_end"] <= frame["alt_start"]).any():
        raise ValueError("replacement_candidate contains rows where alt_end <= alt_start")


def _write_frame(frame: pd.DataFrame, path: Path, output_format: str) -> None:
    fmt = output_format.lower()
    if fmt == "parquet":
        if not _parquet_available():
            raise RuntimeError(
                "Parquet output requires pyarrow or fastparquet. "
                "Install one of those engines or choose --output-format csv/jsonl."
            )
        frame.to_parquet(path, index=False)
    elif fmt == "csv":
        frame.to_csv(path, index=False)
    elif fmt == "jsonl":
        frame.to_json(path, orient="records", lines=True)
    else:
        raise ValueError("output_format must be one of: parquet, csv, jsonl")


def build_candidate_qc_summary(frame: pd.DataFrame) -> dict[str, object]:
    window_counts = frame["window_class"].value_counts().sort_index().to_dict()
    repeat_counts = frame["repeat_class"].value_counts().sort_index().to_dict()
    return {
        "row_count": int(frame.shape[0]),
        "window_class_counts": {str(k): int(v) for k, v in window_counts.items()},
        "repeat_class_counts": {str(k): int(v) for k, v in repeat_counts.items()},
        "seq_len": {
            "min": int(frame["seq_len"].min()),
            "mean": float(frame["seq_len"].mean()),
            "max": int(frame["seq_len"].max()),
        },
    }


def write_candidate_qc_summary(summary: dict[str, object], path: str | Path) -> Path:
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return out


def run_candidate_discovery(
    *,
    ccre_state_path: str | Path,
    output_path: str | Path,
    qc_summary_path: str | Path,
    output_format: str = "jsonl",
    ccre_state_format: str | None = None,
) -> CandidateDiscoveryResult:
    state = read_ccre_state(ccre_state_path, input_format=ccre_state_format)
    candidates = discover_replacement_candidates(state)

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    _write_frame(candidates, out, output_format)

    summary = build_candidate_qc_summary(candidates)
    qc = write_candidate_qc_summary(summary, qc_summary_path)

    return CandidateDiscoveryResult(
        row_count=int(candidates.shape[0]),
        output_path=out,
        qc_summary_path=qc,
        output_format=output_format,
    )
