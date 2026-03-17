"""Scorer fanout, disagreement features, and ablation tooling."""

from panccre.scorers.ablation import AblationResult, run_disagreement_ablation
from panccre.scorers.fanout import (
    DEFAULT_EXPECTED_SCORERS,
    DISAGREEMENT_COLUMNS,
    SCORER_OUTPUT_COLUMNS,
    SHORTLIST_COLUMNS,
    DisagreementResult,
    ScorerFanoutResult,
    ShortlistResult,
    build_disagreement_features,
    build_shortlist,
    disagreement_to_feature_rows,
    run_disagreement_build,
    run_scorer_fanout,
    run_shortlist_build,
    validate_disagreement_features,
    validate_scorer_output,
    validate_shortlist,
)

__all__ = [
    "AblationResult",
    "DEFAULT_EXPECTED_SCORERS",
    "DISAGREEMENT_COLUMNS",
    "SCORER_OUTPUT_COLUMNS",
    "SHORTLIST_COLUMNS",
    "DisagreementResult",
    "ScorerFanoutResult",
    "ShortlistResult",
    "build_disagreement_features",
    "build_shortlist",
    "disagreement_to_feature_rows",
    "run_disagreement_ablation",
    "run_disagreement_build",
    "run_scorer_fanout",
    "run_shortlist_build",
    "validate_disagreement_features",
    "validate_scorer_output",
    "validate_shortlist",
]
