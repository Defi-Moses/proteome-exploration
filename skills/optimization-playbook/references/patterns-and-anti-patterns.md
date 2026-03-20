# Patterns and Anti-Patterns

## Approved Patterns

- Replace full materialization with streaming/chunking where contract allows.
- Remove redundant dataframe/table copies.
- Preserve deterministic row ordering and schema contracts.
- Cache stable, reusable intermediate data with explicit invalidation.

## Anti-Patterns

- Optimize without baseline metrics.
- Combine multiple unrelated optimization hypotheses in one patch.
- Merge speed improvements without parity validation.
- Mask regressions by widening thresholds without rationale.

## Evidence Minimum

- Before/after runtime metrics.
- Before/after memory metrics when relevant.
- Parity gate results.
- Clear rollback strategy.
