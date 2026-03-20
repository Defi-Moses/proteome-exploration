#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate an optimization brief template")
    parser.add_argument("--title", required=True, help="Optimization title")
    parser.add_argument("--output", required=True, type=Path, help="Output markdown path")
    parser.add_argument("--owner", default="", help="Owner name")
    parser.add_argument("--hotpath", default="", help="Target command/stage")
    parser.add_argument("--target-metric", default="", help="Primary target metric (e.g., wall time)")
    parser.add_argument("--target-improvement", default="", help="Expected improvement (for example: -30 percent duration)")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    timestamp = datetime.now(timezone.utc).isoformat()

    owner = args.owner or "<owner>"
    hotpath = args.hotpath or "<stage-or-command>"
    target_metric = args.target_metric or "<metric>"
    target_improvement = args.target_improvement or "<target-improvement>"

    content = f"""# Optimization Brief: {args.title}

Created: {timestamp}
Owner: {owner}

## 1) Scope

Target hot path: {hotpath}

In scope:
- 

Out of scope:
- 

## 2) Baseline

Primary metric: {target_metric}
Baseline value:

Secondary metrics:
- Peak RSS:
- Throughput:

Baseline evidence artifacts:
- 

## 3) Hypothesis

Expected improvement: {target_improvement}

Mechanism:
- 

Risks:
- 

## 4) Change Plan

Step plan:
1. 
2. 
3. 

Rollback plan:
- 

## 5) Validation Plan

Parity commands:
- 

Performance commands:
- 

Gate thresholds:
- Max duration regression:
- Max RSS regression:

## 6) Results

Before:
- 

After:
- 

Decision:
- [ ] Accept
- [ ] Reject

Follow-up:
- 
"""

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(content, encoding="utf-8")
    print(f"brief={args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
