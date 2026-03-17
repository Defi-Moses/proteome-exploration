"""Simple worker entrypoint for Railway service orchestration."""

from __future__ import annotations

from datetime import datetime, timezone
import os
import time


def main() -> int:
    mode = os.environ.get("PANCCRE_WORKER_MODE", "heartbeat")
    interval = int(os.environ.get("PANCCRE_WORKER_INTERVAL_SEC", "30"))

    if mode == "once":
        print(f"worker_once timestamp_utc={datetime.now(timezone.utc).isoformat()}")
        return 0

    while True:
        print(f"worker_heartbeat timestamp_utc={datetime.now(timezone.utc).isoformat()}")
        time.sleep(max(interval, 5))


if __name__ == "__main__":
    raise SystemExit(main())
