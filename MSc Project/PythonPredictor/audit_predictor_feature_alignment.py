#!/usr/bin/env python3
"""Audit telemetry logs against the canonical long-model payload contract."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple

from predictor_input_contract import LONG_MODEL_PAYLOAD_REQUIREMENTS, find_missing_payload_requirements


def _extract_logged_topic_payload(record: dict) -> Tuple[str, Optional[Dict[str, object]]]:
    topic = str(record.get("topic") or "")
    payload = record.get("payload")
    if isinstance(payload, dict):
        return topic, payload
    if topic and isinstance(record.get("system"), dict):
        return topic, record
    return topic, None


def _iter_jsonl_files(log_dir: Path, plant_id: str) -> Iterable[Path]:
    prefix = f"{plant_id}_telemetry_"
    for path in sorted(log_dir.glob(f"{prefix}*.jsonl")):
        if path.is_file():
            yield path


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit telemetry logs for predictor feature alignment")
    parser.add_argument("--log-dir", required=True, help="Telemetry log directory")
    parser.add_argument("--plant-id", default="factory1", help="Plant identifier")
    parser.add_argument("--limit", type=int, default=500, help="Max telemetry records to inspect")
    args = parser.parse_args()

    log_dir = Path(args.log_dir).expanduser().resolve()
    if not log_dir.exists():
        raise SystemExit(f"log_dir_not_found:{log_dir}")

    missing_counter: Counter[str] = Counter()
    inspected = 0
    files_used = 0
    for path in _iter_jsonl_files(log_dir, args.plant_id):
        files_used += 1
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if inspected >= args.limit:
                    break
                text = line.strip()
                if not text:
                    continue
                try:
                    record = json.loads(text)
                except Exception:
                    continue
                topic, payload = _extract_logged_topic_payload(record if isinstance(record, dict) else {})
                if topic != f"dt/{args.plant_id}/telemetry" or not isinstance(payload, dict):
                    continue
                inspected += 1
                for name in find_missing_payload_requirements(payload):
                    missing_counter[name] += 1
        if inspected >= args.limit:
            break

    total_requirements = len(LONG_MODEL_PAYLOAD_REQUIREMENTS)
    missing_any = sorted(name for name, count in missing_counter.items() if count > 0)
    print(json.dumps(
        {
            "log_dir": str(log_dir),
            "plant_id": args.plant_id,
            "files_used": files_used,
            "records_inspected": inspected,
            "total_requirements": total_requirements,
            "requirements_missing_in_any_record": missing_any,
            "missing_counts": {name: missing_counter[name] for name in missing_any},
        },
        indent=2,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
