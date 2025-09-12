"""
CGM sanity checks that work with both:
  1) Flat records like {"start_date", "end_date", "blood_glucose": 110, ...}
  2) fcgm records under body.cgm[], with nested fields:
       effective_time_frame.time_interval.start_date_time / end_date_time
       blood_glucose.value
       transmitter_time.value

Usage:
    from cgm_sanity import sanity_check_cgm_file
    summary = sanity_check_cgm_file("/path/to/file.json", logger)
"""

from datetime import datetime
import json
from typing import Any, Dict, List, Tuple, Optional
import contextlib


def _parse_iso8601(s: Any) -> Optional[datetime]:
    if not isinstance(s, str):
        return None
    try:
        s2 = s.replace("Z", "+00:00") if s.endswith("Z") else s
        return datetime.fromisoformat(s2)
    except Exception:
        return None


def _iter_records(obj: Any) -> List[Dict[str, Any]]:
    """
    Return the records list from common CGM shapes:
      - top-level list
      - {"records": [...]}
      - {"data": [...]}
      - {"egvs": [...]}
      - {"body": {"cgm": [...]}}  <-- fcgm format
    """
    if isinstance(obj, list):
        return obj
    if isinstance(obj, dict):
        for key in ["records", "data", "egvs"]:
            val = obj.get(key)
            if isinstance(val, list):
                return val
        body = obj.get("body")
        if isinstance(body, dict) and isinstance(body.get("cgm"), list):
            return body["cgm"]
    return []


def _extract_fields(rec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract a unified set of fields from either flat or fcgm records.
    Returns dict with keys:
      start, end, glucose_value, uuid, event_type, source_device_id, transmitter_id, transmitter_time_value
    Any missing field is None.
    """
    start = end = glucose_value = uuid = event_type = source_device_id = (
        transmitter_id
    ) = None
    transmitter_time_value = None

    if not isinstance(rec, dict):
        return {
            "start": None,
            "end": None,
            "glucose_value": None,
            "uuid": None,
            "event_type": None,
            "source_device_id": None,
            "transmitter_id": None,
            "transmitter_time_value": None,
        }

    # fcgm nested time field
    with contextlib.suppress(Exception):
        etf = rec.get("effective_time_frame") or {}
        ti = etf.get("time_interval") or {}
        start = ti.get("start_date_time", start)
        end = ti.get("end_date_time", end)

    # flat time fields fallback
    start = rec.get("start_date", start)
    end = rec.get("end_date", end)

    # glucose, handle flat numeric or nested {"value": ...}
    bg = rec.get("blood_glucose")
    glucose_value = bg.get("value") if isinstance(bg, dict) else bg

    # ids and metadata
    uuid = rec.get("uuid")
    event_type = rec.get("event_type")
    source_device_id = rec.get("source_device_id")
    transmitter_id = rec.get("transmitter_id")

    tt = rec.get("transmitter_time")
    transmitter_time_value = tt.get("value") if isinstance(tt, dict) else tt

    return {
        "start": start,
        "end": end,
        "glucose_value": glucose_value,
        "uuid": uuid,
        "event_type": event_type,
        "source_device_id": source_device_id,
        "transmitter_id": transmitter_id,
        "transmitter_time_value": transmitter_time_value,
    }


def _make_key(rec: Dict[str, Any]) -> Tuple[Any, ...] or str:
    """
    Stable key for duplicate detection.
    Use extracted fields when available. Fall back to canonical JSON.
    Coerce to str to avoid 110 vs "110" mismatches.
    """
    try:
        f = _extract_fields(rec)
        return (
            str(f["uuid"]),
            str(f["start"]),
            str(f["end"]),
            str(f["event_type"]),
            str(f["source_device_id"]),
            str(f["transmitter_id"]),
            str(f["transmitter_time_value"]),
            str(f["glucose_value"]),
        )
    except Exception:
        try:
            return json.dumps(rec, sort_keys=True, separators=(",", ":"))
        except Exception:
            return str(rec)


def sanity_check_cgm_file(
    file_path: str, logger, max_log_examples: int = 50
) -> Dict[str, int]:
    """
    Run sanity checks and log issues via the given logger.

    Returns a summary dict:
      {
        "bad_date_cnt": int,
        "negative_glucose_cnt": int,
        "duplicate_groups": int,
        "total_duplicate_records": int,
        "total_records": int
      }
    """
    summary = {
        "bad_date_cnt": 0,
        "negative_glucose_cnt": 0,
        "duplicate_groups": 0,
        "total_duplicate_records": 0,
        "total_records": 0,
    }

    # Load JSON
    try:
        with open(file_path, "r") as f:
            data = json.load(f)
    except Exception as e:
        logger.error(f"[SANITY CHECK] Failed to read {file_path}: {e}")
        return summary

    records = _iter_records(data)
    summary["total_records"] = len(records)

    if not records:
        logger.warn(f"[SANITY CHECK] No records found in {file_path}")
        return summary

    # 1) date ordering and 2) negative glucose
    for idx, rec in enumerate(records):
        if not isinstance(rec, dict):
            continue

        f = _extract_fields(rec)
        dt_s = _parse_iso8601(f["start"])
        dt_e = _parse_iso8601(f["end"])

        # Date ordering check
        if dt_s is not None and dt_e is not None and dt_s > dt_e:
            summary["bad_date_cnt"] += 1
            if summary["bad_date_cnt"] <= max_log_examples:
                logger.error(
                    f"[SANITY CHECK] start > end at index {idx} in {file_path}: {f['start']} > {f['end']}"
                )
        elif isinstance(f["start"], str) and isinstance(f["end"], str):
            # Fallback to string compare if parse fails
            if f["start"] > f["end"]:
                summary["bad_date_cnt"] += 1
                if summary["bad_date_cnt"] <= max_log_examples:
                    logger.error(
                        f"[SANITY CHECK] start > end (string compare) at index {idx} in {file_path}: {f['start']} > {f['end']}"
                    )

        # Negative glucose
        with contextlib.suppress(Exception):
            if f["glucose_value"] is not None and float(f["glucose_value"]) < 0:
                summary["negative_glucose_cnt"] += 1
                if summary["negative_glucose_cnt"] <= max_log_examples:
                    logger.error(
                        f"[SANITY CHECK] Negative blood_glucose at index {idx} in {file_path}: {f['glucose_value']}"
                    )

    if summary["bad_date_cnt"]:
        logger.error(
            f"[SANITY CHECK] Total records with start_date > end_date: {summary['bad_date_cnt']}"
        )
    if summary["negative_glucose_cnt"]:
        logger.error(
            f"[SANITY CHECK] Total records with negative blood_glucose: {summary['negative_glucose_cnt']}"
        )

    # 3) duplicate detection
    seen = {}
    for idx, rec in enumerate(records):
        key = _make_key(rec)
        seen.setdefault(key, []).append(idx)

    duplicate_groups = [(k, idxs) for k, idxs in seen.items() if len(idxs) > 1]
    summary["duplicate_groups"] = len(duplicate_groups)
    summary["total_duplicate_records"] = sum(
        len(idxs) - 1 for _, idxs in duplicate_groups
    )

    if duplicate_groups:
        logger.error(
            f"[SANITY CHECK] Duplicate records detected: {summary['total_duplicate_records']} duplicates across {summary['duplicate_groups']} groups"
        )
        # Log up to max_log_examples example groups
        logged = 0
        for _, idxs in duplicate_groups:
            if logged >= max_log_examples:
                break
            logger.error(f"[SANITY CHECK] Duplicate group indices: {idxs}")
            logged += 1

    return summary
