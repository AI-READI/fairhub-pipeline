# garmin_sanity.py
from __future__ import annotations
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Tuple, Optional, Iterable
import json
import re
import contextlib

# ----------------------------
# Time & record helpers
# ----------------------------


def _parse_iso8601_or_epoch(x: Any) -> Optional[datetime]:
    if x is None:
        return None

    if isinstance(x, str):
        s = x.strip()
        with contextlib.suppress(Exception):
            s2 = s.replace("Z", "+00:00") if s.endswith("Z") else s
            dt = datetime.fromisoformat(s2)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)

    try:
        val = float(x)
        if val > 10**11:  # millis
            val /= 1000.0
        return datetime.fromtimestamp(val, tz=timezone.utc)
    except Exception:
        return None


def _iter_records(obj: Any) -> List[Dict[str, Any]]:
    """
    Accepts Garmin JSON variants:
      - top-level list of dicts
      - {"records"| "data"| "samples": [...]}
      - {"values": [...]}
      - {"body": { <any_key>: [ ... ] }}  <-- Garmin export in your files
    Returns a flat list of dict records.
    """
    if isinstance(obj, list):
        return [r for r in obj if isinstance(r, dict)]
    if not isinstance(obj, dict):
        return []

    # Common containers
    for key in ["records", "data", "samples"]:
        v = obj.get(key)
        if isinstance(v, list):
            return [r for r in v if isinstance(r, dict)]

    # Garmin body container: merge all list-of-dict arrays under body.*
    body = obj.get("body")
    if isinstance(body, dict):
        recs: List[Dict[str, Any]] = []
        for v in body.values():
            if isinstance(v, list):
                recs.extend([r for r in v if isinstance(r, dict)])
        if recs:
            return recs

    # Fallback: any list-of-dict at top-level
    for v in obj.values():
        if isinstance(v, list) and all(isinstance(r, dict) for r in v):
            return v

    # Metric container with "values"
    v = obj.get("values")
    if isinstance(v, list):
        return [r if isinstance(r, dict) else {"value": r} for r in v]

    return []


def _deep_find_first(d: Any, keys: List[str]) -> Optional[Any]:
    """Depth-first search for the first existing key among 'keys'."""
    if isinstance(d, dict):
        for k in keys:
            if k in d:
                return d[k]
        for v in d.values():
            got = _deep_find_first(v, keys)
            if got is not None:
                return got
    elif isinstance(d, list):
        for it in d:
            got = _deep_find_first(it, keys)
            if got is not None:
                return got
    return None


def _extract_time_fields(
    rec: Dict[str, Any],
) -> Tuple[Optional[datetime], Optional[datetime]]:
    """
    Extract start/end datetimes (UTC) from nested shapes like:
      effective_time_frame.time_interval.{start_date_time,end_date_time}
      sleep_stage_time_frame.time_interval.{start_date_time,end_date_time}
      effective_time_frame.date_time (instantaneous)
      ...and the usual Garmin epoch/ISO keys.
    """
    start_raw = _deep_find_first(
        rec,
        [
            "start_date_time",
            "startDateTime",
            "start_time",
            "startTime",
            "startTimeGmt",
            "startTimeLocal",
            "startTimeInSeconds",
            "startTimeInMilliSeconds",
        ],
    )
    end_raw = _deep_find_first(
        rec,
        [
            "end_date_time",
            "endDateTime",
            "end_time",
            "endTime",
            "endTimeGmt",
            "endTimeLocal",
            "endTimeInSeconds",
            "endTimeInMilliSeconds",
        ],
    )

    # Single timestamp (instantaneous)
    single_raw = _deep_find_first(rec, ["date_time", "dateTime", "timestamp", "time"])
    if start_raw is None and end_raw is None and single_raw is not None:
        dt = _parse_iso8601_or_epoch(single_raw)
        return dt, dt  # instantaneous window

    dt_s = _parse_iso8601_or_epoch(start_raw)
    dt_e = _parse_iso8601_or_epoch(end_raw)

    # Synthesize end if only duration present
    if dt_s is not None and dt_e is None:
        dur = _deep_find_first(
            rec, ["durationInSeconds", "duration", "activeDurationInSeconds"]
        )
        with contextlib.suppress(Exception):
            if dur is not None:
                dt_e = dt_s + timedelta(seconds=float(dur))

    return dt_s, dt_e


_MEASUREMENT_KEY_REGEX = re.compile(
    r"(value|values|avg|average|min|max|bpm|hr|rr|resp|spo2|saturation|stress|calorie|calories|steps|count|intensity|met|score)$",
    re.IGNORECASE,
)


def _iter_numeric_measurements(rec: Any) -> Iterable[float]:
    """
    Walk nested structures and yield measurement-like numbers.
    Ignores likely-benign negatives (timezone offsets, lat/lng).
    """

    def _walk(x: Any):
        if isinstance(x, dict):
            for k, v in x.items():
                kl = k.lower()
                if any(
                    s in kl
                    for s in [
                        "offset",
                        "timezone",
                        "tz",
                        "longitude",
                        "latitude",
                        "lng",
                        "lat",
                    ]
                ):
                    continue
                if isinstance(v, (int, float)):
                    if _MEASUREMENT_KEY_REGEX.search(k) or not any(
                        t in kl for t in ["time", "date"]
                    ):
                        yield float(v)
                else:
                    yield from _walk(v)
        elif isinstance(x, list):
            for it in x:
                yield from _walk(it)
        elif isinstance(x, (int, float)):
            yield float(x)

    yield from _walk(rec)


def _make_key(rec: Dict[str, Any]) -> Tuple[Any, ...] | str:
    """
    Stable key for duplicate detection: (name/stage, start ISO, end ISO, primary value).
    """
    try:
        dt_s, dt_e = _extract_time_fields(rec)
        # try to capture an identifying label
        name = None
        for k in ["metric", "type", "name", "activity_name", "sleep_stage_state"]:
            if k in rec:
                name = rec[k]
                break
        # try to capture a primary value-ish field
        val = _deep_find_first(
            rec,
            [
                "value",
                "values",
                "heart_rate",
                "oxygen_saturation",
                "calories_value",
                "base_movement_quantity",
            ],
        )
        if isinstance(val, dict):
            val = json.dumps(val, sort_keys=True, separators=(",", ":"))
        return (
            str(name),
            dt_s.isoformat() if dt_s else "None",
            dt_e.isoformat() if dt_e else "None",
            str(val),
        )
    except Exception:
        try:
            return json.dumps(rec, sort_keys=True, separators=(",", ":"))
        except Exception:
            return str(rec)


# ----------------------------
# Public API
# ----------------------------


def sanity_check_garmin_file(
    file_path: str, logger, max_log_examples: int = 50
) -> Dict[str, int]:
    """
    Checks:
      1) start_date > end_date
      2) negative values
      3) duplicate records (identical content by salient key)

    Returns:
      {
        "bad_date_cnt": int,
        "negative_value_cnt": int,
        "duplicate_groups": int,
        "total_duplicate_records": int,
        "total_records": int
      }
    """
    summary = {
        "bad_date_cnt": 0,
        "negative_value_cnt": 0,
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
        logger.warning(f"[SANITY CHECK] No records found in {file_path}")
        return summary

    # 1) start_date > end_date
    for idx, rec in enumerate(records):
        dt_s, dt_e = _extract_time_fields(rec)
        if dt_s is not None and dt_e is not None and dt_s > dt_e:
            summary["bad_date_cnt"] += 1
            if summary["bad_date_cnt"] <= max_log_examples:
                logger.error(
                    f"[SANITY CHECK] start > end at index {idx} in {file_path}: {dt_s.isoformat()} > {dt_e.isoformat()}"
                )

    if summary["bad_date_cnt"]:
        logger.error(
            f"[SANITY CHECK] Total records with start_date > end_date: {summary['bad_date_cnt']}"
        )

    # 2) negative values
    for idx, rec in enumerate(records):
        with contextlib.suppress(Exception):
            for v in _iter_numeric_measurements(rec):
                if v < 0:
                    summary["negative_value_cnt"] += 1
                    if summary["negative_value_cnt"] <= max_log_examples:
                        logger.error(
                            f"[SANITY CHECK] Negative value at index {idx} in {file_path}: {v} "
                            f"(record snippet: {json.dumps(rec, sort_keys=True)[:300]})"
                        )
                    break  # count each record once

    if summary["negative_value_cnt"]:
        logger.error(
            f"[SANITY CHECK] Total records with negative values: {summary['negative_value_cnt']}"
        )

    # 3) duplicates
    seen: Dict[Any, List[int]] = {}
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
        # Print example groups (indices) up to max_log_examples
        logged = 0
        for _, idxs in duplicate_groups:
            if logged >= max_log_examples:
                break
            logger.error(f"[SANITY CHECK] Duplicate group indices: {idxs}")
            logged += 1

    return summary
