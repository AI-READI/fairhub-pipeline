"""Find participants whose DICOM study dates don't agree with each other.

Inputs:
  - dicom_files_unique-54m171.csv (patient_id, study_date, file_count):
    one row per unique (participant, study date), across all modalities.
  - dicom_files-54m171.csv (file_path, patient_id, study_date, error):
    one row per DICOM file; the path gives the modality and device.
  - Final Participant List.csv (person_id, Visit Date)

Each participant's study dates are compared against two reference dates,
each in its own section of the output:
  - vs_most_common_dicom_date: the study date with the most files across all
    modalities (earliest date on a tie)
  - vs_visit_date: the visit date from the final participant list

Offset: for each participant, the largest gap in days between any of their
study dates and the reference date (the worst case). Each participant goes in
exactly one bucket based on that worst case, so buckets add up to the total.

Output: dicom_date_stats-54m171/
  - summary.json: participant list check, plus bucket counts and offset
    distribution overall and for every device (no participant lists)
  - <reference>/overall.json: bucket lists across all modalities
  - <reference>/<modality>/<submodality>_<device>.json: bucket lists for one device
  Bucket lists look like {bucket: [{patient_id: [{study_date: file_count}]}]}
"""

import csv
import json
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

UNIQUE_CSV = "dicom_files_unique-54m171.csv"
FILES_CSV = "dicom_files-54m171.csv"
PARTICIPANT_LIST_CSV = "Final Participant List.csv"
OUTPUT_DIR = "dicom_date_stats-54m171"

# (bucket name, lowest offset in days); each bucket runs up to the next one
BUCKETS = [
    ("exact_match", 0),
    ("off_by_1_to_2_days", 1),
    ("off_by_3_to_9_days", 3),
    ("off_by_10_to_19_days", 10),
    ("off_by_20_or_more_days", 20),
]


def days_between(a: str, b: str) -> int:
    return abs((datetime.strptime(a, "%Y%m%d") - datetime.strptime(b, "%Y%m%d")).days)


def bucket_for(offset: int) -> str:
    return next(name for name, low in reversed(BUCKETS) if offset >= low)


def pct(n: int, total: int) -> float:
    return round(100 * n / total, 2) if total else 0.0


def load_unique() -> dict[str, dict[str, int]]:
    """patient_id -> {study_date: file_count}"""
    patients: dict[str, dict[str, int]] = defaultdict(dict)
    with open(UNIQUE_CSV, encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            patients[row["patient_id"]][row["study_date"]] = int(row["file_count"])
    return patients


def load_visit_dates() -> dict[str, str]:
    """person_id -> visit date as YYYYMMDD"""
    with open(PARTICIPANT_LIST_CSV, encoding="utf-8-sig") as f:
        return {
            row["person_id"]
            .strip(): datetime.strptime(row["Visit Date"].strip(), "%m/%d/%Y")
            .strftime("%Y%m%d")
            for row in csv.DictReader(f)
        }


def load_by_device() -> dict[str, dict[str, dict[str, Counter]]]:
    """modality -> "<submodality>/<device>" -> patient_id -> Counter(study_date)"""
    groups = defaultdict(lambda: defaultdict(lambda: defaultdict(Counter)))
    with open(FILES_CSV, encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            # G:\dataset\<modality>\<submodality>\<device>\<patient>\<file>
            parts = row["file_path"].split("\\")
            modality, device = parts[2], f"{parts[3]}/{parts[4]}"
            groups[modality][device][row["patient_id"]][row["study_date"]] += 1
    return groups


def report(
    patients: dict[str, dict[str, int]], reference: dict[str, str]
) -> tuple[dict, dict]:
    """Stats and bucket lists for one group of participants.

    stats: bucket counts plus offset_distribution (worst-case offset in days ->
    number of participants with that offset).
    buckets: bucket name -> [{patient_id: [{study_date: file_count}]}]
    """
    offsets = {
        pid: max(days_between(sd, reference[pid]) for sd in dates)
        for pid, dates in patients.items()
    }
    total = len(patients)

    buckets = {name: [] for name, _ in BUCKETS}
    for pid in sorted(patients, key=int):
        dates = patients[pid]
        buckets[bucket_for(offsets[pid])].append(
            {pid: [{sd: dates[sd]} for sd in sorted(dates)]}
        )

    stats = {
        "summary": {
            "participants": total,
            **{
                name: f"{pct(len(members), total)}% ({len(members)})"
                for name, members in buckets.items()
            },
        },
        "offset_distribution": {
            str(days): n for days, n in sorted(Counter(offsets.values()).items())
        },
    }
    return stats, buckets


def write_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"Wrote {path}")


def write_buckets(path: Path, buckets: dict[str, list]) -> None:
    """Like write_json, but with each participant on one line to keep files small."""
    path.parent.mkdir(parents=True, exist_ok=True)
    parts = []
    for name, members in buckets.items():
        rows = ",\n".join(f"    {json.dumps(m)}" for m in members)
        parts.append(f'  "{name}": [\n{rows}\n  ]' if members else f'  "{name}": []')
    with open(path, "w") as f:
        f.write("{\n" + ",\n".join(parts) + "\n}\n")
    print(f"Wrote {path}")


def main():
    patients = load_unique()
    reference = {
        pid: min(dates, key=lambda sd: (-dates[sd], sd))
        for pid, dates in patients.items()
    }

    by_device = load_by_device()

    # Both inputs should describe the same files
    for pid, dates in patients.items():
        counted = Counter()
        for devices in by_device.values():
            for per_patient in devices.values():
                counted.update(per_patient.get(pid, {}))
        if dict(counted) != dates:
            print(
                f"Warning: {pid} file counts differ between {UNIQUE_CSV} and {FILES_CSV}"
            )

    visit_dates = load_visit_dates()
    not_on_list = sorted(set(patients) - set(visit_dates), key=int)
    if not_on_list:
        print(
            f"Warning: not on {PARTICIPANT_LIST_CSV}, left out of vs_visit_date: {not_on_list}"
        )

    out = Path(OUTPUT_DIR)
    summary = {
        "offset": "largest gap in days between any of the participant's study dates and the reference date",
        "offset_distribution": "worst-case offset in days -> number of participants with that offset",
        "participant_list": {
            "participants": len(visit_dates),
            "with_dicoms": len(set(visit_dates) & set(patients)),
            "without_dicoms": sorted(set(visit_dates) - set(patients), key=int),
            "dicoms_not_on_list": not_on_list,
        },
    }

    for name, ref in (
        ("vs_most_common_dicom_date", reference),
        ("vs_visit_date", visit_dates),
    ):

        def with_ref(group):
            return {pid: dates for pid, dates in group.items() if pid in ref}

        stats, buckets = report(with_ref(patients), ref)
        summary[name] = {"overall": stats, "by_modality": {}}
        write_buckets(out / name / "overall.json", buckets)

        for modality, devices in sorted(by_device.items()):
            summary[name]["by_modality"][modality] = {}
            for device, device_patients in sorted(devices.items()):
                stats, buckets = report(with_ref(device_patients), ref)
                summary[name]["by_modality"][modality][device] = stats
                # "<submodality>/<device>" -> <modality>/<submodality>_<device>.json
                write_buckets(
                    out / name / modality / f"{device.replace('/', '_')}.json", buckets
                )

    write_json(out / "summary.json", summary)


if __name__ == "__main__":
    main()
