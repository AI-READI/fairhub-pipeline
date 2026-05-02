"""Compare Spectralis DICOMDIR IDs against OCTA IDs for year3 and year3+ and report discrepancies.

Run check_oct_octa_ids.py and check_spectralis_dicomdir.py first to generate:
  - oct_octa_ids.json
  - spectralis_dicomdir.json
"""

import csv
import json

OCT_OCTA_JSON = "oct_octa_ids.json"
DICOMDIR_JSON = "spectralis_dicomdir.json"
REPORT_PATH = "id_discrepancy_report.txt"

YEAR3_CSV = r"/Users/sanjay/Developer/fairhub-pipeline/AllParticipantIDs07-01-2023through05-01-2025.csv"
YEAR3PLUS_CSV = r"/Users/sanjay/Developer/fairhub-pipeline/AllParticipantIDs_year_01-01-25-through-3_12-31-2025.csv"


def load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_participant_ids(csv_path: str) -> set[str]:
    ids = set()
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            if row:
                ids.add(row[0].strip())
    return ids


def _sort_key(x: str) -> tuple:
    return (0, int(x)) if x.isdigit() else (1, x)


def site_split(ids: list[str]) -> dict[str, list[str]]:
    buckets: dict[str, list[str]] = {"UW": [], "UCSD": [], "UAB": [], "Other": []}
    for i in ids:
        if i and i[0] in ("1", "2"):
            buckets["UW"].append(i)
        elif i and i[0] in ("4", "5"):
            buckets["UCSD"].append(i)
        elif i and i[0] in ("7", "8"):
            buckets["UAB"].append(i)
        else:
            buckets["Other"].append(i)
    return buckets


def write_site_counts(f, ids: list[str], indent: str = "    ") -> None:
    sp = site_split(ids)
    parts = [f"UW={len(sp['UW'])}", f"UCSD={len(sp['UCSD'])}", f"UAB={len(sp['UAB'])}"]
    if sp["Other"]:
        parts.append(f"Other={len(sp['Other'])}")
    f.write(f"{indent}[{', '.join(parts)}]\n")


def diff_section(
    label: str,
    set_a: set,
    name_a: str,
    set_b: set,
    name_b: str,
    participants: set,
) -> dict:
    """Return counts and ID lists for both directions of a set difference, plus missing from both."""
    only_in_a = sorted(set_a - set_b, key=_sort_key)
    only_in_b = sorted(set_b - set_a, key=_sort_key)
    in_both = sorted(set_a & set_b, key=_sort_key)
    missing_from_both = sorted(participants - (set_a | set_b), key=_sort_key)
    return {
        "label": label,
        "name_a": name_a,
        "name_b": name_b,
        "only_in_a": only_in_a,
        "only_in_b": only_in_b,
        "in_both": in_both,
        "missing_from_both": missing_from_both,
    }


def write_diff(f, d: dict) -> None:
    f.write(f"\n{d['label']}\n")
    f.write("-" * 60 + "\n")
    f.write(
        f"  In both ({len(d['in_both'])}): "
        f"raw DICOMDIR entry exists AND processed OCTA folder exists\n"
    )
    write_site_counts(f, d["in_both"])
    f.write(
        f"  Only in {d['name_a']} ({len(d['only_in_a'])}): "
        f"raw data in DICOMDIR but does not exist in stage-one OCTA\n"
    )
    write_site_counts(f, d["only_in_a"])
    if d["only_in_a"]:
        f.write("    " + ", ".join(d["only_in_a"]) + "\n")
    f.write(
        f"  Only in {d['name_b']} ({len(d['only_in_b'])}): "
        f"processed OCTA exists in stage-one but no matching DICOMDIR entry in raw-storage\n"
    )
    write_site_counts(f, d["only_in_b"])
    if d["only_in_b"]:
        f.write("    " + ", ".join(d["only_in_b"]) + "\n")
    f.write(
        f"  Missing from both ({len(d['missing_from_both'])}): "
        f"participant has no DICOMDIR entry and no processed OCTA — completely absent\n"
    )
    write_site_counts(f, d["missing_from_both"])
    if d["missing_from_both"]:
        f.write("    " + ", ".join(d["missing_from_both"]) + "\n")


def write_oct_only_section(
    f,
    label: str,
    oct_only: list[str],
    in_dicomdir: list[str],
    not_in_dicomdir: list[str],
) -> None:
    f.write(f"\n{label}\n")
    f.write("-" * 60 + "\n")
    f.write(
        f"  OCT present but OCTA absent ({len(oct_only)}): "
        f"participant has OCT data in stage-one but no matching OCTA folder\n"
    )
    write_site_counts(f, oct_only)
    f.write(
        f"    In DICOMDIR ({len(in_dicomdir)}): raw data exists in DICOMDIR\n"
    )
    write_site_counts(f, in_dicomdir, indent="      ")
    if in_dicomdir:
        f.write("      " + ", ".join(in_dicomdir) + "\n")
    f.write(
        f"    Not in DICOMDIR ({len(not_in_dicomdir)}): no raw DICOMDIR entry found\n"
    )
    write_site_counts(f, not_in_dicomdir, indent="      ")
    if not_in_dicomdir:
        f.write("      " + ", ".join(not_in_dicomdir) + "\n")


def pipeline() -> None:
    print("=" * 80)
    print("Loading JSON outputs...")

    oct_octa = load_json(OCT_OCTA_JSON)
    dicomdir = load_json(DICOMDIR_JSON)

    year3_participants = load_participant_ids(YEAR3_CSV)
    year3plus_participants = load_participant_ids(YEAR3PLUS_CSV)

    all_dicomdir_ids = set(dicomdir["all_patient_ids"])
    dicomdir_year3 = all_dicomdir_ids & year3_participants
    dicomdir_year3plus = all_dicomdir_ids & year3plus_participants

    year3_oct = set(oct_octa["YEAR 3"]["oct_present"])
    year3_octa = set(oct_octa["YEAR 3"]["octa_present"])
    year3plus_oct = set(oct_octa["YEAR 3+"]["oct_present"])
    year3plus_octa = set(oct_octa["YEAR 3+"]["octa_present"])

    def _oct_only_lists(oct: set, octa: set, dicomdir_ids: set) -> tuple[list, list, list]:
        oct_only = sorted(oct - octa, key=_sort_key)
        in_dcm = sorted((oct - octa) & dicomdir_ids, key=_sort_key)
        not_in_dcm = sorted((oct - octa) - dicomdir_ids, key=_sort_key)
        return oct_only, in_dcm, not_in_dcm

    y3_oct_only, y3_in_dcm, y3_not_in_dcm = _oct_only_lists(year3_oct, year3_octa, dicomdir_year3)
    y3p_oct_only, y3p_in_dcm, y3p_not_in_dcm = _oct_only_lists(year3plus_oct, year3plus_octa, dicomdir_year3plus)

    comparisons = [
        diff_section(
            "DICOMDIR (raw-storage, year3 participants) vs Year 3 OCTA (stage-one)",
            dicomdir_year3,
            "DICOMDIR year3",
            year3_octa,
            "Year3 OCTA",
            year3_participants,
        ),
        diff_section(
            "DICOMDIR (raw-storage, year3+ participants) vs Year 3+ OCTA (stage-one)",
            dicomdir_year3plus,
            "DICOMDIR year3+",
            year3plus_octa,
            "Year3+ OCTA",
            year3plus_participants,
        ),
    ]

    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write("ID Discrepancy Report\n")
        f.write("=" * 80 + "\n")
        f.write(f"Sources:\n")
        f.write(f"  OCT/OCTA data: {OCT_OCTA_JSON}\n")
        f.write(f"  DICOMDIR data: {DICOMDIR_JSON}\n")
        f.write(f"  Year 3 participants CSV:  {YEAR3_CSV}\n")
        f.write(f"  Year 3+ participants CSV: {YEAR3PLUS_CSV}\n")
        f.write(f"\nDICOMDIR total unique IDs (all sites):  {len(all_dicomdir_ids)}\n")
        f.write(f"DICOMDIR IDs matching year3 participants:  {len(dicomdir_year3)}\n")
        f.write(
            f"DICOMDIR IDs matching year3+ participants: {len(dicomdir_year3plus)}\n"
        )
        f.write(f"Year 3  OCT present:       {len(year3_oct)}\n")
        f.write(f"Year 3  OCTA present:      {len(year3_octa)}\n")
        f.write(f"Year 3+ OCT present:       {len(year3plus_oct)}\n")
        f.write(f"Year 3+ OCTA present:      {len(year3plus_octa)}\n")

        for comp in comparisons:
            write_diff(f, comp)

        write_oct_only_section(
            f,
            "Year 3 — OCT present but OCTA absent (cross-referenced with DICOMDIR)",
            y3_oct_only,
            y3_in_dcm,
            y3_not_in_dcm,
        )
        write_oct_only_section(
            f,
            "Year 3+ — OCT present but OCTA absent (cross-referenced with DICOMDIR)",
            y3p_oct_only,
            y3p_in_dcm,
            y3p_not_in_dcm,
        )

        f.write("\n" + "=" * 80 + "\n")
        f.write("End of report\n")

    print(f"Report written to: {REPORT_PATH}")
    print("=" * 80)
    print("Done.")


if __name__ == "__main__":
    pipeline()
