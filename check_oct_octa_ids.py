"""Check which participant IDs are present/missing in year3 and year3+ OCT/OCTA folders."""

import csv
import azure.storage.filedatalake as azurelake
import config


def get_folder_ids(file_system_client, folder_path):
    """Return the set of immediate child folder names under folder_path."""
    paths = file_system_client.get_paths(path=folder_path, recursive=False)
    return {str(p.name).split("/")[-1] for p in paths}


def check_section(label, oct_folders, octa_folder, participant_ids, file_system_client):
    """Compute present/missing IDs for one cohort's OCT (union) and OCTA (single folder)."""
    print(f"\n{'=' * 60}")
    print(f"Section: {label}")
    print(f"{'=' * 60}")

    # OCT — union across all instrument folders
    oct_present = set()
    for folder in oct_folders:
        print(f"  Fetching OCT folder: {folder}")
        ids = get_folder_ids(file_system_client, folder)
        print(f"    -> {len(ids)} IDs found")
        oct_present |= ids

    oct_missing = sorted(
        participant_ids - oct_present, key=lambda x: int(x) if x.isdigit() else x
    )
    oct_present_sorted = sorted(
        oct_present & participant_ids, key=lambda x: int(x) if x.isdigit() else x
    )

    print(f"  OCT union: {len(oct_present)} unique IDs across all folders")
    print(f"  OCT present (in participant list): {len(oct_present_sorted)}")
    print(f"  OCT missing: {len(oct_missing)}")

    # OCTA — single folder
    print(f"  Fetching OCTA folder: {octa_folder}")
    octa_ids = get_folder_ids(file_system_client, octa_folder)
    print(f"    -> {len(octa_ids)} IDs found")

    octa_missing = sorted(
        participant_ids - octa_ids, key=lambda x: int(x) if x.isdigit() else x
    )
    octa_present_sorted = sorted(
        octa_ids & participant_ids, key=lambda x: int(x) if x.isdigit() else x
    )

    print(f"  OCTA present (in participant list): {len(octa_present_sorted)}")
    print(f"  OCTA missing: {len(octa_missing)}")

    return {
        "label": label,
        "total": len(participant_ids),
        "oct_present": oct_present_sorted,
        "oct_missing": oct_missing,
        "octa_present": octa_present_sorted,
        "octa_missing": octa_missing,
    }


def load_participant_ids(csv_path):
    """Load participant IDs (column 0, skip header) from a CSV file."""
    ids = set()
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            if row:
                ids.add(row[0].strip())
    return ids


def write_report(results, report_path):
    """Write presence/absence report to disk."""
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("OCT/OCTA ID Presence Report\n")
        f.write("=" * 80 + "\n\n")

        for r in results:
            f.write(f"{r['label']} (N={r['total']} participant IDs)\n")
            f.write("-" * 60 + "\n")

            f.write(f"OCT (union of topcon_maestro2, topcon_triton, zeiss_cirrus):\n")
            f.write(f"  Present: {len(r['oct_present'])}\n")
            f.write(f"  Missing: {len(r['oct_missing'])}\n")
            if r["oct_missing"]:
                f.write("    " + ", ".join(r["oct_missing"]) + "\n")
            else:
                f.write("    (none)\n")
            f.write("\n")

            f.write(f"OCTA (heidelberg_spectralis):\n")
            f.write(f"  Present: {len(r['octa_present'])}\n")
            f.write(f"  Missing: {len(r['octa_missing'])}\n")
            if r["octa_missing"]:
                f.write("    " + ", ".join(r["octa_missing"]) + "\n")
            else:
                f.write("    (none)\n")
            f.write("\n")

        f.write("=" * 80 + "\n")
        f.write("End of report\n")


def pipeline():
    """Main pipeline: check OCT/OCTA ID presence for year3 and year3+ cohorts."""
    print("=" * 80)
    print("Starting OCT/OCTA ID presence check")
    print("=" * 80)

    year3_csv = r"/Users/sanjay/Developer/fairhub-pipeline/AllParticipantIDs07-01-2023through05-01-2025.csv"
    year3plus_csv = r"/Users/sanjay/Developer/fairhub-pipeline/AllParticipantIDs_year_01-01-25-through-3_12-31-2025.csv"

    print(f"\nLoading year3 IDs from: {year3_csv}")
    year3_ids = load_participant_ids(year3_csv)
    print(f"  Loaded {len(year3_ids)} IDs")

    print(f"Loading year3+ IDs from: {year3plus_csv}")
    year3plus_ids = load_participant_ids(year3plus_csv)
    print(f"  Loaded {len(year3plus_ids)} IDs")

    print("\nConnecting to Azure Data Lake Storage...")
    file_system_client = azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_PRODUCTION_DANGEROUS_CONNECTION_STRING,
        file_system_name="stage-one",
    )
    print("Connected.")

    year3_oct_folders = [
        "AI-READI/year3_imaging/retinal_oct/structural_oct/topcon_maestro2/",
        "AI-READI/year3_imaging/retinal_oct/structural_oct/topcon_triton/",
        "AI-READI/year3_imaging/retinal_oct/structural_oct/zeiss_cirrus/",
    ]
    year3_octa_folder = "AI-READI/pooled-data/Spectralis-processed/retinal_octa/enface/heidelberg_spectralis/"

    year3plus_oct_folders = [
        "AI-READI/year3+/imaging-combined-clean/retinal_oct/structural_oct/topcon_maestro2/",
        "AI-READI/year3+/imaging-combined-clean/retinal_oct/structural_oct/topcon_triton/",
        "AI-READI/year3+/imaging-combined-clean/retinal_oct/structural_oct/zeiss_cirrus/",
    ]
    year3plus_octa_folder = (
        "AI-READI/year3+/spectralis-s/retinal_octa/enface/heidelberg_spectralis/"
    )

    results = []

    results.append(
        check_section(
            "YEAR 3",
            year3_oct_folders,
            year3_octa_folder,
            year3_ids,
            file_system_client,
        )
    )
    results.append(
        check_section(
            "YEAR 3+",
            year3plus_oct_folders,
            year3plus_octa_folder,
            year3plus_ids,
            file_system_client,
        )
    )

    report_path = "oct_octa_ids_report.txt"
    write_report(results, report_path)
    print(f"\nReport written to: {report_path}")
    print("=" * 80)
    print("Done.")
    print("=" * 80)


if __name__ == "__main__":
    pipeline()
