"""Parse DICOMDIR.HTM files from Spectralis batch folders to collect patient IDs."""

import json
import azure.storage.filedatalake as azurelake
from bs4 import BeautifulSoup
import config

# Batch folder names (last path segment) to skip — add test/junk folders here
IGNORE_FOLDERS = [
    "UCSD_Spectralis_20250701-20250731-old2",
    "UCSD_Spectralis_20250901-20250930-old",
    "UCSD_Spectralis_20251101-20251130-old",
    "UCSD_Spectralis_2_participant_test",
]

BASE_PATHS = [
    "AI-READI/UAB/UAB_Spectralis",
    "AI-READI/UCSD/UCSD_Spectralis",
    "AI-READI/UW/UW_Spectralis",
]


def normalize_patient_id(raw_id: str) -> str:
    """Strip the 'AIREADI-' prefix so IDs match the plain-number format used elsewhere."""
    prefix_list = ["AIREADI-", "AIREAD-", "AIREAID-"]
    for prefix in prefix_list:
        if raw_id.startswith(prefix):
            return raw_id.removeprefix(prefix)
    return raw_id


def parse_dicomdir_htm(content: str) -> list[str]:
    """Return normalized patient IDs from a DICOMDIR.HTM file's patient tables."""
    soup = BeautifulSoup(content, "html.parser")
    patient_ids = []
    for table in soup.find_all("table", {"border": "1"}):
        rows = table.find_all("tr", recursive=False)
        if not rows:
            continue
        header = rows[0]
        if header.get("bgcolor") != "#9acd32":
            continue
        for data_row in rows[1:]:
            cells = data_row.find_all("td")
            if len(cells) >= 2:
                pid = normalize_patient_id(cells[1].get_text(strip=True))
                if pid:
                    patient_ids.append(pid)
    return patient_ids


def get_batch_folders(file_system_client, base_path: str) -> list[str]:
    """Return full paths of immediate child folders under base_path, excluding ignored ones."""
    paths = file_system_client.get_paths(path=base_path, recursive=False)
    result = []
    for p in paths:
        folder_name = str(p.name).split("/")[-1]
        if folder_name in IGNORE_FOLDERS:
            print(f"    Skipping ignored folder: {folder_name}")
            continue
        result.append(str(p.name))
    return result


def download_dicomdir(file_system_client, batch_folder_path: str) -> str | None:
    """Download and return the content of DICOMDIR.HTM for a batch folder, or None if missing."""
    htm_path = f"{batch_folder_path}/IHE_PDI/DICOMDIR.HTM"
    try:
        file_client = file_system_client.get_file_client(htm_path)
        content = file_client.download_file().readall().decode("windows-1252")
        return content
    except Exception as e:
        print(f"    WARNING: Could not download {htm_path}: {e}")
        return None


def process_site(file_system_client, base_path: str) -> dict:
    """Process all batch folders under a site base path and collect patient IDs."""
    print(f"\n{'=' * 60}")
    print(f"Site: {base_path}")
    print(f"{'=' * 60}")

    batch_results = []
    print(f"  Listing batch folders...")
    batch_folders = get_batch_folders(file_system_client, base_path)
    print(f"  Found {len(batch_folders)} batch folder(s)")

    for batch_path in sorted(batch_folders):
        folder_name = batch_path.split("/")[-1]
        print(f"  Processing batch: {folder_name}")
        content = download_dicomdir(file_system_client, batch_path)
        if content is None:
            batch_results.append({"folder": folder_name, "patient_ids": []})
            continue
        patient_ids = parse_dicomdir_htm(content)
        print(f"    -> {len(patient_ids)} patient ID(s) found")
        batch_results.append(
            {"folder": folder_name, "patient_ids": sorted(patient_ids)}
        )

    return {"base_path": base_path, "batches": batch_results}


def write_json(site_results: list[dict], json_path: str) -> None:
    """Write all normalized patient IDs to a JSON file for consumption by other scripts."""
    all_ids: set[str] = set()
    for site in site_results:
        for batch in site["batches"]:
            all_ids.update(batch["patient_ids"])
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump({"all_patient_ids": sorted(all_ids)}, f, indent=2)


def write_report(site_results: list[dict], report_path: str) -> None:
    """Write the patient ID report to disk."""
    all_ids: set[str] = set()
    for site in site_results:
        for batch in site["batches"]:
            all_ids.update(batch["patient_ids"])

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("Spectralis DICOMDIR Patient ID Report\n")
        f.write("=" * 80 + "\n\n")

        for site in site_results:
            f.write(f"Site: {site['base_path']}\n")
            f.write("-" * 60 + "\n")
            for batch in site["batches"]:
                ids = batch["patient_ids"]
                f.write(f"  Batch: {batch['folder']}\n")
                if ids:
                    f.write(f"    Patient IDs (N={len(ids)}): {', '.join(ids)}\n")
                else:
                    f.write(f"    Patient IDs (N=0): (none)\n")
            f.write("\n")

        f.write("=" * 80 + "\n")
        f.write(f"Overall unique patient IDs (N={len(all_ids)}):\n")
        f.write(f"  {', '.join(sorted(all_ids))}\n")
        f.write("=" * 80 + "\n")
        f.write("End of report\n")


def pipeline() -> None:
    """Main pipeline: download and parse DICOMDIR.HTM from all Spectralis batch folders."""
    print("=" * 80)
    print("Starting Spectralis DICOMDIR patient ID extraction")
    print("=" * 80)

    print("\nConnecting to Azure Data Lake Storage (raw-storage)...")
    file_system_client = azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_PRODUCTION_DANGEROUS_CONNECTION_STRING,
        file_system_name="raw-storage",
    )
    print("Connected.")

    site_results = []
    for base_path in BASE_PATHS:
        site_results.append(process_site(file_system_client, base_path))

    report_path = "spectralis_dicomdir_report.txt"
    write_report(site_results, report_path)
    print(f"\nReport written to: {report_path}")

    json_path = "spectralis_dicomdir.json"
    write_json(site_results, json_path)
    print(f"JSON written to:   {json_path}")
    print("=" * 80)
    print("Done.")
    print("=" * 80)


if __name__ == "__main__":
    pipeline()
