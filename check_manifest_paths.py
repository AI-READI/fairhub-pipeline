"""Check that a folder's manifest and its files match in both directions.

Lists every file under root_folder in the stage-one container, downloads the
manifest.tsv that sits in the same folder, and reports:
- paths in the configured manifest columns that are not in the folder
- files in the folder that no manifest path column references
"""

import csv
import os
import tempfile

import azure.storage.filedatalake as azurelake  # type: ignore
import config

container_name = "stage-one"

# root_folder = "AI-READI/phase1+phase2+Year3/cardiac_ecg/"
# root_folder = "AI-READI/phase1+phase2+Year3/environment/"
# root_folder = "AI-READI/phase1+phase2+Year3/wearable_activity_monitor/"
# root_folder = "AI-READI/phase1+phase2+Year3/wearable_blood_glucose/"
root_folder = "AI-READI/phase1+phase2+Year3/retinal_flio/"

# Folder the manifest paths are relative to. Manifest paths look like
# "/cardiac_ecg/ecg_12lead/...", so this is the parent of root_folder.
manifest_path_root = "AI-READI/phase1+phase2+Year3/"
# Last part of root_folder, e.g. "cardiac_ecg"
folder_name = root_folder.rstrip("/").split("/")[-1]
manifest_name = "manifest.tsv"
# Manifest columns that hold file paths relative to manifest_path_root

# cardiac_ecg
# path_columns = ["wfdb_hea_filepath", "wfdb_dat_filepath"]

# environment
# path_columns = ["env_sensor_filepath"]

# wearable_activity_monitor
# path_columns = [
#     "heartrate_filepath",
#     "oxygen_saturation_filepath",
#     "stress_level_filepath",
#     "sleep_filepath",
#     "respiratory_rate_filepath",
#     "physical_activity_filepath",
#     "active_calories_filepath",
# ]

# wearable_blood_glucose
# path_columns = ["glucose_filepath"]

# retinal_flio
path_columns = ["filepath"]

output_file = f"{folder_name}_missing_paths.csv"
# Manifest values that mean "no file" and should not be checked
skip_values = {"None", "Not reported"}
# Files under root_folder (relative to it) that are not expected in the manifest
ignore_files = {manifest_name}


def to_full_path(manifest_value):
    """Turn a manifest path like "/cardiac_ecg/..." into its full container path."""
    return manifest_path_root + manifest_value.strip().lstrip("/")


def pipeline():
    """Compare the manifest's file path columns against the folder listing."""
    print("Connecting to Azure Data Lake Storage...")
    file_system_client = azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_PRODUCTION_DANGEROUS_CONNECTION_STRING,
        file_system_name=container_name,
    )
    print("Connected.")

    print(f"Fetching paths under {root_folder}...")
    paths = file_system_client.get_paths(path=root_folder, recursive=True)

    file_paths = set()
    for path in paths:
        if path.is_directory:
            continue

        file_paths.add(path.name)

        if len(file_paths) % 100 == 0:
            print(f"Found {len(file_paths)} files so far...")

    print(f"Found {len(file_paths)} files total.")

    manifest_path = root_folder + manifest_name
    print(f"Downloading {manifest_path}...")
    with tempfile.TemporaryDirectory() as temp_dir:
        local_manifest = os.path.join(temp_dir, manifest_name)
        file_client = file_system_client.get_file_client(manifest_path)
        with open(local_manifest, "wb") as f:
            f.write(file_client.download_file().readall())

        with open(local_manifest, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f, delimiter="\t")
            fieldnames = reader.fieldnames or []
            rows = list(reader)

    print(f"Read {len(rows)} row(s) from manifest")

    absent_columns = [column for column in path_columns if column not in fieldnames]
    if absent_columns:
        raise ValueError(f"Manifest is missing column(s): {absent_columns}")

    issues = []
    referenced = set()
    checked = 0
    for row_number, row in enumerate(rows, start=2):
        for column in path_columns:
            # A short row gives None here; treat it like an empty cell
            value = row[column] or ""
            if value.strip() in skip_values:
                continue

            checked += 1
            if not value.strip():
                issues.append(("empty_in_manifest", row_number, column, value))
                continue

            full_path = to_full_path(value)
            referenced.add(full_path)
            if full_path not in file_paths:
                issues.append(("missing_from_folder", row_number, column, value))

    missing_from_folder = len(issues)
    print(f"Checked {checked} path(s) across columns {path_columns}")
    print(f"{missing_from_folder} manifest path(s) empty or not found in {root_folder}")

    ignored = {root_folder + name for name in ignore_files}
    for full_path in sorted(file_paths - referenced - ignored):
        manifest_value = "/" + full_path[len(manifest_path_root) :]
        issues.append(("missing_from_manifest", "", "", manifest_value))

    print(f"{len(issues) - missing_from_folder} file(s) not referenced in the manifest")

    with open(output_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["issue", "row", "column", "path"])
        writer.writerows(issues)

    print(f"Wrote {len(issues)} issue(s) to {output_file}")


if __name__ == "__main__":
    pipeline()
