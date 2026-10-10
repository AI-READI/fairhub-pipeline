"""One-off check that the linked retinal manifests and their files match.

retinal_oct, retinal_octa and retinal_photography reference each other's files
(e.g. retinal_oct's reference_filepath points into retinal_photography), so the
three folders are checked together. For each folder, lists every file under it
in the stage-one container and downloads the manifest.tsv that sits in it. The
files and manifest paths of all three are pooled, and the script reports:
- paths in the manifest path columns that are not in any of the folders
- files in the folders that no manifest path column references

Path columns are taken from the qc/qc_retinal_*.py scripts.
"""

import csv
import os
import tempfile

import azure.storage.filedatalake as azurelake  # type: ignore
import config

container_name = "stage-one"

# Folder the manifest paths are relative to. Manifest paths look like
# "/retinal_oct/structural_oct/...", so this is the parent of each folder.
manifest_path_root = "AI-READI/phase1+phase2+Year3/"
manifest_name = "manifest.tsv"

# Manifest columns that hold file paths relative to manifest_path_root, keyed
# by folder under manifest_path_root
folder_path_columns = {
    "retinal_oct": ["filepath", "reference_filepath"],
    "retinal_octa": [
        "flow_cube_file_path",
        "associated_retinal_photography_file_path",
        "associated_structural_oct_file_path",
        "associated_segmentation_file_path",
        "variant_segmentation_file_path",
        "associated_enface_1_file_path",
        "associated_enface_2_file_path",
        "associated_enface_2_projection_removed_filepath",
        "associated_enface_3_file_path",
        "associated_enface_3_projection_removed_filepath",
        "associated_enface_4_file_path",
        "associated_enface_4_projection_removed_filepath",
    ],
    "retinal_photography": ["filepath"],
}

# Folders to check together
folders = list(folder_path_columns)

output_file = "retinal_missing_paths.csv"
# Manifest values that mean "no file" and should not be checked. Compared
# case-insensitively, since the retinal manifests mix "Not Reported" and
# "Not reported".
skip_values = {"none", "not reported", "not provided"}
# Files under each folder (relative to it) that are not expected in a manifest
ignore_files = {manifest_name}


def to_full_path(manifest_value):
    """Turn a manifest path like "/retinal_oct/..." into its full container path.

    Accepts either separator, as the qc scripts do.
    """
    return manifest_path_root + manifest_value.strip().replace("\\", "/").lstrip("/")


def list_files(file_system_client, root_folder):
    """Return the full container path of every file under root_folder."""
    print(f"Fetching paths under {root_folder}...")
    paths = file_system_client.get_paths(path=root_folder, recursive=True)

    file_paths = set()
    for path in paths:
        if path.is_directory:
            continue

        file_paths.add(path.name)

        if len(file_paths) % 10000 == 0:
            print(f"Found {len(file_paths)} files so far...")

    print(f"Found {len(file_paths)} files total.")
    return file_paths


def read_manifest(file_system_client, root_folder, path_columns):
    """Download root_folder's manifest to a temp folder and return its rows."""
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
        raise ValueError(f"{manifest_path} is missing column(s): {absent_columns}")

    return rows


def pipeline():
    """Compare the folders' manifest path columns against their pooled file listing."""
    print("Connecting to Azure Data Lake Storage...")
    file_system_client = azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_PRODUCTION_DANGEROUS_CONNECTION_STRING,
        file_system_name=container_name,
    )
    print("Connected.")

    file_paths = set()
    manifests = {}
    for folder in folders:
        root_folder = f"{manifest_path_root}{folder}/"
        file_paths |= list_files(file_system_client, root_folder)
        manifests[folder] = read_manifest(
            file_system_client, root_folder, folder_path_columns[folder]
        )

    print(f"Found {len(file_paths)} files across {folders}")

    issues = []
    referenced = set()
    checked = 0
    for folder, rows in manifests.items():
        for row_number, row in enumerate(rows, start=2):
            for column in folder_path_columns[folder]:
                # A short row gives None here; treat it like an empty cell
                value = row[column] or ""
                if value.strip().lower() in skip_values:
                    continue

                checked += 1
                if not value.strip():
                    issues.append(
                        ("empty_in_manifest", folder, row_number, column, value)
                    )
                    continue

                full_path = to_full_path(value)
                referenced.add(full_path)
                if full_path not in file_paths:
                    issues.append(
                        ("missing_from_folder", folder, row_number, column, value)
                    )

    missing_from_folder = len(issues)
    print(f"Checked {checked} manifest path(s)")
    print(f"{missing_from_folder} manifest path(s) empty or not found in {folders}")

    ignored = {
        f"{manifest_path_root}{folder}/{name}"
        for folder in folders
        for name in ignore_files
    }
    for full_path in sorted(file_paths - referenced - ignored):
        manifest_value = "/" + full_path[len(manifest_path_root) :]
        folder = manifest_value.split("/")[1]
        issues.append(("missing_from_manifest", folder, "", "", manifest_value))

    print(f"{len(issues) - missing_from_folder} file(s) not referenced in any manifest")

    with open(output_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["issue", "folder", "row", "column", "path"])
        writer.writerows(issues)

    print(f"Wrote {len(issues)} issue(s) to {output_file}")


if __name__ == "__main__":
    pipeline()
