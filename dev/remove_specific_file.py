"""Delete the files listed in a local CSV from a folder in the stage-one container.

Reads PATH_COLUMN from CSV_FILE (paths like retinal_oct\\structural_oct\\
topcon_triton\\<id>\\<file> or retinal_octa\\...), joins each one to
root and deletes that file in the container. Only files are
deleted: a path that points to a folder, or does not exist, is skipped
and reported.

Run with DRY_RUN = True first to preview everything.
"""

import csv
import posixpath

import azure.storage.filedatalake as azurelake  # type: ignore
from azure.core.exceptions import AzureError
import config

container_name = "stage-final-container"
root = "xxxx/"         # folder in the container the CSV paths start from
CSV_FILE = r"/home/xxx.csv"    # local CSV with the paths
PATH_COLUMN = "file_path"

DRY_RUN = True   # True = preview only, nothing is deleted


def read_paths():
    """Return the full container paths listed in the CSV, without duplicates."""
    mother = root.strip("/")
    paths = set()
    with open(CSV_FILE, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if PATH_COLUMN not in (reader.fieldnames or []):
            raise ValueError(f"Column {PATH_COLUMN!r} not in CSV: {reader.fieldnames}")
        for row in reader:
            value = (row[PATH_COLUMN] or "").strip()

            rel = value.replace("\\", "/").strip("/")
            paths.add(posixpath.join(mother, rel))
    return sorted(paths)


def delete_files(fsc, paths):
    """Delete each path that exists and is a file."""
    done = missing = folders = failed = 0

    for i, path in enumerate(paths, start=1):
        prefix = f"[{i}/{len(paths)}]"
        client = fsc.get_file_client(path)
        try:
            if not client.exists():
                missing += 1
                print(f"{prefix} [MISSING] {path}", flush=True)
                continue

            metadata = client.get_file_properties().metadata or {}
            if metadata.get("hdi_isfolder") == "true":
                folders += 1
                print(f"{prefix} [FOLDER]  {path} (not a file, skipped)", flush=True)
                continue

            if DRY_RUN:
                done += 1
                print(f"{prefix} [DRY]     would delete {path}", flush=True)
                continue

            client.delete_file()
            done += 1
            print(f"{prefix} [DELETED] {path}", flush=True)
        except AzureError as e:
            failed += 1
            print(f"{prefix} [FAIL]    {path}: {e}", flush=True)

    action = "would delete" if DRY_RUN else "deleted"
    print(f"\nDone. {action}: {done}, missing: {missing}, folders skipped: {folders}, failed: {failed}")


def main():
    print("Connecting to Azure Data Lake Storage...", flush=True)
    fsc = azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_PRODUCTION_DANGEROUS_CONNECTION_STRING,
        file_system_name=container_name,
    )
    print("Connected.\n")

    print(f"Container: {container_name}")
    print(f"Folder   : {root}")
    print(f"CSV      : {CSV_FILE}")
    print(f"DRY_RUN  = {DRY_RUN}\n")

    paths = read_paths()
    print(f"{len(paths)} file path(s) in CSV\n")

    if not DRY_RUN:
        answer = input(f"Delete these files from {container_name}/{root}? Type yes: ")
        if answer.strip().lower() != "yes":
            print("Cancelled, nothing was deleted.")
            return

    delete_files(fsc, paths)

    if DRY_RUN:
        print("DRY_RUN is on: nothing was deleted.")


if __name__ == "__main__":
    main()