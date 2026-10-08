"""Remove the files listed in dataset_paths.txt from AI-READI/cloned-phases/
in the stage-one container.

Each line in the paths file is a file path relative to root_folder below.
Backslashes are normalized to forward slashes since Azure Data Lake paths
always use "/".

Default is a dry run: every path is checked against Azure and reported as
found or missing, but nothing is deleted. Pass --no-dry-run to delete.

python remove_cloned_phases_files.py                  # dry run
python remove_cloned_phases_files.py --root "AI-READI/cloned-phases/<sub>/"   # if the files sit one level deeper
python remove_cloned_phases_files.py --no-dry-run     # actually delete
"""

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

import azure.storage.filedatalake as azurelake  # type: ignore
import config

container_name = "stage-one"
root_folder = "AI-READI/cloned-phases"
paths_file = "dataset_paths.txt"
missing_report_file = "cloned_phases_missing_paths.txt"
path_filter = "mac_20x20_hs"
default_workers = 8


def load_relative_paths(file_path):
    """Read non-empty lines from the paths file, normalized to Azure-style paths."""
    with open(file_path, "r", encoding="utf-8") as f:
        return [
            line.strip().replace("\\", "/").lstrip("/") for line in f if line.strip()
        ]


def pipeline(dry_run=True, root=root_folder, workers=default_workers):
    """Match each file in the paths file against Azure and remove it if found."""
    root = root.replace("\\", "/").rstrip("/") + "/"

    print("=" * 80)
    if dry_run:
        print("Starting cloned-phases file removal (DRY RUN)")
    else:
        print("Starting cloned-phases file removal")
    print("=" * 80)

    relative_paths = load_relative_paths(paths_file)
    print(f"Loaded {len(relative_paths)} file path(s) from {paths_file}")

    loaded_count = len(relative_paths)
    relative_paths = [p for p in relative_paths if path_filter in p]
    print(
        f"Kept {len(relative_paths)} path(s) containing '{path_filter}', "
        f"skipped {loaded_count - len(relative_paths)}"
    )
    print(f"Root folder: {container_name}/{root}")

    print("Connecting to Azure Data Lake Storage...")
    file_system_client = azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_PRODUCTION_DANGEROUS_CONNECTION_STRING,
        file_system_name=container_name,
    )
    print("Connected.")
    print("-" * 80)

    full_paths = [root + relative_path for relative_path in relative_paths]
    total = len(full_paths)

    found_paths = []
    missing_paths = []

    def check_exists(full_path):
        try:
            return file_system_client.get_file_client(full_path).exists()
        except Exception as exc:
            print(f"Error checking {full_path}: {exc}")
            return False

    print(f"Matching paths against Azure with {workers} worker(s)...")
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(check_exists, p): p for p in full_paths}
        for idx, future in enumerate(as_completed(futures), 1):
            full_path = futures[future]
            if future.result():
                print(f"[{idx}/{total}] Found: {full_path}")
                found_paths.append(full_path)
            else:
                print(f"[{idx}/{total}] Not found: {full_path}")
                missing_paths.append(full_path)

    found_paths.sort()
    missing_paths.sort()

    for idx, full_path in enumerate(missing_paths, 1):
        print(f"[{idx}/{len(missing_paths)}] Not found: {full_path}")

    with open(missing_report_file, "w", encoding="utf-8") as f:
        f.writelines(p + "\n" for p in missing_paths)

    print("-" * 80)
    print(f"Matched {len(found_paths)}/{total} path(s), {len(missing_paths)} missing")
    print(f"Missing paths written to {missing_report_file}")
    print("-" * 80)

    if dry_run:
        for idx, full_path in enumerate(found_paths, 1):
            print(f"[{idx}/{len(found_paths)}] [DRY RUN] Would remove: {full_path}")
        print("-" * 80)
        print(f"DRY RUN complete: Would remove {len(found_paths)} file(s)")
        print("=" * 80)
        return

    removed_count = 0
    error_count = 0

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(file_system_client.delete_file, p): p for p in found_paths
        }
        for idx, future in enumerate(as_completed(futures), 1):
            full_path = futures[future]
            try:
                future.result()
                print(f"[{idx}/{len(found_paths)}] Removed: {full_path}")
                removed_count += 1
            except Exception as exc:
                print(f"[{idx}/{len(found_paths)}] Error removing {full_path}: {exc}")
                error_count += 1

    print("-" * 80)
    print(f"Removal complete: Removed {removed_count} file(s), {error_count} error(s)")
    print("=" * 80)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=f"Remove files listed in {paths_file} from "
        f"{container_name}/{root_folder} in Azure Data Lake Storage"
    )
    parser.add_argument(
        "--no-dry-run",
        action="store_true",
        help="Actually delete the files (default is dry run)",
    )
    parser.add_argument(
        "--root",
        default=root_folder,
        help=f"Folder the paths are relative to (default: {root_folder})",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=default_workers,
        help=f"Number of parallel Azure requests (default: {default_workers})",
    )
    args = parser.parse_args()

    pipeline(dry_run=not args.no_dry_run, root=args.root, workers=args.workers)
