"""Remove the topcon_maestro2/topcon_triton files listed in
topcon_maestro2_triton_paths_to_remove.txt from the stage-one container.

Each line in the paths file is a file path relative to root_folder below.
"""

import argparse

import azure.storage.filedatalake as azurelake  # type: ignore
import config

container_name = "stage-one"
root_folder = "AI-READI/sanjay/old-data-removed-dataset/"
paths_file = "topcon_maestro2_triton_paths_to_remove.txt"


def load_relative_paths(file_path):
    """Read non-empty, stripped lines from the paths file."""
    with open(file_path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


def pipeline(dry_run=True):
    """Remove each file in the paths file from Azure Data Lake Storage."""
    print("=" * 80)
    if dry_run:
        print("Starting topcon_maestro2/topcon_triton file removal (DRY RUN)")
    else:
        print("Starting topcon_maestro2/topcon_triton file removal")
    print("=" * 80)

    relative_paths = load_relative_paths(paths_file)
    print(f"Loaded {len(relative_paths)} file path(s) from {paths_file}")

    print("Connecting to Azure Data Lake Storage...")
    file_system_client = azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_PRODUCTION_DANGEROUS_CONNECTION_STRING,
        file_system_name=container_name,
    )
    print("Connected.")
    print("-" * 80)

    removed_count = 0
    error_count = 0

    for idx, relative_path in enumerate(relative_paths, 1):
        full_path = root_folder + relative_path

        if dry_run:
            print(f"[{idx}/{len(relative_paths)}] [DRY RUN] Would remove: {full_path}")
            continue

        try:
            file_system_client.delete_file(full_path)
            print(f"[{idx}/{len(relative_paths)}] Removed: {full_path}")
            removed_count += 1
        except Exception as exc:
            print(f"[{idx}/{len(relative_paths)}] Error removing {full_path}: {exc}")
            error_count += 1

    print("-" * 80)
    if dry_run:
        print(f"DRY RUN complete: Would remove {len(relative_paths)} file(s)")
    else:
        print(
            f"Removal complete: Removed {removed_count} file(s), {error_count} error(s)"
        )
    print("=" * 80)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Remove topcon_maestro2/topcon_triton files listed in "
        f"{paths_file} from Azure Data Lake Storage"
    )
    parser.add_argument(
        "--no-dry-run",
        action="store_true",
        help="Actually delete the files (default is dry run)",
    )
    args = parser.parse_args()

    pipeline(dry_run=not args.no_dry_run)
