"""
Remove .DS_Store files from Azure Storage

This script scans the Azure Data Lake Storage container "stage-final-container"
and identifies all .DS_Store files. These are macOS system files that are
automatically created by Finder and should be removed from cloud storage.

WARNING: This script operates on PRODUCTION storage and requires explicit
user confirmation before execution to prevent accidental data deletion.

@megasanjay for any questions
"""

import config
import azure.storage.filedatalake as azurelake  # type: ignore
import argparse
import time
from datetime import datetime


def parse_arguments():
    """
    Parse command line arguments for the script.

    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description="Remove .DS_Store files from Azure Storage",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python remove_ds_store.py                    # Dry run (default)
  python remove_ds_store.py --delete           # Actually delete files
  python remove_ds_store.py --delete --force   # Skip confirmation prompt
        """,
    )

    parser.add_argument(
        "--delete",
        action="store_true",
        help="Actually delete the .DS_Store files (default is dry run)",
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help="Skip confirmation prompt (use with caution)",
    )

    return parser.parse_args()


def confirm_execution(delete_mode=False, force=False):
    """
    Require explicit user confirmation before proceeding with file operations.

    Args:
        delete_mode (bool): Whether files will actually be deleted
        force (bool): Skip confirmation if True

    Returns:
        bool: True if user confirms, False otherwise
    """
    if force:
        print("Force mode enabled - skipping confirmation prompt")
        return True

    print("=" * 60)
    if delete_mode:
        print("WARNING: This script will DELETE .DS_Store files")
        print("in the PRODUCTION Azure Storage container 'stage-final-container'")
        print("THIS ACTION CANNOT BE UNDONE!")
    else:
        print("DRY RUN: This script will scan and identify .DS_Store files")
        print("in the PRODUCTION Azure Storage container 'stage-final-container'")
        print("No files will be deleted in dry run mode")
    print("=" * 60)
    print()

    # Require explicit "yes" confirmation
    confirmation = input("Type 'yes' to confirm you want to proceed: ").strip().lower()

    if confirmation == "yes":
        mode_text = "deletion" if delete_mode else "scan"
        print(f"Confirmation received. Proceeding with {mode_text}...")
        return True
    else:
        print("Operation cancelled. No files will be processed.")
        return False


def main():
    """
    Main function to scan and optionally delete .DS_Store files from Azure Storage.

    This function:
    1. Parses command line arguments
    2. Connects to the Azure Data Lake Storage using production credentials
    3. Recursively scans all paths in the container
    4. Identifies files ending with ".DS_Store"
    5. Optionally deletes the files (if --delete flag is used)
    6. Provides detailed timing and summary information
    """
    # Parse command line arguments
    args = parse_arguments()
    delete_mode = args.delete
    force_mode = args.force

    # Start timing
    start_time = time.time()
    start_datetime = datetime.now()

    print(f"Script started at: {start_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Mode: {'DELETE' if delete_mode else 'DRY RUN'}")
    print()

    # Require user confirmation before proceeding
    if not confirm_execution(delete_mode=delete_mode, force=force_mode):
        return

    print("Connecting to Azure Data Lake Storage...")
    connection_start = time.time()

    # Initialize connection to Azure Data Lake Storage
    # Using production connection string - handle with care
    datalake_service_client = azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_PRODUCTION_DANGEROUS_CONNECTION_STRING,
        file_system_name="stage-final-container",
    )

    connection_time = time.time() - connection_start
    print(f"Connected in {connection_time:.2f} seconds")
    print("Scanning all files in the container...")

    scan_start = time.time()

    # Get all file paths recursively from the root directory
    file_paths = datalake_service_client.get_paths(
        path="/",  # Start from root directory
        recursive=True,  # Include all subdirectories
    )

    ds_store_count = 0
    total_files_processed = 0
    ds_store_files = []  # Store all .DS_Store files found

    print("\nScanning files...")
    if delete_mode:
        print("Found .DS_Store files (will be deleted):")
    else:
        print("Found .DS_Store files:")
    print("-" * 40)

    # Iterate through all file paths and identify .DS_Store files
    for file_path in file_paths:
        total_files_processed += 1

        # Show progress every 1000 files or for the first few files
        if total_files_processed <= 10 or total_files_processed % 1000 == 0:
            print(f"\rProcessed {total_files_processed:,} files...", end="", flush=True)

        if file_path.name.endswith(".DS_Store"):
            # Move to new line to show the .DS_Store file found
            print(f"\n  {file_path.name}")
            ds_store_files.append(file_path.name)
            ds_store_count += 1

    # Clear the progress line and show final count
    print(f"\rProcessed {total_files_processed:,} files total.                    ")
    scan_time = time.time() - scan_start

    print("-" * 40)
    print(f"Total .DS_Store files found: {ds_store_count}")

    # Delete files if in delete mode
    if delete_mode and ds_store_count > 0:
        print(f"\nStarting deletion of {ds_store_count} .DS_Store files...")
        delete_start = time.time()
        deleted_count = 0

        for file_path in ds_store_files:
            try:
                # Delete the file
                datalake_service_client.delete_file(file_path)
                deleted_count += 1
                print(f"  Deleted: {file_path}")
            except Exception as e:
                print(f"  ERROR deleting {file_path}: {str(e)}")

        delete_time = time.time() - delete_start
        print(f"\nDeletion completed in {delete_time:.2f} seconds")
        print(f"Successfully deleted: {deleted_count}/{ds_store_count} files")

    # Show summary
    total_time = time.time() - start_time
    end_datetime = datetime.now()

    print("\n" + "=" * 60)
    print("EXECUTION SUMMARY")
    print("=" * 60)
    print(f"Start time: {start_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"End time: {end_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total execution time: {total_time:.2f} seconds")
    print(f"Connection time: {connection_time:.2f} seconds")
    print(f"Scan time: {scan_time:.2f} seconds")
    if delete_mode and ds_store_count > 0:
        print(f"Deletion time: {delete_time:.2f} seconds")
    print(f"Total files processed: {total_files_processed:,}")
    print(f".DS_Store files found: {ds_store_count}")

    if delete_mode:
        print(f".DS_Store files deleted: {deleted_count if ds_store_count > 0 else 0}")

    if ds_store_count > 0:
        print("\nAll .DS_Store files found:")
        for i, file_path in enumerate(ds_store_files, 1):
            print(f"  {i:3d}. {file_path}")
    else:
        print("\nNo .DS_Store files found in the container.")

    if not delete_mode and ds_store_count > 0:
        print("\nTo actually delete these files, run:")
        print("  python remove_ds_store.py --delete")


if __name__ == "__main__":
    main()
