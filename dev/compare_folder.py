"""
Compare two Azure Data Lake Storage folders and show what files are in the first folder but not in the second folder.

This script compares two Azure Data Lake Storage directories and identifies files that exist in the first
folder but are missing from the second folder. It can be useful for:
- Finding files that haven't been synced between Azure containers
- Identifying missing files after a backup or migration
- Comparing old and new Azure storage locations

@megasanjay for any questions
"""

import config
import azure.storage.filedatalake as azurelake  # type: ignore
import time
from datetime import datetime
import os


def get_all_files_from_azure(service_client, container_name, folder_path="/"):
    """
    Get all files from an Azure Data Lake Storage container within a specific folder.
    Returns file paths relative to the specified folder_path.

    Args:
        service_client: Azure Data Lake Storage service client
        container_name (str): Name of the container
        folder_path (str): Path to the folder within the container (default: root "/")

    Returns:
        set: Set of relative file paths
    """
    print(f"Scanning Azure container: {container_name}, folder: {folder_path}")
    scan_start = time.time()

    files = set()
    total_files_processed = 0

    # Get all file paths recursively from the specified folder
    file_paths = service_client.get_paths(
        path=folder_path,  # Start from specified folder
        recursive=True,  # Include all subdirectories
    )

    print(f"Processing files in {container_name} {folder_path}...")

    # Normalize folder_path for comparison
    normalized_folder_path = folder_path.rstrip("/")
    if normalized_folder_path == "":
        normalized_folder_path = "/"

    # Iterate through all file paths
    for file_path in file_paths:
        file_name = os.path.basename(file_path.name)

        # if file_name has no extension, it is probably a folder
        if len(file_name.split(".")) == 1:
            continue

        total_files_processed += 1

        # Show progress every 1000 files or for the first few files
        if total_files_processed <= 10 or total_files_processed % 1000 == 0:
            print(f"\rProcessed {total_files_processed:,} files...", end="", flush=True)

        # Make file path relative to folder_path
        full_path = file_path.name.lstrip("/")

        # If folder_path is root, use the full path as is
        if normalized_folder_path == "/":
            relative_path = full_path
        else:
            # Remove the folder_path prefix to make it relative
            folder_prefix = normalized_folder_path.lstrip("/")
            if full_path.startswith(f"{folder_prefix}/"):
                relative_path = full_path[len(folder_prefix) + 1 :]
            elif full_path == folder_prefix:
                # This shouldn't happen for files, but handle it just in case
                continue
            else:
                # File is not within the specified folder, skip it
                continue

        files.add(relative_path)

    # Clear the progress line and show final count
    print(f"\rProcessed {total_files_processed:,} files total.                    ")
    scan_time = time.time() - scan_start
    print(
        f"Found {len(files)} files in {container_name} {folder_path} (scan time: {scan_time:.2f}s)"
    )

    return files


def main():
    """
    Main function to compare two Azure Data Lake Storage folders and show missing files.

    This function:
    1. Creates Azure service clients for old and new containers
    2. Scans both folders to get all files (relative to folder paths)
    3. Compares the folders to find missing files using relative paths
    4. Displays results showing relative file paths
    5. Provides detailed timing and summary information
    """
    # Start timing
    start_time = time.time()
    start_datetime = datetime.now()

    print(f"Script started at: {start_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
    print("Mode: Azure Data Lake Storage comparison")
    print()

    old_container_name = "stage-1-container"
    old_folder_path = "AI-READI/dataset/wearable_activity_monitor/"

    new_container_name = "stage-1-container"
    new_folder_path = "AI-READI/pooled-data/FitnessTracker-processed/"

    try:
        # Create Azure service clients
        print("Connecting to Azure Data Lake Storage...")
        connection_start = time.time()

        # Create service client for old container
        old_service_client = azurelake.FileSystemClient.from_connection_string(
            config.AZURE_STORAGE_CONNECTION_STRING,
            file_system_name=old_container_name,
        )

        # Create service client for new container
        new_service_client = azurelake.FileSystemClient.from_connection_string(
            config.AZURE_STORAGE_CONNECTION_STRING,
            file_system_name=new_container_name,
        )

        connection_time = time.time() - connection_start
        print(f"Connected in {connection_time:.2f} seconds")

        # Compare containers
        print("\nComparing Azure containers...")

        # Get all files from both containers
        old_files = get_all_files_from_azure(
            old_service_client, old_container_name, old_folder_path
        )
        new_files = get_all_files_from_azure(
            new_service_client, new_container_name, new_folder_path
        )

        # Find files in old container but not in new container
        missing_files = old_files - new_files

        # Display results
        print("\n" + "=" * 60)
        print("COMPARISON RESULTS")
        print("=" * 60)
        print(
            f"Files in old folder ({old_container_name} {old_folder_path}): {len(old_files)}"
        )
        print(
            f"Files in new folder ({new_container_name} {new_folder_path}): {len(new_files)}"
        )
        print(
            f"Files in old folder but not in new folder (relative comparison): {len(missing_files)}"
        )

        if missing_files:
            print(f"\nMissing files ({len(missing_files)}) - relative to folder paths:")
            print("-" * 80)
            print(f"{'#':<4} {'Relative File Path'}")
            print("-" * 80)

            # Sort files for consistent output
            sorted_missing = sorted(missing_files)

            # Show all files in table format
            for i, file_path in enumerate(sorted_missing, 1):
                print(f"{i:<4} {file_path}")

            print("-" * 80)
        else:
            print(
                f"\nNo missing files found! All files in {old_container_name}{old_folder_path} are also present in {new_container_name}{new_folder_path} (relative comparison)."
            )

        # Show summary
        total_time = time.time() - start_time
        end_datetime = datetime.now()

        print("\n" + "=" * 60)
        print("EXECUTION SUMMARY")
        print("=" * 60)
        print(f"Start time: {start_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"End time: {end_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Total execution time: {total_time:.2f} seconds")
        print(f"Old folder: {old_container_name} {old_folder_path}")
        print(f"New folder: {new_container_name} {new_folder_path}")
        print(f"Files in old folder: {len(old_files)}")
        print(f"Files in new folder: {len(new_files)}")
        print(f"Missing files (relative comparison): {len(missing_files)}")

    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
