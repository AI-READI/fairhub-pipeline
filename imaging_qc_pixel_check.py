"""pixel check for imaging data"""

import argparse
import azure.storage.filedatalake as azurelake
import config
from concurrent.futures import ThreadPoolExecutor, as_completed
import pydicom
import json
import tempfile
import os
import time
from threading import Lock


def check_file_pixel(file_path, file_system_client):
    """
    Helper to check one DICOM file. Downloads file and checks pixel data.
    Returns tuple (file_path, status, error_message).
    """
    try:
        # Download file to local temporary folder
        # This creates a temporary directory that will be cleaned up automatically
        with tempfile.TemporaryDirectory() as temp_dir:
            file_name = os.path.basename(file_path)
            temp_file_path = os.path.join(temp_dir, file_name)

            # Download the file from Azure Data Lake Storage
            with open(temp_file_path, "wb") as f:
                file_system_client.get_file_client(file_path).download_file().readinto(
                    f
                )

            # Read DICOM file and verify pixel data can be loaded
            # Accessing pixel_array forces the pixel data to be decoded and loaded
            dataset = pydicom.dcmread(temp_file_path)
            _ = dataset.pixel_array  # force pixel data load

            # Clean up: remove temp file (directory cleanup happens automatically)
            os.remove(temp_file_path)
            return (file_path, "valid", None)
    except Exception as e:
        # Return error information if file processing fails
        return (file_path, "error", f"Error reading file {file_path}: {e}")


def find_files_in_folder(source_folder, file_system_client):
    """
    Find all .dcm files in a source folder.
    Recursively searches the folder and returns list of file paths.
    """
    print(f"[{source_folder}] Starting to search for .dcm files...")
    file_paths = []
    found_paths = 0
    total_valid_files = 0

    # Get all paths recursively from the source folder
    print(f"[{source_folder}] Fetching paths from Azure Data Lake...")
    paths = file_system_client.get_paths(path=source_folder, recursive=True)

    # Filter for .dcm files and track progress
    for path in paths:
        file_name = path.name.split("/")[-1]

        # Only include .dcm files
        if file_name.endswith(".dcm"):
            file_paths.append(path.name)
            total_valid_files += 1
        found_paths += 1

        # Print progress every 1000 paths processed
        if found_paths % 1000 == 0:
            print(
                f"[{source_folder}] Processed {found_paths} paths, found {total_valid_files} .dcm files so far..."
            )

    # Final summary for this folder
    print(
        f"[{source_folder}] Complete! Processed {found_paths} total paths, found {total_valid_files} .dcm files"
    )
    return file_paths


def main(thread_count=4):
    """
    Main function to run the imaging QC pixel check pipeline.

    Process:
    1. Find all .dcm files in source folders (parallel)
    2. Check pixel data in each file (parallel with thread_count workers)
    3. Write results to JSON files
    """
    print("=" * 80)
    print("Starting imaging QC pixel check pipeline...")
    print("=" * 80)

    # Define source folders to search for DICOM files
    source_folders = [
        "retinal_oct",
        "retinal_octa",
        "retinal_flio",
        "retinal_photography",
    ]
    print(f"Source folders to process: {', '.join(source_folders)}")

    # Initialize Azure Data Lake Storage client
    print("\n[Step 1/3] Connecting to Azure Data Lake Storage...")
    file_system_client = azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_PRODUCTION_DANGEROUS_CONNECTION_STRING,
        file_system_name="stage-final-container",
    )
    print("Connected successfully!")

    # Phase 1: Find all .dcm files in parallel (one thread per folder)
    print("\n" + "=" * 80)
    print("[Step 2/3] Finding file paths with 4 threads (one per source folder)...")
    print("=" * 80)
    file_paths = []
    with ThreadPoolExecutor(max_workers=4) as executor:
        # Submit tasks to find files in each folder
        future_to_folder = {
            executor.submit(find_files_in_folder, folder, file_system_client): folder
            for folder in source_folders
        }

        # Process results as they complete
        for future in as_completed(future_to_folder):
            folder = future_to_folder[future]
            try:
                folder_paths = future.result()
                file_paths.extend(folder_paths)
                print(
                    f"✓ Completed finding files in '{folder}': {len(folder_paths)} .dcm files found"
                )
            except Exception as exc:
                print(f"✗ ERROR: Folder '{folder}' generated an exception: {exc}")

    print(
        f"\n[Step 2/3 Complete] Total files found across all folders: {len(file_paths)}"
    )
    print("=" * 80)

    if not file_paths:
        print("WARNING: No files found! Exiting.")
        return

    # Phase 2: Check pixel data in parallel using thread_count workers
    print("\n" + "=" * 80)
    print(f"[Step 3/3] Checking pixel data with {thread_count} threads...")
    print("=" * 80)
    qc_results = []
    errors = []

    # Progress tracking for overall file processing
    progress_lock = Lock()
    total_files_processed = 0
    start_time = time.time()

    def format_time(seconds):
        """Format seconds into human-readable time string."""
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        if hours > 0:
            return f"{hours}h {minutes}m {secs}s"
        elif minutes > 0:
            return f"{minutes}m {secs}s"
        else:
            return f"{secs}s"

    def process_file_batch(file_batch, thread_id):
        """
        Process a batch of files assigned to this thread.
        Files are distributed in round-robin fashion for better load balancing.
        """
        batch_results = []
        batch_errors = []

        for file_path in file_batch:
            # Process each file in the batch
            _, status, error_msg = check_file_pixel(file_path, file_system_client)

            if status == "valid":
                batch_results.append({"file_path": file_path, "status": "valid"})
            else:
                batch_errors.append({"file_path": file_path, "status": error_msg})

            # Update overall progress (thread-safe)
            with progress_lock:
                nonlocal total_files_processed
                total_files_processed += 1
                current_total = total_files_processed

                # Print progress every 10 files
                if current_total % 10 == 0 or current_total == len(file_paths):
                    elapsed_time = time.time() - start_time
                    progress_pct = (current_total / len(file_paths)) * 100

                    # Calculate ETA
                    if current_total > 0:
                        files_per_second = current_total / elapsed_time
                        remaining_files = len(file_paths) - current_total
                        eta_seconds = (
                            remaining_files / files_per_second
                            if files_per_second > 0
                            else 0
                        )
                        eta_str = format_time(eta_seconds)
                    else:
                        eta_str = "calculating..."

                    elapsed_str = format_time(elapsed_time)

                    print(
                        f"Progress: {current_total}/{len(file_paths)} ({progress_pct:.1f}%) | "
                        f"Time: {elapsed_str} | ETA: {eta_str}"
                    )

        return batch_results, batch_errors

    # Distribute files across threads in round-robin fashion
    # Thread 0 gets files 0, thread_count, 2*thread_count, ...
    # Thread 1 gets files 1, thread_count+1, 2*thread_count+1, ...
    # This ensures better load balancing than sequential chunks
    file_batches = [[] for _ in range(thread_count)]
    for i, file_path in enumerate(file_paths):
        file_batches[i % thread_count].append(file_path)

    print(
        f"Distributed {len(file_paths)} files across {thread_count} threads (round-robin)"
    )
    print(f"Batch sizes: {[len(batch) for batch in file_batches]}")

    # Process batches in parallel
    with ThreadPoolExecutor(max_workers=thread_count) as executor:
        # Submit all batches for processing
        future_to_batch = {
            executor.submit(process_file_batch, batch, i): i
            for i, batch in enumerate(file_batches)
        }

        # Collect results as batches complete
        for future in as_completed(future_to_batch):
            batch_idx = future_to_batch[future]
            try:
                batch_results, batch_errors = future.result()
                qc_results.extend(batch_results)
                errors.extend(batch_errors)

                print(
                    f"✓ [Thread {batch_idx + 1}] Completed "
                    f"({len(batch_results)} valid, {len(batch_errors)} errors)"
                )
            except Exception as exc:
                print(
                    f"✗ ERROR: [Thread {batch_idx + 1}] generated an exception: {exc}"
                )

    # Phase 3: Write results to files
    print("\n" + "=" * 80)
    print("Writing results to files...")
    print("=" * 80)

    # Write valid results
    print(f"Writing {len(qc_results)} valid file results to 'qc_results.json'...")
    with open("qc_results.json", "w") as f:
        json.dump(qc_results, f, indent=2)
    print("✓ qc_results.json written successfully")

    # Write error results
    print(f"Writing {len(errors)} error records to 'errors.json'...")
    with open("errors.json", "w") as f:
        json.dump(errors, f, indent=2)
    print("✓ errors.json written successfully")

    # Final summary
    total_elapsed_time = time.time() - start_time
    total_elapsed_str = format_time(total_elapsed_time)

    print("\n" + "=" * 80)
    print("PIPELINE COMPLETE")
    print("=" * 80)
    print(f"Total files processed: {len(file_paths)}")
    print(f"Valid files: {len(qc_results)}")
    print(f"Files with errors: {len(errors)}")
    if file_paths:
        success_rate = (len(qc_results) / len(file_paths)) * 100
        print(f"Success rate: {success_rate:.2f}%")
        files_per_second = (
            len(file_paths) / total_elapsed_time if total_elapsed_time > 0 else 0
        )
        print(f"Processing rate: {files_per_second:.2f} files/second")
    print(f"Total time taken: {total_elapsed_str}")
    print("=" * 80)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create a file manifest for folders")
    parser.add_argument(
        "--threads",
        type=int,
        default=4,
        help="Number of threads to use for checking pixel data (default: 4)",
    )
    args = parser.parse_args()
    main(thread_count=args.threads)
