"""pixel check for imaging data (local processing)"""

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import pydicom
import json
import os
import time
from threading import Lock
from pathlib import Path


def check_file_pixel(file_path):
    """
    Helper to check one DICOM file. Reads file and checks pixel data.
    Returns tuple (file_path, status, error_message).
    """
    try:
        # Read DICOM file and verify pixel data can be loaded
        # Accessing pixel_array forces the pixel data to be decoded and loaded
        dataset = pydicom.dcmread(file_path)
        _ = dataset.pixel_array  # force pixel data load
        return (file_path, "valid", None)
    except Exception as e:
        # Return error information if file processing fails
        return (file_path, "error", f"Error reading file {file_path}: {e}")


def find_files_in_folder(source_folder):
    """
    Find all .dcm files in a source folder.
    Recursively searches the folder and returns list of file paths.
    """
    print(f"[{source_folder}] Starting to search for .dcm files...")
    file_paths = []
    found_paths = 0
    total_valid_files = 0

    # Check if folder exists
    if not os.path.exists(source_folder):
        print(f"[{source_folder}] WARNING: Folder does not exist, skipping...")
        return file_paths

    # Recursively search for .dcm files using pathlib
    source_path = Path(source_folder)
    for path in source_path.rglob("*.dcm"):
        file_paths.append(str(path))
        total_valid_files += 1
        found_paths += 1

        # Print progress every 1000 paths processed
        if found_paths % 1000 == 0:
            print(f"[{source_folder}] Found {total_valid_files} .dcm files so far...")

    # Final summary for this folder
    print(f"[{source_folder}] Complete! Found {total_valid_files} .dcm files")
    return file_paths


def main(thread_count=4):
    """
    Main function to run the imaging QC pixel check pipeline.

    Process:
    1. Find all .dcm files in source folders (sequential)
    2. Check pixel data in each file (parallel with thread_count workers)
    3. Write results to JSON files
    """
    print("=" * 80)
    print("Starting imaging QC pixel check pipeline (local processing)...")
    print("=" * 80)

    # Define source folders to search for DICOM files
    # Update these paths to match your local folder structure
    source_folders = [
        r"D:\retinal_oct",
        r"D:\retinal_octa",
        r"D:\retinal_flio",
        r"D:\retinal_photography",
    ]
    print(f"Source folders to process: {', '.join(source_folders)}")

    # Phase 1: Find all .dcm files sequentially (no threading)
    print("\n" + "=" * 80)
    print("[Step 1/3] Finding file paths (sequential search)...")
    print("=" * 80)
    file_paths = []

    for folder in source_folders:
        try:
            folder_paths = find_files_in_folder(folder)
            file_paths.extend(folder_paths)
            print(
                f"✓ Completed finding files in '{folder}': {len(folder_paths)} .dcm files found"
            )
        except Exception as exc:
            print(f"✗ ERROR: Folder '{folder}' generated an exception: {exc}")

    print(
        f"\n[Step 1/3 Complete] Total files found across all folders: {len(file_paths)}"
    )
    print("=" * 80)

    if not file_paths:
        print("WARNING: No files found! Exiting.")
        return

    # Phase 2: Check pixel data in parallel using thread_count workers
    print("\n" + "=" * 80)
    print(f"[Step 2/3] Checking pixel data with {thread_count} threads...")
    print("=" * 80)
    qc_results = []
    errors = []

    def chunk_list(lst, n):
        """
        Split list into n approximately equal chunks.
        Used to distribute files across worker threads.
        """
        chunk_size = len(lst) // n
        if chunk_size == 0:
            chunk_size = 1
        for i in range(0, len(lst), chunk_size):
            yield lst[i : i + chunk_size]

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

    def process_file_batch(file_batch, batch_id):
        """
        Process a batch of files and return results.
        Reads each file, checks pixel data, and categorizes results.

        Args:
            file_batch: List of local file paths to process
            batch_id: Identifier for this batch/thread
        """
        batch_results = []
        batch_errors = []
        batch_size = len(file_batch)

        for idx, file_path in enumerate(file_batch):
            # Process each file in the batch
            _, status, error_msg = check_file_pixel(file_path)

            if status == "valid":
                batch_results.append({"file_path": file_path, "status": "valid"})
            else:
                batch_errors.append({"file_path": file_path, "status": error_msg})

            # Update overall progress (thread-safe)
            with progress_lock:
                nonlocal total_files_processed
                total_files_processed += 1
                current_total = total_files_processed

            # Print progress within batch every 10 files
            if (idx + 1) % 10 == 0 or (idx + 1) == batch_size:
                # Calculate overall progress metrics
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
                    f"[Thread {batch_id + 1}] Processed {idx + 1}/{batch_size} files in batch "
                    f"(valid: {len(batch_results)}, errors: {len(batch_errors)}) | "
                    f"Overall: {current_total}/{len(file_paths)} ({progress_pct:.1f}%) | "
                    f"Time: {elapsed_str} | ETA: {eta_str}"
                )

        return batch_results, batch_errors

    # Split files into batches for parallel processing
    file_batches = list(chunk_list(file_paths, thread_count))
    print(
        f"Split {len(file_paths)} files into {len(file_batches)} batches for parallel processing"
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
        completed = 0
        total_processed = 0
        for future in as_completed(future_to_batch):
            batch_idx = future_to_batch[future]
            try:
                batch_results, batch_errors = future.result()
                qc_results.extend(batch_results)
                errors.extend(batch_errors)
                completed += 1
                total_processed += len(batch_results) + len(batch_errors)

                elapsed_time = time.time() - start_time
                elapsed_str = format_time(elapsed_time)
                progress_pct = (total_processed / len(file_paths)) * 100

                print(
                    f"✓ [Thread {batch_idx + 1}] Completed batch {batch_idx + 1}/{len(file_batches)} "
                    f"({len(batch_results)} valid, {len(batch_errors)} errors) - "
                    f"Total processed: {total_processed}/{len(file_paths)} files ({progress_pct:.1f}%) | "
                    f"Elapsed time: {elapsed_str}"
                )
            except Exception as exc:
                print(
                    f"✗ ERROR: [Thread {batch_idx + 1}] Batch {batch_idx} generated an exception: {exc}"
                )

    # Phase 3: Write results to files
    print("\n" + "=" * 80)
    print("[Step 3/3] Writing results to files...")
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
