"""Create a file manifest for the folder"""

# import tempfile
import argparse
import azure.storage.filedatalake as azurelake
import config
import utils.logwatch as logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# from tqdm import tqdm

# from utils.time_estimator import TimeEstimator

# import hashlib


def process_source_folder(
    source_folder,
    idx,
    total_folders,
    file_system_client,
    file_paths,
    file_paths_lock,
    FAST_FOLDER_CHECK,
):
    """Process a single source folder and add discovered files to file_paths"""
    print(f"\n[{idx}/{total_folders}] Processing source folder: {source_folder}")

    # Get the list of folders in the source folder
    # This should be a list of participants (each participant has their own folder)
    folder_paths = file_system_client.get_paths(path=source_folder, recursive=False)

    # Process each participant folder
    files_found_in_folder = 0
    local_file_paths = []

    for folder_path in folder_paths:
        # print(f"Processing folder: {folder_path.name}")

        # Get all files recursively within this participant's folder
        participant_files = file_system_client.get_paths(
            path=folder_path.name, recursive=True
        )

        # Process each file in the participant's folder
        for file_path in participant_files:
            fp = str(file_path.name)
            fn = fp.split("/")[-1]

            # ignore .DS_Store files
            if fn == ".DS_Store":
                print(f"Skipping .DS_Store file: {fp}")
                continue

            print(f"Found file: {fp}")

            # Fast folder check: skip files without proper extensions
            # This helps avoid processing directory entries as files
            if FAST_FOLDER_CHECK:
                length = len(fn.split("."))
                extension = fn.split(".")[-1]

                # Skip if file has no extension (likely a folder)
                if length == 1 or extension is None:
                    continue

            # Get file properties to verify it's actually a file, not a folder
            file_client = file_system_client.get_file_client(file_path=fp)
            file_properties = file_client.get_file_properties()
            file_metadata = file_properties.metadata

            # Double-check: skip if metadata indicates this is a folder
            if file_metadata.get("hdi_isfolder"):
                print(f"  Skipping {fp} - Seems to be a folder (metadata check)")
                continue

            local_file_paths.append(
                {
                    "file_path": fp,
                    "file_name": fn,
                }
            )
            files_found_in_folder += 1

            # Break after finding the first valid file per participant folder
            # This is a fast check - we only need one file per participant to verify they exist
            break

    # Thread-safe update of shared file_paths list
    with file_paths_lock:
        file_paths.extend(local_file_paths)

    print(f"Found {files_found_in_folder} files in the {source_folder} folder")
    return files_found_in_folder


def pipeline(thread_count=4):  # sourcery skip: low-code-quality
    """Create a file manifest for the folder"""
    print("=" * 80)
    print("Starting file manifest generation pipeline")
    print(f"Using {thread_count} threads")
    print("=" * 80)

    # Flag to enable fast folder checking (skip files without extensions)
    FAST_FOLDER_CHECK = True

    # source_folder_root = "/"
    # source_folder = f"{study_id}/completed/cardiac_ecg"

    # Define source folders to search for participant data
    # These folders represent different data types and instruments
    source_folders = [
        "cardiac_ecg/ecg_12lead/philips_tc30",
        "environment/environmental_sensor/leelab_anura",
        "retinal_flio/flio/heidelberg_flio",
        "retinal_oct/structural_oct/heidelberg_spectralis",
        "retinal_oct/structural_oct/topcon_maestro2",
        "retinal_oct/structural_oct/topcon_triton",
        "retinal_oct/structural_oct/zeiss_cirrus",
        "retinal_octa/enface/heidelberg_spectralis",
        "retinal_octa/enface/topcon_maestro2",
        "retinal_octa/enface/topcon_triton",
        "retinal_octa/enface/zeiss_cirrus",
        "retinal_octa/flow_cube/heidelberg_spectralis",
        "retinal_octa/flow_cube/topcon_maestro2",
        "retinal_octa/flow_cube/topcon_triton",
        "retinal_octa/flow_cube/zeiss_cirrus",
        "retinal_octa/segmentation/heidelberg_spectralis",
        "retinal_octa/segmentation/topcon_maestro2",
        "retinal_octa/segmentation/topcon_triton",
        "retinal_octa/segmentation/zeiss_cirrus",
        "retinal_photography/cfp/icare_eidon",
        "retinal_photography/cfp/optomed_aurora",
        "retinal_photography/cfp/topcon_maestro2",
        "retinal_photography/cfp/topcon_triton",
        "retinal_photography/faf/icare_eidon",
        "retinal_photography/ir/heidelberg_spectralis",
        "retinal_photography/ir/icare_eidon",
        "retinal_photography/ir/topcon_maestro2",
        "retinal_photography/ir/zeiss_cirrus",
        "wearable_activity_monitor/heart_rate/garmin_vivosmart5",
        "wearable_activity_monitor/oxygen_saturation/garmin_vivosmart5",
        "wearable_activity_monitor/physical_activity/garmin_vivosmart5",
        "wearable_activity_monitor/physical_activity_calorie/garmin_vivosmart5",
        "wearable_activity_monitor/respiratory_rate/garmin_vivosmart5",
        "wearable_activity_monitor/sleep/garmin_vivosmart5",
        "wearable_activity_monitor/stress/garmin_vivosmart5",
        "wearable_blood_glucose/continuous_glucose_monitoring/dexcom_g6",
    ]

    print(f"Total source folders to process: {len(source_folders)}")
    print("-" * 80)

    # destination_file = "/s/file-manifest.tsv"
    # destination_file = f"{study_id}/completed/file-manifest.tsv"

    # Initialize logger for tracking operations
    logger = logging.Logwatch("drain", print=True)

    # Connect to Azure Data Lake Storage
    # This will be used to access files in the stage-final-container
    print("Connecting to Azure Data Lake Storage...")
    file_system_client = azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_PRODUCTION_DANGEROUS_CONNECTION_STRING,
        file_system_name="stage-final-container",
    )
    print("✓ Connected to Azure Data Lake Storage successfully")
    print("-" * 80)

    # List to store all discovered file paths and metadata
    file_paths = []
    file_paths_lock = Lock()

    # Process source folders in parallel using ThreadPoolExecutor
    print(
        f"\nProcessing {len(source_folders)} source folders using {thread_count} threads..."
    )

    with ThreadPoolExecutor(max_workers=thread_count) as executor:
        # Submit all tasks
        future_to_folder = {
            executor.submit(
                process_source_folder,
                source_folder,
                idx,
                len(source_folders),
                file_system_client,
                file_paths,
                file_paths_lock,
                FAST_FOLDER_CHECK,
            ): source_folder
            for idx, source_folder in enumerate(source_folders, 1)
        }

        # Wait for all tasks to complete
        for future in as_completed(future_to_folder):
            source_folder = future_to_folder[future]
            try:
                future.result()
            except Exception as exc:
                print(f"Source folder {source_folder} generated an exception: {exc}")

    # Count total files discovered across all source folders
    total_files = len(file_paths)
    print("\n" + "=" * 80)
    print(f"File discovery complete: Found {total_files} total files")
    print("=" * 80)
    logger.info(f"Current total files: {total_files}")

    # Write the manifest file to disk
    # The manifest will be a TSV file with file names and their full paths
    print("\nWriting manifest file to 'file-manifest.tsv'...")
    manifest_filename = "file-manifest.tsv"
    with open(manifest_filename, "w") as f:
        # Write TSV header
        f.write("file_name\tfile_path\n")

        # Write each file entry to the manifest
        for file_item in file_paths:
            f.write(f"{file_item['file_name']}\t{file_item['file_path']}\n")

    print(f"✓ Manifest file written successfully: {manifest_filename}")
    print(f"  Total entries: {total_files}")
    print("=" * 80)
    print("Pipeline completed successfully!")
    print("=" * 80)

    # time_estimator = TimeEstimator(total_files)

    # with tempfile.TemporaryDirectory(
    #     prefix="folder_manifest_meta_pipeline_"
    # ) as temp_folder_path:
    #     # Write the manifest file
    #     manifest_file_path = f"{temp_folder_path}/file-manifest.tsv"

    #     with open(manifest_file_path, "w") as f:
    #         f.write("file_name\tmd5_checksum\tfile_path\n")

    #         for file_item in tqdm(file_paths, desc="Writing manifest"):
    #             f.write(
    #                 f"{file_item['file_name']}\t{file_item['md5_checksum']}\t{file_item['manifest_file_path']}\n"
    #             )

    #     # Upload the manifest file to the destination folder
    #     output_file_path = f"{destination_file}"

    #     with open(file=manifest_file_path, mode="rb") as f:
    #         output_file_client = file_system_client.get_file_client(
    #             file_path=output_file_path
    #         )

    #         output_file_client.upload_data(f, overwrite=True)

    #         logger.info(f"Uploaded manifest to {output_file_path}")


# Main entry point: run the pipeline when script is executed directly
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create a file manifest for folders")
    parser.add_argument(
        "--threads",
        type=int,
        default=4,
        help="Number of threads to use for processing source folders (default: 4)",
    )
    args = parser.parse_args()
    pipeline(thread_count=args.threads)
