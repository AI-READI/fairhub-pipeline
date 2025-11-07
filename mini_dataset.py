"""Create a mini dataset based on a list of prespecified IDs"""

import azure.storage.filedatalake as azurelake
from uuid import uuid4
import os
import csv
import tempfile

import config
import utils.logwatch as logging
from utils.time_estimator import TimeEstimator


def pipeline():  # sourcery skip: use-itertools-product
    """Create a mini dataset based on a list of prespecified IDs"""

    logger = logging.Logwatch("drain", print=True)

    logger.info("Starting mini dataset creation pipeline...")

    # Define source folders to search for participant data
    source_folders = [
        "YR3/cardiac_ecg/ecg_12lead/philips_tc30",
        "YR3/environment/environmental_sensor/leelab_anura",
        "YR3/retinal_flio/flio/heidelberg_flio",
        "YR3/retinal_oct/structural_oct/heidelberg_spectralis",
        "YR3/retinal_oct/structural_oct/topcon_maestro2",
        "YR3/retinal_oct/structural_oct/topcon_triton",
        "YR3/retinal_oct/structural_oct/zeiss_cirrus",
        "YR3/retinal_octa/enface/heidelberg_spectralis",
        "YR3/retinal_octa/enface/topcon_maestro2",
        "YR3/retinal_octa/enface/topcon_triton",
        "YR3/retinal_octa/enface/zeiss_cirrus",
        "YR3/retinal_octa/flow_cube/heidelberg_spectralis",
        "YR3/retinal_octa/flow_cube/topcon_maestro2",
        "YR3/retinal_octa/flow_cube/topcon_triton",
        "YR3/retinal_octa/flow_cube/zeiss_cirrus",
        "YR3/retinal_octa/segmentation/heidelberg_spectralis",
        "YR3/retinal_octa/segmentation/topcon_maestro2",
        "YR3/retinal_octa/segmentation/topcon_triton",
        "YR3/retinal_octa/segmentation/zeiss_cirrus",
        "YR3/retinal_photography/cfp/icare_eidon",
        "YR3/retinal_photography/cfp/optomed_aurora",
        "YR3/retinal_photography/cfp/topcon_maestro2",
        "YR3/retinal_photography/cfp/topcon_triton",
        "YR3/retinal_photography/faf/icare_eidon",
        "YR3/retinal_photography/ir/heidelberg_spectralis",
        "YR3/retinal_photography/ir/icare_eidon",
        "YR3/retinal_photography/ir/topcon_maestro2",
        "YR3/retinal_photography/ir/zeiss_cirrus",
        "YR3/wearable_activity_monitor/heart_rate/garmin_vivosmart5",
        "YR3/wearable_activity_monitor/oxygen_saturation/garmin_vivosmart5",
        "YR3/wearable_activity_monitor/physical_activity/garmin_vivosmart5",
        "YR3/wearable_activity_monitor/physical_activity_calorie/garmin_vivosmart5",
        "YR3/wearable_activity_monitor/respiratory_rate/garmin_vivosmart5",
        "YR3/wearable_activity_monitor/sleep/garmin_vivosmart5",
        "YR3/wearable_activity_monitor/stress/garmin_vivosmart5",
        "YR3/wearable_blood_glucose/continuous_glucose_monitoring/dexcom_g6",
    ]

    logger.info(f"Configured {len(source_folders)} source folders to search")

    # Define manifest file paths for each data category
    manifest_file_paths = [
        "YR3/cardiac_ecg/manifest.tsv",
        "YR3/environment/manifest.tsv",
        "YR3/retinal_flio/manifest.tsv",
        "YR3/retinal_oct/manifest.tsv",
        "YR3/retinal_octa/manifest.tsv",
        "YR3/retinal_photography/manifest.tsv",
        "YR3/wearable_activity_monitor/manifest.tsv",
        "YR3/wearable_blood_glucose/manifest.tsv",
    ]

    logger.info(f"Configured {len(manifest_file_paths)} manifest files to process")

    # List of participant IDs to include in the mini dataset
    person_ids = []

    with open("mini_dataset_participants.tsv", "r") as data:
        reader = csv.reader(data, delimiter="\t")
        person_ids.extend(row[0] for row in reader)
        person_ids = list(set(person_ids))

        logger.info(
            f"Configured {len(person_ids)} participant IDs to include in mini dataset"
        )

    # Create the file system clients for Azure Data Lake Storage
    # Connect to the production container that contains the source data
    logger.info("Connecting to source Azure Data Lake Storage container...")
    input_file_system_client = azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_PRODUCTION_DANGEROUS_CONNECTION_STRING,
        file_system_name="stage-final-container",
    )
    logger.info("Successfully connected to source container")

    # Generate a unique container name for the new mini dataset
    new_container_name = f"mini-dataset-{uuid4()}"
    logger.info(f"Generated new container name: {new_container_name}")

    # Create the new container for the mini dataset
    logger.info(f"Creating new container: {new_container_name}...")
    new_container_client = azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_PRODUCTION_DANGEROUS_CONNECTION_STRING,
        file_system_name=new_container_name,
    )
    new_container_client.create_file_system()
    logger.info(f"Successfully created new container: {new_container_name}")

    # Initialize lists to track files and errors
    file_paths = []
    errors = []

    logger.info("Starting file discovery phase...")
    logger.debug(
        f"Iterating through {len(source_folders)} source folders and {len(person_ids)} participants..."
    )

    # Track files for discovery progress reporting
    total_files_checked = 0

    # Iterate through each source folder and participant ID to find matching files
    for source_folder in source_folders:
        for participant_id in person_ids:
            logger.debug(
                f"Searching for files in {source_folder} for participant {participant_id}"
            )
            # Construct the path to the participant's folder in this source folder
            patient_folder_path = f"{source_folder}/{participant_id}"

            # Get a client for the participant's folder
            patient_folder_client = input_file_system_client.get_directory_client(
                patient_folder_path,
            )

            # Check if the folder exists for this participant in this source folder
            if not patient_folder_client.exists():
                logger.warn(
                    f"Folder {patient_folder_path} does not exist. Skipping participant {participant_id} in {source_folder}..."
                )
                errors.append(
                    f"Folder {patient_folder_path} does not exist. Skipping participant {participant_id} in {source_folder}..."
                )
                continue

            # Get all paths in the participant's folder (recursively)
            paths = patient_folder_client.get_paths(recursive=True)

            # Process each path found in the participant's folder
            for path in paths:
                try:
                    # Check if the path is a file (not a directory)
                    if not path.is_directory:
                        total_files_checked += 1
                        file_path = path.name
                        logger.debug(f"Found file: {file_path}")

                        # Log progress every 100 files
                        if total_files_checked % 100 == 0:
                            logger.info(
                                f"Discovery progress: Found {len(file_paths)} valid files out of {total_files_checked} total files checked"
                            )

                        # Get file client to check properties
                        file_client = input_file_system_client.get_file_client(
                            file_path
                        )
                        file_properties = file_client.get_file_properties()
                        file_metadata = file_properties.metadata

                        # Skip if this is marked as a folder in metadata
                        if file_metadata.get("hdi_isfolder"):
                            logger.debug(
                                f"Skipping {file_path} - marked as folder in metadata"
                            )
                            continue

                        # Add the file path to our collection
                        file_paths.append(file_path)
                except Exception as e:
                    total_files_checked += 1
                    # Log progress every 100 files (including errors)
                    if total_files_checked % 100 == 0:
                        logger.info(
                            f"Discovery progress: Found {len(file_paths)} valid files out of {total_files_checked} total files checked"
                        )
                    logger.error(
                        f"Error processing path in {source_folder} for participant {participant_id}: {e}. Skipping..."
                    )
                    errors.append(
                        f"Error processing path in {source_folder} for participant {participant_id}: {e}. Skipping..."
                    )
                    continue

    logger.info(
        f"File discovery phase complete. Found {len(file_paths)} valid files out of {total_files_checked} total files checked"
    )

    # Create a temporary directory to download files before uploading to the new container
    logger.info("Starting file copy phase...")
    logger.debug("Creating temporary directory for file transfers...")
    with tempfile.TemporaryDirectory(prefix="mini_dataset_") as temp_folder_path:
        logger.debug(f"Using temporary directory: {temp_folder_path}")
        total_files = len(file_paths)
        copy_time_estimator = TimeEstimator(total_files)
        for idx, file_path in enumerate(file_paths, 1):
            logger.debug(f"Copying file {idx}/{total_files}: {file_path}")
            try:
                # Get file client for the source file
                file_client = input_file_system_client.get_file_client(file_path)
                file_name = os.path.basename(file_path)

                # Download the file to temporary storage
                download_path = os.path.join(temp_folder_path, file_name)
                with open(download_path, "wb") as data:
                    file_client.download_file().readinto(data)

                # Upload the file to the new container, preserving the original path structure
                with open(download_path, "rb") as data:
                    new_file_client = new_container_client.get_file_client(
                        file_path=file_path
                    )
                    new_file_client.upload_data(data, overwrite=True)

                # Clean up temporary file
                os.remove(download_path)
                logger.time(copy_time_estimator.step())
            except Exception as e:
                logger.error(f"Error copying file {file_path}: {e}. Skipping...")
                errors.append(f"Error copying file {file_path}: {e}. Skipping...")
                logger.time(copy_time_estimator.step())
                continue

    logger.info("File copy phase complete")

    # Process manifest files to filter them to only include rows for the specified participant IDs
    logger.info("Starting manifest file processing phase...")
    with tempfile.TemporaryDirectory(
        prefix="mini_dataset_manifest_"
    ) as temp_folder_path:
        logger.debug(
            f"Using temporary directory for manifest processing: {temp_folder_path}"
        )
        manifest_time_estimator = TimeEstimator(len(manifest_file_paths))
        for manifest_file_path in manifest_file_paths:
            logger.info(f"Processing manifest file: {manifest_file_path}")

            # Get client for the manifest file in the source container
            manifest_file_client = input_file_system_client.get_file_client(
                manifest_file_path
            )

            manifest_file_name = "manifest.tsv"

            # Download the manifest file to temporary storage
            logger.debug(f"Downloading manifest file: {manifest_file_name}...")
            download_path = os.path.join(temp_folder_path, manifest_file_name)
            with open(download_path, "wb") as data:
                manifest_file_client.download_file().readinto(data)
            logger.debug(f"Downloaded manifest file to: {download_path}")

            # Filter the manifest file to only include rows for participants in person_ids list
            # The first column (row[0]) should contain the person_id
            logger.debug("Filtering manifest file rows...")
            with open(download_path, "r") as data:
                reader = csv.reader(data, delimiter="\t")
                # Keep the header row (first row)
                target_rows = [next(reader)]
                # Add rows where the person_id (first column) matches our list
                target_rows.extend(row for row in reader if row[0] in str(person_ids))
                logger.debug(
                    f"Filtered manifest: kept {len(target_rows) - 1} data rows (plus 1 header row)"
                )

                # Write the filtered rows to a new manifest file
                logger.debug(
                    f"Writing {len(target_rows)} rows to filtered manifest file..."
                )
                with open(
                    os.path.join(temp_folder_path, f"filtered_{manifest_file_name}"),
                    "w",
                    newline="",
                ) as data:
                    writer = csv.writer(data, delimiter="\t")
                    for row in target_rows:
                        writer.writerow(row)
                logger.debug("Successfully wrote filtered manifest file")

                # Upload the filtered manifest file to the new container

                logger.debug(
                    f"Uploading filtered manifest file to: {manifest_file_path}"
                )
                with open(
                    os.path.join(temp_folder_path, f"filtered_{manifest_file_name}"),
                    "rb",
                ) as data:
                    new_manifest_file_client = new_container_client.get_file_client(
                        file_path=manifest_file_path,
                    )
                    new_manifest_file_client.upload_data(data, overwrite=True)
                logger.debug("Successfully uploaded filtered manifest file")

                # Clean up the filtered manifest file from temporary storage
                filtered_temp_path = os.path.join(
                    temp_folder_path, f"filtered_{manifest_file_name}"
                )
                os.remove(filtered_temp_path)
                logger.debug(f"Cleaned up temporary file: {filtered_temp_path}")

            os.remove(download_path)
            logger.debug(f"Cleaned up temporary file: {download_path}")

            logger.time(manifest_time_estimator.step())

    logger.info("Manifest file processing phase complete")

    # Print summary of errors encountered during processing
    logger.info("-" * 100)
    logger.info("ERROR SUMMARY")
    logger.info("-" * 100)
    for error in errors:
        logger.error(error)
    logger.info("-" * 100)

    # Print final summary statistics
    logger.info("=" * 100)
    logger.info("PIPELINE SUMMARY")
    logger.info("=" * 100)
    logger.info(f"Total errors encountered: {len(errors)}")
    logger.info(f"Total files copied: {len(file_paths)}")
    logger.info(f"Total participants processed: {len(person_ids)}")
    logger.info(f"Total source folders searched: {len(source_folders)}")
    logger.info(f"Total manifest files processed: {len(manifest_file_paths)}")
    logger.info(f"New container name: {new_container_name}")
    logger.info("=" * 100)
    logger.info("Mini dataset creation pipeline completed successfully!")


if __name__ == "__main__":
    pipeline()
