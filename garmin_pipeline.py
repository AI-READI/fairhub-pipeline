"""Process Fitness tracker data files"""

import contextlib
import datetime
import os
import tempfile
import shutil
from traceback import format_exc
import multiprocessing
from pathlib import Path

import garmin.Garmin_Read_Sleep as garmin_read_sleep
import azure.storage.blob as azureblob
import azure.storage.filedatalake as azurelake
import config
import utils.dependency as deps
import time
import csv
from utils.file_map_processor import FileMapProcessor
import utils.logwatch as logging

"""
### Usage Instructions:
### The `FitnessTracker_Path` variable is used to specify the base directory path where the Garmin data is located.
### Depending on the dataset you want to process, uncomment and update the appropriate `FitnessTracker_Path` line.
### Make sure only one `FitnessTracker_Path` is uncommented at a time.
### Please note the folder names for UCSD_All and UW is GARMIN, but for UAB it should be changed to Gamrin (Lines 13, 14, and 15)
### Update the paths in lines 22 and 24 to point to the correct API code

FitnessTracker_Path="/Users/arashalavi/Desktop/AIREADI-STANDARD-CODE/UAB/FitnessTracker" #(it uses /FitnessTracker-*/Garmin/* below)
#FitnessTracker_Path="/Users/arashalavi/Desktop/AIREADI-STANDARD-CODE/UCSD_All/FitnessTracker" #(it uses /FitnessTracker-*/GARMIN/* below)
#FitnessTracker_Path="/Users/arashalavi/Desktop/AIREADI-STANDARD-CODE/UW/FitnessTracker" #(it uses /FitnessTracker-*/GARMIN/* below)


for file in "$FitnessTracker_Path"/FitnessTracker-*/Garmin/Activity/*.fit \
            "$FitnessTracker_Path"/FitnessTracker-*/Garmin/Monitor/*.FIT \
            "$FitnessTracker_Path"/FitnessTracker-*/Garmin/Sleep/*.fit; do
    if [ -f "$file" ]; then
        dir=$(dirname "$file")
        echo "$file"
        echo "$dir"
        cd "$dir" || exit
        if [[ "$file" == *"/Sleep/"* ]]; then
            python3 /Users/arashalavi/Desktop/AIREADI-STANDARD-CODE/Garmin_Read_Sleep.py "$file"
        else
            python3 /Users/arashalavi/Desktop/AIREADI-STANDARD-CODE/Garmin_Read_Activity.py "$file"
        fi
        cd - || exit
    fi
done
"""


def pipeline(study_id: str):  # sourcery skip: low-code-quality
    """Process fitness tracker data files for a study
    Args:
        study_id (str): the study id
    """

    if study_id is None or not study_id:
        raise ValueError("study_id is required")

    input_folder = f"{study_id}/pooled-data/FitnessTracker"
    processed_data_output_folder = f"{study_id}/pooled-data/FitnessTracker-processed"
    dependency_folder = f"{study_id}/dependency/FitnessTracker"
    manifest_folder = f"{study_id}/manifest/FitnessTracker"
    pipeline_workflow_log_folder = f"{study_id}/logs/FitnessTracker"
    ignore_file = f"{study_id}/ignore/fitnessTracker.ignore"

    logger = logging.Logwatch("fitness_tracker", print=True)

    sas_token = azureblob.generate_account_sas(
        account_name="b2aistaging",
        account_key=config.AZURE_STORAGE_ACCESS_KEY,
        resource_types=azureblob.ResourceTypes(container=True, object=True),
        permission=azureblob.AccountSasPermissions(
            read=True, write=True, list=True, delete=True
        ),
        expiry=datetime.datetime.now(datetime.timezone.utc)
        + datetime.timedelta(hours=24),
    )

    # Get the blob service client
    blob_service_client = azureblob.BlobServiceClient(
        account_url="https://b2aistaging.blob.core.windows.net/",
        credential=sas_token,
    )

    # Get the list of blobs in the input folder
    file_system_client = azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_CONNECTION_STRING,
        file_system_name="stage-1-container",
    )

    # Delete the output folder if it exists
    # TODO: Remove this after merge
    with contextlib.suppress(Exception):
        file_system_client.delete_directory(processed_data_output_folder)

    paths = file_system_client.get_paths(path=input_folder)

    file_paths = []

    # dev limit
    dev_limit = 1000

    file_count = 0
    added_file_count = 0

    for path in paths:
        file_count += 1

        if file_count % 1000 == 0:
            logger.info(f"Found {file_count} files...")

        t = str(path.name)

        original_file_name = t.split("/")[-1]

        # Check if the item is a .fit or .FIT file
        if original_file_name.split(".")[-1].lower() != "fit":
            continue

        added_file_count += 1

        # dev limit
        if added_file_count > dev_limit:
            break

        if added_file_count % 1000 == 0:
            logger.info(f"Added {added_file_count} files to the processing queue...")

        parts = t.split("/")

        if len(parts) != 7:
            continue

        file_paths.append(
            {
                "file_path": t,
                "status": "failed",
                "processed": False,
                "convert_error": True,
                "output_uploaded": False,
                "output_files": [],
                "patient_id": parts[3],
                "modality": parts[5],
                "file_name": original_file_name,
            }
        )

    logger.debug(f"Found {len(file_paths)} files in {input_folder}")

    # Create the output folder
    file_system_client.create_directory(processed_data_output_folder)

    workflow_file_dependencies = deps.WorkflowFileDependencies()

    file_processor = FileMapProcessor(dependency_folder, ignore_file)

    total_files = len(file_paths)

    allowed_modalities = ["Activity", "Monitor", "Sleep"]

    for idx, file_item in enumerate(file_paths):
        log_idx = idx + 1

        if file_item["modality"] not in allowed_modalities:
            logger.info(
                f"Modality not requested for processing - {file_item['modality']} - ({log_idx}/{total_files})"
            )
            continue

        # Create a temporary folder on the local machine
        temp_folder_path = tempfile.mkdtemp()

        path = file_item["file_path"]

        workflow_input_files = [path]

        # get the file name from the path
        original_file_name = path.split("/")[-1]

        should_file_be_ignored = file_processor.is_file_ignored(file_item, path)

        if should_file_be_ignored:
            logger.info(f"Ignoring {original_file_name} - ({log_idx}/{total_files})")
            continue

        # download the file to the temp folder
        blob_client = blob_service_client.get_blob_client(
            container="stage-1-container", blob=path
        )

        input_last_modified = blob_client.get_blob_properties().last_modified

        should_process = file_processor.file_should_process(path, input_last_modified)

        if not should_process:
            logger.debug(
                f"The file {path} has not been modified since the last time it was processed",
            )
            logger.debug(
                f"Skipping {path} - ({log_idx}/{total_files}) - File has not been modified"
            )

            continue

        file_processor.add_entry(path, input_last_modified)

        file_processor.clear_errors(path)

        download_path = os.path.join(temp_folder_path, original_file_name)

        with open(download_path, "wb") as data:
            blob_client.download_blob().readinto(data)

        print(file_item)

        logger.info(
            f"Downloaded {original_file_name} to {download_path} - ({log_idx}/{total_files})"
        )

        try:
            garmin_read_sleep.convert(download_path)
        except Exception as e:
            logger.error(
                f"Failed to convert {original_file_name} - ({log_idx}/{total_files})"
            )
            error_exception = format_exc()
            error_exception = "".join(error_exception.splitlines())

            logger.error(error_exception)

            file_processor.append_errors(error_exception, path)
            continue

    file_item["convert_error"] = False
    file_item["processed"] = True

    logger.debug(
        f"Uploading outputs of {original_file_name} to {processed_data_output_folder} - ({log_idx}/{total_files})"
    )

    # list the contents of temp_folder_path
    for root, dirs, files in os.walk(temp_folder_path):
        for file in files:
            file_path = os.path.join(root, file)

            print(file_path)


if __name__ == "__main__":
    pipeline("AI-READI")
