"""Process Fitness tracker data files"""

import contextlib
import datetime
import os
import tempfile
import shutil
from traceback import format_exc

import garmin.Garmin_Read_Sleep as garmin_read_sleep
import garmin.Garmin_Read_Activity as garmin_read_activity
import garmin.standard_heart_rate as garmin_standardize_heart_rate
import azure.storage.blob as azureblob
import azure.storage.filedatalake as azurelake
import config
import utils.dependency as deps
from utils.file_map_processor import FileMapProcessor
import utils.logwatch as logging

"""
# Usage Instructions:
# The `FitnessTracker_Path` variable is used to specify the base directory path where the Garmin data is located.
# Depending on the dataset you want to process, uncomment and update the appropriate `FitnessTracker_Path` line.
# Make sure only one `FitnessTracker_Path` is uncommented at a time.
# Please note the folder names for UCSD_All and UW is GARMIN, but for UAB it should be changed to Gamrin (Lines 13, 14, and 15)
# Update the paths in lines 22 and 24 to point to the correct API code

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

    # Create a unique set of patient ids
    patient_ids = set()

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

        patient_identifier = parts[3]
        patient_id_parts = patient_identifier.split("-")
        if len(patient_id_parts) != 2:
            continue

        patient_id = patient_id_parts[1]

        modality = parts[5]

        if modality not in ["Activity", "Monitor", "Sleep"]:
            continue

        file_paths.append(
            {
                "file_path": t,
                "status": "failed",
                "processed": False,
                "convert_error": True,
                "output_uploaded": False,
                "output_files": [],
                "patient_id": patient_id,
                "modality": modality,
                "file_name": original_file_name,
            }
        )

        patient_ids.add(patient_id)

    logger.debug(f"Found {len(file_paths)} files in {input_folder}")

    # Create a temporary folder on the local machine
    temp_folder_path = tempfile.mkdtemp()

    # Create the output folder
    file_system_client.create_directory(processed_data_output_folder)

    workflow_file_dependencies = deps.WorkflowFileDependencies()

    file_processor = FileMapProcessor(dependency_folder, ignore_file)

    total_patients = len(patient_ids)

    for idx, patient_id in enumerate(patient_ids):
        patient_idx = idx + 1

        logger.debug(f"Processing {patient_id} - ({patient_idx}/{total_patients})")

        patient_files = [
            file_item
            for file_item in file_paths
            if file_item["patient_id"] == patient_id
        ]

        logger.debug(f"Found {len(patient_files)} files for {patient_id}")

        # Recreate the patient folder
        temp_patient_folder_path = os.path.join(temp_folder_path, patient_id)

        os.makedirs(temp_patient_folder_path, exist_ok=True)

        workflow_input_files = []

        file_processor.add_entry(patient_id, "")

        file_processor.clear_errors(patient_id)

        temp_conversion_output_folder_path = os.path.join(temp_folder_path, "converted")

        total_files = len(patient_files)

        logger.info(
            f"Begin downloading and conversion of all files for {patient_id} - ({patient_idx}/{total_patients})"
        )

        for idx2, file_item in enumerate(patient_files):
            file_idx = idx2 + 1

            path = file_item["file_path"]

            workflow_input_files.append(path)

            original_file_name = path.split("/")[-1]
            original_file_name_only = original_file_name.split(".")[0]
            file_modality = file_item["modality"]

            logger.debug(
                f"Downloading {path} - ({file_idx}/{total_files}) - ({patient_idx}/{total_patients})"
            )

            download_path = os.path.join(
                temp_patient_folder_path, file_modality, original_file_name
            )

            # Create the directory if it does not exist
            os.makedirs(os.path.dirname(download_path), exist_ok=True)

            blob_client = blob_service_client.get_blob_client(
                container="stage-1-container", blob=path
            )

            with open(download_path, "wb") as data:
                blob_client.download_blob().readinto(data)

            logger.info(
                f"Downloaded {original_file_name} to {download_path} - ({file_idx}/{total_files}) - ({patient_idx}/{total_patients})"
            )

            converted_output_folder_path = os.path.join(
                temp_conversion_output_folder_path, original_file_name_only
            )

            logger.info(
                f"Converting {modality}/{original_file_name} - ({file_idx}/{total_files}) - ({patient_idx}/{total_patients})"
            )

            if file_item["modality"] == "Sleep":
                try:
                    garmin_read_sleep.convert(
                        download_path, converted_output_folder_path
                    )

                    logger.info(
                        f"Converted {modality}/{original_file_name} - ({file_idx}/{total_files}) - ({patient_idx}/{total_patients})"
                    )

                except Exception:
                    logger.error(
                        f"Failed to convert {modality}/{original_file_name} - ({file_idx}/{total_files}) - ({patient_idx}/{total_patients})"
                    )
                    error_exception = format_exc()
                    error_exception = "".join(error_exception.splitlines())

                    logger.error(error_exception)

                    file_processor.append_errors(error_exception, patient_id)
                    continue
            elif file_item["modality"] in ["Activity", "Monitor"]:
                try:
                    garmin_read_activity.convert(
                        download_path, converted_output_folder_path
                    )

                    logger.info(
                        f"Converted {modality}/{original_file_name} - ({file_idx}/{total_files}) - ({patient_idx}/{total_patients})"
                    )

                except Exception:
                    logger.error(
                        f"Failed to convert {modality}/{original_file_name} - ({file_idx}/{total_files}) - ({patient_idx}/{total_patients})"
                    )
                    error_exception = format_exc()
                    error_exception = "".join(error_exception.splitlines())

                    logger.error(error_exception)

                    file_processor.append_errors(error_exception, patient_id)
                    continue
            else:
                logger.info(
                    f"Skipping {modality}/{original_file_name} - ({file_idx}/{total_files}) - ({patient_idx}/{total_patients})"
                )
                continue

        output_files = []

        try:
            logger.info(
                f"Standardizing heart rate for {patient_id} - ({patient_idx}/{total_patients})"
            )

            heart_rate_jsons_output_folder = os.path.join(
                temp_folder_path, "heart_rate_jsons"
            )
            final_heart_rate_output_folder = os.path.join(
                temp_folder_path, "final_heart_rate"
            )

            garmin_standardize_heart_rate.standardize_heart_rate(
                temp_conversion_output_folder_path,
                patient_id,
                heart_rate_jsons_output_folder,
                final_heart_rate_output_folder,
            )

            logger.info(
                f"Standardized heart rate for {patient_id} - ({patient_idx}/{total_patients})"
            )

            # list the contents of temp_folder_path
            for root, dirs, files in os.walk(final_heart_rate_output_folder):
                for file in files:
                    file_path = os.path.join(root, file)

                    print(file_path)
                    output_files.append(
                        {
                            "file_to_upload": file_path,
                            "uploaded_file_path": f"{processed_data_output_folder}/heart_rate/garmin_vivosmart5/{patient_id}/{file}",
                        }
                    )

        except Exception:
            logger.error(
                f"Failed to standardize heart rate for {original_file_name} - ({log_idx}/{total_files})"
            )
            error_exception = format_exc()
            error_exception = "".join(error_exception.splitlines())

            logger.error(error_exception)

            file_processor.append_errors(error_exception, path)
            continue

        file_item["convert_error"] = False
        file_item["processed"] = True

        logger.debug(
            f"Uploading outputs of {patient_id} - ({log_idx}/{total_files}) - {len(output_files)} files"
        )

        workflow_output_files = []

        outputs_uploaded = True

        for file in output_files:
            f_path = file["file_to_upload"]
            f_name = f_path.split("/")[-1]

            with open(f_path, "rb") as data:
                logger.info(
                    f"Uploading {f_name} to {processed_data_output_folder} - ({log_idx}/{total_files})"
                )

                output_file_path = file["uploaded_file_path"]

                try:
                    output_blob_client = blob_service_client.get_blob_client(
                        container="stage-1-container",
                        blob=output_file_path,
                    )
                    output_blob_client.upload_blob(data)
                except Exception:
                    outputs_uploaded = False
                    logger.error(f"Failed to upload {file} - ({log_idx}/{total_files})")
                    error_exception = format_exc()
                    error_exception = "".join(error_exception.splitlines())

                    logger.error(error_exception)

                    file_processor.append_errors(error_exception, path)
                    continue

                file_item["output_files"].append(output_file_path)
                workflow_output_files.append(output_file_path)

        if outputs_uploaded:
            file_item["output_uploaded"] = True
        else:
            file_item["output_uploaded"] = False

        workflow_file_dependencies.add_dependency(
            workflow_input_files, workflow_output_files
        )

        shutil.rmtree(temp_folder_path)


if __name__ == "__main__":
    pipeline("AI-READI")
