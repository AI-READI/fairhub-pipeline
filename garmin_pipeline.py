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
import garmin.standard_oxygen_saturation as garmin_standardize_oxygen_saturation
import garmin.standard_physical_activities as garmin_standardize_physical_activities
import garmin.standard_physical_activity_calorie as garmin_standardize_physical_activity_calories
import garmin.standard_respiratory_rate as garmin_standardize_respiratory_rate
import garmin.standard_sleep_stages as garmin_standardize_sleep_stages
import garmin.standard_stress as garmin_standardize_stress
import azure.storage.blob as azureblob
import azure.storage.filedatalake as azurelake
import config
import utils.dependency as deps
from utils.file_map_processor import FileMapProcessor
import utils.logwatch as logging
import csv
import time
from utils.time_estimator import TimeEstimator

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
    pipeline_workflow_log_folder = f"{study_id}/logs/FitnessTracker"
    dependency_folder = f"{study_id}/dependency/FitnessTracker"
    ignore_file = f"{study_id}/ignore/fitnessTracker.ignore"

    logger = logging.Logwatch("fitness_tracker", print=True)

    # sas_token = azureblob.generate_account_sas(
    #     account_name="b2aistaging",
    #     account_key=config.AZURE_STORAGE_ACCESS_KEY,
    #     resource_types=azureblob.ResourceTypes(container=True, object=True),
    #     permission=azureblob.AccountSasPermissions(
    #         read=True, write=True, list=True, delete=True
    #     ),
    #     expiry=datetime.datetime.now(datetime.timezone.utc)
    #     + datetime.timedelta(hours=24),
    # )

    # Get the blob service client
    # blob_service_client = azureblob.BlobServiceClient(
    #     account_url="https://b2aistaging.blob.core.windows.net/",
    #     credential=sas_token,
    # )

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

    # Create the output folder
    file_system_client.create_directory(processed_data_output_folder)

    # Create a temporary folder on the local machine
    meta_temp_folder_path = tempfile.mkdtemp(prefix="garmin_pipeline_meta_")

    workflow_file_dependencies = deps.WorkflowFileDependencies()

    file_processor = FileMapProcessor(dependency_folder, ignore_file)

    total_patients = len(patient_ids)

    time_estimator = TimeEstimator(len(file_paths))

    for idx, patient_id in enumerate(patient_ids):
        patient_idx = idx + 1

        logger.debug(f"Processing {patient_id} - ({patient_idx}/{total_patients})")

        patient_files = [
            file_item
            for file_item in file_paths
            if file_item["patient_id"] == patient_id
        ]

        logger.debug(f"Found {len(patient_files)} files for {patient_id}")

        # Create a temporary folder on the local machine
        temp_folder_path = tempfile.mkdtemp(prefix="garmin_pipeline_")

        # Recreate the patient folder
        temp_patient_folder_path = os.path.join(temp_folder_path, patient_id)

        os.makedirs(temp_patient_folder_path, exist_ok=True)

        workflow_input_files = []

        file_processor.add_entry(patient_id, "")

        file_processor.clear_errors(patient_id)

        temp_conversion_output_folder_path = os.path.join(temp_folder_path, "converted")

        total_files = len(patient_files)

        logger.info(
            f"Begin download and conversion of all files for {patient_id} - ({patient_idx}/{total_patients})"
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

            blob_client = file_system_client.get_file_client(file_path=path)


            with open(download_path, "wb") as data:
                blob_client.download_file().readinto(data)

            logger.info(
                f"Downloaded {original_file_name} to {download_path} - ({file_idx}/{total_files}) - ({patient_idx}/{total_patients})"
            )

            converted_output_folder_path = os.path.join(
                temp_conversion_output_folder_path, original_file_name_only
            )

            logger.info(
                f"Converting {file_modality}/{original_file_name} - ({file_idx}/{total_files}) - ({patient_idx}/{total_patients})"
            )

            if file_modality == "Sleep":
                try:
                    garmin_read_sleep.convert(
                        download_path, converted_output_folder_path
                    )

                    logger.info(
                        f"Converted {file_modality}/{original_file_name} - ({file_idx}/{total_files}) - ({patient_idx}/{total_patients})"
                    )

                    for root, dirs, files in os.walk(converted_output_folder_path):
                        for file in files:
                            file_path = os.path.join(root, file)

                            logger.info(
                                f"Adding {file_path} to the output files for {patient_id} - ({patient_idx}/{total_patients})"
                            )
                except Exception:
                    logger.error(
                        f"Failed to convert {file_modality}/{original_file_name} - ({file_idx}/{total_files}) - ({patient_idx}/{total_patients})"
                    )
                    error_exception = format_exc()
                    error_exception = "".join(error_exception.splitlines())

                    logger.error(error_exception)

                    file_processor.append_errors(error_exception, patient_id)
                    continue
            elif file_modality in ["Activity", "Monitor"]:
                try:
                    garmin_read_activity.convert(
                        download_path, converted_output_folder_path
                    )

                    logger.info(
                        f"Converted {file_modality}/{original_file_name} - ({file_idx}/{total_files}) - ({patient_idx}/{total_patients})"
                    )

                except Exception:
                    logger.error(
                        f"Failed to convert {file_modality}/{original_file_name} - ({file_idx}/{total_files}) - ({patient_idx}/{total_patients})"
                    )
                    error_exception = format_exc()
                    error_exception = "".join(error_exception.splitlines())

                    logger.error(error_exception)

                    file_processor.append_errors(error_exception, patient_id)
                    continue
            else:
                logger.info(
                    f"Skipping {file_modality}/{original_file_name} - ({file_idx}/{total_files}) - ({patient_idx}/{total_patients})"
                )
                continue

            # Delete the downloaded file
            os.remove(download_path)

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

            shutil.rmtree(heart_rate_jsons_output_folder)

            logger.info(
                f"Standardized heart rate for {patient_id} - ({patient_idx}/{total_patients})"
            )

            # list the contents of the final heart rate folder
            for root, dirs, files in os.walk(final_heart_rate_output_folder):
                for file in files:
                    file_path = os.path.join(root, file)

                    logger.info(
                        f"Adding {file_path} to the output files for {patient_id} - ({patient_idx}/{total_patients})"
                    )

                    output_files.append(
                        {
                            "file_to_upload": file_path,
                            "uploaded_file_path": f"{processed_data_output_folder}/heart_rate/garmin_vivosmart5/{patient_id}/{file}",
                        }
                    )
        except Exception:
            logger.error(
                f"Failed to standardize heart rate for {patient_id} - ({patient_idx}/{total_patients})"
            )
            error_exception = format_exc()
            error_exception = "".join(error_exception.splitlines())

            logger.error(error_exception)

            file_processor.append_errors(error_exception, patient_id)
            continue

        try:
            logger.info(
                f"Standardizing oxygen saturation for {patient_id} - ({patient_idx}/{total_patients})"
            )

            oxygen_saturation_jsons_output_folder = os.path.join(
                temp_folder_path, "oxygen_saturation_jsons"
            )
            final_oxygen_saturation_output_folder = os.path.join(
                temp_folder_path, "final_oxygen_saturation"
            )

            garmin_standardize_oxygen_saturation.standardize_oxygen_saturation(
                temp_conversion_output_folder_path,
                patient_id,
                oxygen_saturation_jsons_output_folder,
                final_oxygen_saturation_output_folder,
            )

            shutil.rmtree(oxygen_saturation_jsons_output_folder)

            logger.info(
                f"Standardized oxygen saturation for {patient_id} - ({patient_idx}/{total_patients})"
            )

            # list the contents of the final oxygen saturation folder
            for root, dirs, files in os.walk(final_oxygen_saturation_output_folder):
                for file in files:
                    file_path = os.path.join(root, file)

                    logger.info(
                        f"Adding {file_path} to the output files for {patient_id} - ({patient_idx}/{total_patients})"
                    )

                    output_files.append(
                        {
                            "file_to_upload": file_path,
                            "uploaded_file_path": f"{processed_data_output_folder}/oxygen_saturation/garmin_vivosmart5/{patient_id}/{file}",
                        }
                    )
        except Exception:
            logger.error(
                f"Failed to standardize oxygen saturation for {patient_id} - ({patient_idx}/{total_patients})"
            )
            error_exception = format_exc()
            error_exception = "".join(error_exception.splitlines())

            logger.error(error_exception)

            file_processor.append_errors(error_exception, patient_id)
            continue

        try:
            logger.info(
                f"Standardizing physical activities for {patient_id} - ({patient_idx}/{total_patients})"
            )

            physical_activities_jsons_output_folder = os.path.join(
                temp_folder_path, "physical_activities_jsons"
            )
            final_physical_activities_output_folder = os.path.join(
                temp_folder_path, "final_physical_activities"
            )

            garmin_standardize_physical_activities.standardize_physical_activities(
                temp_conversion_output_folder_path,
                patient_id,
                physical_activities_jsons_output_folder,
                final_physical_activities_output_folder,
            )

            shutil.rmtree(physical_activities_jsons_output_folder)

            logger.info(
                f"Standardized physical activities for {patient_id} - ({patient_idx}/{total_patients})"
            )

            # list the contents of the final physical activities folder
            for root, dirs, files in os.walk(final_physical_activities_output_folder):
                for file in files:
                    file_path = os.path.join(root, file)

                    logger.info(
                        f"Adding {file_path} to the output files for {patient_id} - ({patient_idx}/{total_patients})"
                    )

                    output_files.append(
                        {
                            "file_to_upload": file_path,
                            "uploaded_file_path": f"{processed_data_output_folder}/physical_activity/garmin_vivosmart5/{patient_id}/{file}",
                        }
                    )
        except Exception:
            logger.error(
                f"Failed to standardize physical activities for {patient_id} - ({patient_idx}/{total_patients})"
            )
            error_exception = format_exc()
            error_exception = "".join(error_exception.splitlines())

            logger.error(error_exception)

            file_processor.append_errors(error_exception, patient_id)
            continue

        try:
            logger.info(
                f"Standardizing physical activity calories for {patient_id} - ({patient_idx}/{total_patients})"
            )

            physical_activity_calories_jsons_output_folder = os.path.join(
                temp_folder_path, "physical_activity_calories_jsons"
            )
            final_physical_activity_calories_output_folder = os.path.join(
                temp_folder_path, "final_physical_activity_calories"
            )

            garmin_standardize_physical_activity_calories.standardize_physical_activity_calories(
                temp_conversion_output_folder_path,
                patient_id,
                physical_activity_calories_jsons_output_folder,
                final_physical_activity_calories_output_folder,
            )

            shutil.rmtree(physical_activity_calories_jsons_output_folder)

            logger.info(
                f"Standardized physical activity calories for {patient_id} - ({patient_idx}/{total_patients})"
            )

            # list the contents of the final physical activity calories folder
            for root, dirs, files in os.walk(
                final_physical_activity_calories_output_folder
            ):
                for file in files:
                    file_path = os.path.join(root, file)

                    logger.info(
                        f"Adding {file_path} to the output files for {patient_id} - ({patient_idx}/{total_patients})"
                    )

                    output_files.append(
                        {
                            "file_to_upload": file_path,
                            "uploaded_file_path": f"{processed_data_output_folder}/physical_activity_calorie/garmin_vivosmart5/{patient_id}/{file}",
                        }
                    )
        except Exception:
            logger.error(
                f"Failed to standardize physical activity calories for {patient_id} - ({patient_idx}/{total_patients})"
            )
            error_exception = format_exc()
            error_exception = "".join(error_exception.splitlines())

            logger.error(error_exception)

            file_processor.append_errors(error_exception, patient_id)
            continue

        try:
            logger.info(
                f"Standardizing respiratory rate for {patient_id} - ({patient_idx}/{total_patients})"
            )

            respiratory_rate_jsons_output_folder = os.path.join(
                temp_folder_path, "respiratory_rate_jsons"
            )
            final_respiratory_rate_output_folder = os.path.join(
                temp_folder_path, "final_respiratory_rate"
            )

            garmin_standardize_respiratory_rate.standardize_respiratory_rate(
                temp_conversion_output_folder_path,
                patient_id,
                respiratory_rate_jsons_output_folder,
                final_respiratory_rate_output_folder,
            )

            shutil.rmtree(respiratory_rate_jsons_output_folder)

            logger.info(
                f"Standardized respiratory rate for {patient_id} - ({patient_idx}/{total_patients})"
            )

            # list the contents of the final respiratory rate folder
            for root, dirs, files in os.walk(final_respiratory_rate_output_folder):
                for file in files:
                    file_path = os.path.join(root, file)

                    logger.info(
                        f"Adding {file_path} to the output files for {patient_id} - ({patient_idx}/{total_patients})"
                    )

                    output_files.append(
                        {
                            "file_to_upload": file_path,
                            "uploaded_file_path": f"{processed_data_output_folder}/respiratory_rate/garmin_vivosmart5/{patient_id}/{file}",
                        }
                    )
        except Exception:
            logger.error(
                f"Failed to standardize respiratory rate for {patient_id} - ({patient_idx}/{total_patients})"
            )
            error_exception = format_exc()
            error_exception = "".join(error_exception.splitlines())

            logger.error(error_exception)

            file_processor.append_errors(error_exception, patient_id)
            continue

        try:
            logger.info(
                f"Standardizing sleep stages for {patient_id} - ({patient_idx}/{total_patients})"
            )

            sleep_stages_jsons_output_folder = os.path.join(
                temp_folder_path, "sleep_jsons"
            )
            final_sleep_stages_output_folder = os.path.join(
                temp_folder_path, "final_sleep_stages"
            )

            garmin_standardize_sleep_stages.standardize_sleep_stages(
                temp_conversion_output_folder_path,
                patient_id,
                sleep_stages_jsons_output_folder,
                final_sleep_stages_output_folder,
            )

            shutil.rmtree(sleep_stages_jsons_output_folder)

            logger.info(
                f"Standardized sleep stages for {patient_id} - ({patient_idx}/{total_patients})"
            )

            for root, dirs, files in os.walk(final_sleep_stages_output_folder):
                for file in files:
                    file_path = os.path.join(root, file)

                    logger.info(
                        f"Adding {file_path} to the output files for {patient_id} - ({patient_idx}/{total_patients})"
                    )

                    output_files.append(
                        {
                            "file_to_upload": file_path,
                            "uploaded_file_path": f"{processed_data_output_folder}/sleep/garmin_vivosmart5/{patient_id}/{file}",
                        }
                    )
        except Exception:
            logger.error(
                f"Failed to standardize sleep stages for {patient_id} - ({patient_idx}/{total_patients})"
            )
            error_exception = format_exc()
            error_exception = "".join(error_exception.splitlines())

            logger.error(error_exception)

            file_processor.append_errors(error_exception, patient_id)
            continue

        try:
            logger.info(
                f"Standardizing stress for {patient_id} - ({patient_idx}/{total_patients})"
            )

            stress_jsons_output_folder = os.path.join(temp_folder_path, "stress_jsons")
            final_stress_output_folder = os.path.join(temp_folder_path, "final_stress")

            garmin_standardize_stress.standardize_stress(
                temp_conversion_output_folder_path,
                patient_id,
                stress_jsons_output_folder,
                final_stress_output_folder,
            )

            shutil.rmtree(stress_jsons_output_folder)

            logger.info(
                f"Standardized stress for {patient_id} - ({patient_idx}/{total_patients})"
            )

            # list the contents of the final stress folder
            for root, dirs, files in os.walk(final_stress_output_folder):
                for file in files:
                    file_path = os.path.join(root, file)

                    logger.info(
                        f"Adding {file_path} to the output files for {patient_id} - ({patient_idx}/{total_patients})"
                    )

                    output_files.append(
                        {
                            "file_to_upload": file_path,
                            "uploaded_file_path": f"{processed_data_output_folder}/stress/garmin_vivosmart5/{patient_id}/{file}",
                        }
                    )
        except Exception:
            logger.error(
                f"Failed to standardize stress for {patient_id} - ({patient_idx}/{total_patients})"
            )
            error_exception = format_exc()
            error_exception = "".join(error_exception.splitlines())

            logger.error(error_exception)

            file_processor.append_errors(error_exception, patient_id)
            continue

        file_item["convert_error"] = False
        file_item["processed"] = True

        workflow_output_files = []

        outputs_uploaded = True

        total_output_files = len(output_files)

        logger.info(
            f"Uploading {total_output_files} output files for {patient_id} - ({patient_idx}/{total_patients})"
        )

        for idx3, file in enumerate(output_files):
            log_idx = idx3 + 1

            f_path = file["file_to_upload"]
            f_name = f_path.split("/")[-1]

            with open(f_path, "rb") as data:
                logger.info(
                    f"Uploading {f_name} - ({log_idx}/{total_output_files}) - ({patient_idx}/{total_patients})"
                )

                output_file_path = file["uploaded_file_path"]

                try:
                    output_blob_client = file_system_client.get_file_client(file_path=output_file_path)
                    output_blob_client.download_file(data)

                except Exception:
                    outputs_uploaded = False
                    logger.error(f"Failed to upload {file} - ({log_idx}/{total_files})")
                    error_exception = format_exc()
                    error_exception = "".join(error_exception.splitlines())

                    logger.error(error_exception)

                    file_processor.append_errors(error_exception, patient_id)
                    continue

                file_item["output_files"].append(output_file_path)
                workflow_output_files.append(output_file_path)

        file_processor.confirm_output_files(
            patient_id, [file["uploaded_file_path"] for file in output_files], ""
        )

        if outputs_uploaded:
            file_item["output_uploaded"] = True
            file_item["status"] = "success"
            logger.info(
                f"Uploaded outputs of {original_file_name} to {processed_data_output_folder} - ({log_idx}/{total_files})"
            )
        else:
            logger.error(
                f"Failed to upload outputs of {original_file_name} to {processed_data_output_folder} - ({log_idx}/{total_files})"
            )

        workflow_file_dependencies.add_dependency(
            workflow_input_files, workflow_output_files
        )

        logger.debug(f"Uploading file map to {dependency_folder}/file_map.json")

        try:
            file_processor.upload_json()
            logger.info(f"Uploaded file map to {dependency_folder}/file_map.json")
        except Exception as e:
            logger.error(
                f"Failed to upload file map to {dependency_folder}/file_map.json"
            )
            raise e

        logger.time(time_estimator.step())

        shutil.rmtree(temp_folder_path)

    # file_processor.delete_out_of_date_output_files()

    # file_processor.remove_seen_flag_from_map()

    logger.debug(f"Uploading file map to {dependency_folder}/file_map.json")

    try:
        file_processor.upload_json()
        logger.info(f"Uploaded file map to {dependency_folder}/file_map.json")
    except Exception as e:
        logger.error(f"Failed to upload file map to {dependency_folder}/file_map.json")
        raise e

    # Write the workflow log to a file
    timestr = time.strftime("%Y%m%d-%H%M%S")
    file_name = f"status_report_{timestr}.csv"
    workflow_log_file_path = os.path.join(meta_temp_folder_path, file_name)

    with open(workflow_log_file_path, mode="w") as f:
        fieldnames = [
            "file_path",
            "status",
            "processed",
            "convert_error",
            "output_uploaded",
            "output_files",
            "patient_id",
            "modality",
            "file_name",
        ]

        writer = csv.DictWriter(f, fieldnames=fieldnames)

        for file_item in file_paths:
            writer.writerow(file_item)

        writer.writeheader()
        writer.writerows(file_paths)

    with open(workflow_log_file_path, mode="rb") as data:
        logger.debug(
            f"Uploading workflow log to {pipeline_workflow_log_folder}/{file_name}"
        )

        output_blob_client = blob_service_client.get_blob_client(
            container="stage-1-container",
            blob=f"{pipeline_workflow_log_folder}/{file_name}",
        )

        output_blob_client.upload_blob(data)

        logger.info(
            f"Uploaded workflow log to {pipeline_workflow_log_folder}/{file_name}"
        )

    # Write the dependencies to a file
    deps_output = workflow_file_dependencies.write_to_file(meta_temp_folder_path)

    json_file_path = deps_output["file_path"]
    json_file_name = deps_output["file_name"]

    logger.debug(f"Uploading dependencies to {dependency_folder}/{json_file_name}")

    with open(json_file_path, "rb") as data:

        output_blob_client = file_system_client.get_file_client(file_path=f"{dependency_folder}/{json_file_name}")

        output_blob_client.upload_data(data)

        logger.info(f"Uploaded dependencies to {dependency_folder}/{json_file_name}")

    # Clean up the temporary folder
    shutil.rmtree(meta_temp_folder_path)


if __name__ == "__main__":
    pipeline("AI-READI")
