"""Process Fitness tracker data files"""

import garmin.Garmin_Read_Sleep as garmin_read_sleep
import garmin.Garmin_Read_Activity as garmin_read_activity
import garmin.standard_heart_rate as garmin_standardize_heart_rate
import garmin.standard_oxygen_saturation as garmin_standardize_oxygen_saturation
import garmin.standard_physical_activities as garmin_standardize_physical_activities
import garmin.standard_physical_activity_calorie as garmin_standardize_physical_activity_calories
import garmin.standard_respiratory_rate as garmin_standardize_respiratory_rate
import garmin.standard_sleep_stages as garmin_standardize_sleep_stages
import garmin.standard_stress as garmin_standardize_stress
import garmin.metadata as garmin_metadata
from garmin.garmin_sanity import sanity_check_garmin_file
from garmin.garmin_deduplicate import deduplicate_garmin_folder


import argparse
import os
import tempfile
import shutil
import contextlib
import time
from traceback import format_exc
import sys

import azure.storage.filedatalake as azurelake
import config
import utils.dependency as deps
import csv
import utils.logwatch as logging
from utils.file_map_processor import FileMapProcessor
from utils.time_estimator import TimeEstimator
from functools import partial
from multiprocessing.pool import Pool
import threading
import json

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

"""
IMPORTANT: COPY THE RAW DATA FROM THE PRODUCTION 'xx-pilot' CONTAINER TO THE POOLED-DATA 'FitnessTracker' CONTAINER
"""

overall_time_estimator = TimeEstimator(1)  # default to 1 for now


def progress_monitor(progress_file, total_files, start_time, stop_event):
    """Monitor overall progress and provide periodic updates"""
    last_progress = 0
    while not stop_event.is_set():
        try:
            # Read progress from file
            if os.path.exists(progress_file):
                with open(progress_file, "r") as f:
                    progress_data = json.load(f)
                    current_progress = progress_data.get("completed", 0)
            else:
                current_progress = 0

            if current_progress > last_progress:
                percentage = (current_progress / total_files) * 100
                elapsed_time = time.time() - start_time

                if current_progress > 0:
                    avg_time_per_file = elapsed_time / current_progress
                    remaining_files = total_files - current_progress
                    estimated_remaining_time = remaining_files * avg_time_per_file

                    print(
                        f"\n📊 Overall Progress: {current_progress}/{total_files} ({percentage:.1f}%)"
                    )
                    print(
                        f"⏱️  Elapsed: {elapsed_time / 60:.1f} min | Estimated Remaining: {estimated_remaining_time / 60:.1f} min"
                    )
                    print(
                        f"📈 Rate: {current_progress / elapsed_time * 60:.1f} files/min"
                    )

                last_progress = current_progress
        except Exception:
            # Ignore errors reading progress file
            pass

        time.sleep(3)  # Update every 3 seconds


def worker(
    processed_data_output_folder,
    file_paths: list,
    worker_id: int,
    progress_file=None,
):  # sourcery skip: low-code-quality
    """This function handles the work done by the worker threads,
    and contains core operations: downloading, processing, and uploading files."""

    # Create a local time estimator for this worker process
    local_time_estimator = TimeEstimator(1)

    logger = logging.Logwatch(
        "fitness_tracker",
        print=True,
        thread_id=worker_id,
        overall_time_estimator=local_time_estimator,
    )

    # Create local instances of shared objects for this worker process
    local_workflow_dependencies = deps.WorkflowFileDependencies()
    local_manifest = garmin_metadata.GarminManifest(processed_data_output_folder)

    # Results to return from this worker process
    worker_results = {
        "dependencies": [],
        "manifest_updates": [],
        "file_processor_updates": [],
        "processed_files": [],
    }

    # Get the list of blobs in the input folder
    # file_system_client = azurelake.FileSystemClient.from_connection_string(
    #     config.AZURE_STORAGE_CONNECTION_STRING,
    #     file_system_name="stage-1-container",
    # )

    total_files = len(file_paths)
    local_time_estimator = TimeEstimator(total_files)

    for patient_folder in file_paths:
        patient_id = patient_folder["patient_id"]

        logger.info(f"Processing {patient_id}")

        timezone = "pst"
        if patient_id.startswith("7"):
            timezone = "cst"

        patient_folder_path = patient_folder["folder_path"]
        patient_folder_name = patient_folder["patient_folder_name"]

        workflow_input_files = [patient_folder_path]

        # get the file name from the path
        # file_name = patient_folder_name

        # download the file to the temp folder
        # input_file_client = file_system_client.get_file_client(
        #     file_path=patient_folder_path
        # )

        # input_last_modified = input_file_client.get_file_properties().last_modified
        input_last_modified = time.time()

        # For multiprocessing, we'll process all files and let the main process handle deduplication
        should_process = True

        if not should_process:
            logger.debug(
                f"The file {patient_folder_path} has not been modified since the last time it was processed",
            )
            logger.debug(f"Skipping {patient_folder_path} - File has not been modified")

            logger.time(local_time_estimator.step())
            continue

        # Track file processing for results
        file_processing_info = {
            "patient_folder_path": patient_folder_path,
            "input_last_modified": input_last_modified,
            "errors": [],
        }

        # Create a temporary folder on the local machine
        with tempfile.TemporaryDirectory(prefix="garmin_pipeline_") as temp_folder_path:
            temp_input_folder = os.path.join(temp_folder_path, patient_folder_name)
            # os.makedirs(temp_input_folder, exist_ok=True)

            # download_path = os.path.join(temp_input_folder, "raw_data.zip")

            # copy the folder to the temp folder
            shutil.copytree(patient_folder_path, temp_input_folder)

            # logger.debug(f"Downloading {file_name} to {download_path}")

            # with open(file=download_path, mode="wb") as f:
            # f.write(input_file_client.download_file().readall())

            # logger.info(f"Downloaded {file_name} to {download_path}")

            # Deduplicate the folder
            # logger.info(f"Extracting and deduplicating {download_path}")
            try:
                deduplicate_garmin_folder(temp_input_folder, logger=logger)
                logger.info(f"Successfully deduplicated {temp_input_folder}")
            except Exception as e:
                logger.error(f"Error during deduplication of {temp_input_folder}: {e}")
                # Fallback to original folder structure
                logger.warning("Falling back to original folder structure")

            # Create a modality list
            patient_files = []
            total_patient_files = 0

            for root, _, files in os.walk(temp_input_folder):
                for file in files:
                    full_file_path = os.path.join(root, file)

                    if "activity" in full_file_path.lower():
                        file_extension = full_file_path.split(".")[-1]
                        if file_extension == "fit":
                            total_patient_files += 1
                            patient_files.append(
                                {"file_path": full_file_path, "modality": "Activity"}
                            )
                    elif "monitor" in full_file_path.lower():
                        file_extension = full_file_path.split(".")[-1]
                        if file_extension == "FIT":
                            total_patient_files += 1
                            patient_files.append(
                                {"file_path": full_file_path, "modality": "Monitor"}
                            )
                    elif "sleep" in full_file_path.lower():
                        file_extension = full_file_path.split(".")[-1]
                        if file_extension == "fit":
                            total_patient_files += 1
                            patient_files.append(
                                {"file_path": full_file_path, "modality": "Sleep"}
                            )

            logger.debug(
                f"Number of valid files in {temp_input_folder}: {total_patient_files}"
            )

            temp_conversion_output_folder_path = os.path.join(
                temp_folder_path, "converted"
            )

            for idx, patient_file in enumerate(patient_files):
                file_idx = idx + 1

                patient_file_path = patient_file["file_path"]
                file_modality = patient_file["modality"]

                workflow_input_files.append(patient_file_path)

                original_file_name = os.path.basename(patient_file_path)
                original_file_name_only = original_file_name.split(".")[0]

                converted_output_folder_path = os.path.join(
                    temp_conversion_output_folder_path, original_file_name_only
                )

                logger.debug(
                    f"Converting {file_modality}/{original_file_name} - ({file_idx}/{total_patient_files})"
                )

                if file_modality == "Sleep":
                    try:
                        garmin_read_sleep.convert(
                            patient_file_path, converted_output_folder_path
                        )

                        logger.info(
                            f"Converted {file_modality}/{original_file_name} - ({file_idx}/{total_patient_files})"
                        )

                    except Exception:
                        logger.error(
                            f"Failed to convert {file_modality}/{original_file_name} - ({file_idx}/{total_patient_files})"
                        )
                        error_exception = format_exc()
                        error_exception = "".join(error_exception.splitlines())

                        logger.error(error_exception)

                        file_processing_info["errors"].append(error_exception)
                        continue
                elif file_modality in ["Activity", "Monitor"]:
                    try:
                        garmin_read_activity.convert(
                            patient_file_path, converted_output_folder_path
                        )

                        logger.info(
                            f"Converted {file_modality}/{original_file_name} - ({file_idx}/{total_patient_files})"
                        )

                    except Exception:
                        logger.error(
                            f"Failed to convert {file_modality}/{original_file_name} - ({file_idx}/{total_patient_files})"
                        )
                        error_exception = format_exc()
                        error_exception = "".join(error_exception.splitlines())

                        logger.error(error_exception)

                        file_processing_info["errors"].append(error_exception)
                        continue
                else:
                    logger.info(
                        f"Skipping {file_modality}/{original_file_name} - ({file_idx}/{total_patient_files})"
                    )
                    continue

            output_files = []

            try:
                logger.info(f"Standardizing heart rate for {patient_id}")

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
                    timezone,
                )

                logger.info(f"Standardized heart rate for {patient_id}")
                with contextlib.suppress(Exception):
                    shutil.rmtree(heart_rate_jsons_output_folder)

                logger.debug(f"Generating manifest for heart rate for {patient_id}")
                local_manifest.process_heart_rate(final_heart_rate_output_folder)
                logger.info(f"Generated manifest for heart rate for {patient_id}")

                logger.debug(f"Calculating sensor sampling duration for {patient_id}")
                local_manifest.calculate_sensor_sampling_duration(
                    final_heart_rate_output_folder
                )
                logger.info(f"Calculated sensor sampling duration for {patient_id}")

                # list the contents of the final heart rate folder
                for root, _, files in os.walk(final_heart_rate_output_folder):
                    for file in files:
                        file_path = os.path.join(root, file)

                        logger.info(
                            f"Adding {file_path} to the output files for {patient_id}"
                        )

                        output_files.append(
                            {
                                "file_to_upload": file_path,
                                "uploaded_file_path": os.path.join(
                                    processed_data_output_folder,
                                    "heart_rate",
                                    "garmin_vivosmart5",
                                    patient_id,
                                    file,
                                ),
                            }
                        )
            except Exception:
                logger.error(f"Failed to process heart rate for {patient_id} ")
                error_exception = format_exc()
                error_exception = "".join(error_exception.splitlines())

                logger.error(error_exception)

                file_processing_info["errors"].append(error_exception)
                continue

            try:
                logger.info(f"Standardizing oxygen saturation for {patient_id}")

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
                    timezone,
                )

                logger.info(f"Standardized oxygen saturation for {patient_id}")
                with contextlib.suppress(Exception):
                    shutil.rmtree(oxygen_saturation_jsons_output_folder)

                logger.debug(
                    f"Generating manifest for oxygen saturation for {patient_id}"
                )
                local_manifest.process_oxygen_saturation(
                    final_oxygen_saturation_output_folder
                )
                logger.info(
                    f"Generated manifest for oxygen saturation for {patient_id}"
                )

                # list the contents of the final oxygen saturation folder
                for root, dirs, files in os.walk(final_oxygen_saturation_output_folder):
                    for file in files:
                        file_path = os.path.join(root, file)

                        logger.info(
                            f"Adding {file_path} to the output files for {patient_id}"
                        )

                        output_files.append(
                            {
                                "file_to_upload": file_path,
                                "uploaded_file_path": os.path.join(
                                    processed_data_output_folder,
                                    "oxygen_saturation",
                                    "garmin_vivosmart5",
                                    patient_id,
                                    file,
                                ),
                            }
                        )
            except Exception:
                logger.error(
                    f"Failed to standardize oxygen saturation for {patient_id}"
                )
                error_exception = format_exc()
                error_exception = "".join(error_exception.splitlines())

                logger.error(error_exception)

                file_processing_info["errors"].append(error_exception)

                logger.time(local_time_estimator.step())
                continue

            try:
                logger.info(f"Standardizing physical activities for {patient_id}")

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
                    timezone,
                )

                logger.info(f"Standardized physical activities for {patient_id}")
                with contextlib.suppress(Exception):
                    shutil.rmtree(physical_activities_jsons_output_folder)

                logger.debug(
                    f"Generating manifest for physical activities for {patient_id}"
                )
                local_manifest.process_activity(final_physical_activities_output_folder)
                logger.info(
                    f"Generated manifest for physical activities for {patient_id}"
                )

                # list the contents of the final physical activities folder
                for root, dirs, files in os.walk(
                    final_physical_activities_output_folder
                ):
                    for file in files:
                        file_path = os.path.join(root, file)

                        logger.info(
                            f"Adding {file_path} to the output files for {patient_id}"
                        )

                        output_files.append(
                            {
                                "file_to_upload": file_path,
                                "uploaded_file_path": os.path.join(
                                    processed_data_output_folder,
                                    "physical_activity",
                                    "garmin_vivosmart5",
                                    patient_id,
                                    file,
                                ),
                            }
                        )
            except Exception:
                logger.error(
                    f"Failed to standardize physical activities for {patient_id}"
                )
                error_exception = format_exc()
                error_exception = "".join(error_exception.splitlines())

                logger.error(error_exception)

                file_processing_info["errors"].append(error_exception)

                logger.time(local_time_estimator.step())
                continue

            try:
                logger.info(
                    f"Standardizing physical activity calories for {patient_id}"
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
                    timezone,
                )
                logger.info(f"Standardized physical activity calories for {patient_id}")
                with contextlib.suppress(Exception):
                    shutil.rmtree(physical_activity_calories_jsons_output_folder)

                logger.debug(
                    f"Generating manifest for physical activity calories for {patient_id}"
                )
                local_manifest.process_calories(
                    final_physical_activity_calories_output_folder
                )
                logger.info(
                    f"Generated manifest for physical activity calories for {patient_id}"
                )

                # list the contents of the final physical activity calories folder
                for root, dirs, files in os.walk(
                    final_physical_activity_calories_output_folder
                ):
                    for file in files:
                        file_path = os.path.join(root, file)

                        logger.info(
                            f"Adding {file_path} to the output files for {patient_id}"
                        )

                        output_files.append(
                            {
                                "file_to_upload": file_path,
                                "uploaded_file_path": os.path.join(
                                    processed_data_output_folder,
                                    "physical_activity_calorie",
                                    "garmin_vivosmart5",
                                    patient_id,
                                    file,
                                ),
                            }
                        )
            except Exception:
                logger.error(
                    f"Failed to standardize physical activity calories for {patient_id}"
                )
                error_exception = format_exc()
                error_exception = "".join(error_exception.splitlines())

                logger.error(error_exception)

                file_processing_info["errors"].append(error_exception)

                logger.time(local_time_estimator.step())
                continue

            try:
                logger.info(f"Standardizing respiratory rate for {patient_id}")

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
                    timezone,
                )

                logger.info(f"Standardized respiratory rate for {patient_id}")
                with contextlib.suppress(Exception):
                    shutil.rmtree(respiratory_rate_jsons_output_folder)

                logger.debug(
                    f"Generating manifest for respiratory rate for {patient_id}"
                )
                local_manifest.process_respiratory_rate(
                    final_respiratory_rate_output_folder
                )
                logger.info(f"Generated manifest for respiratory rate for {patient_id}")

                # list the contents of the final respiratory rate folder
                for root, dirs, files in os.walk(final_respiratory_rate_output_folder):
                    for file in files:
                        file_path = os.path.join(root, file)

                        logger.info(
                            f"Adding {file_path} to the output files for {patient_id}"
                        )

                        output_files.append(
                            {
                                "file_to_upload": file_path,
                                "uploaded_file_path": os.path.join(
                                    processed_data_output_folder,
                                    "respiratory_rate",
                                    "garmin_vivosmart5",
                                    patient_id,
                                    file,
                                ),
                            }
                        )
            except Exception:
                logger.error(f"Failed to standardize respiratory rate for {patient_id}")
                error_exception = format_exc()
                error_exception = "".join(error_exception.splitlines())

                logger.error(error_exception)

                file_processing_info["errors"].append(error_exception)

                logger.time(local_time_estimator.step())
                continue

            try:
                logger.info(f"Standardizing sleep stages for {patient_id}")

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
                    timezone,
                )

                logger.info(f"Standardized sleep stages for {patient_id}")
                with contextlib.suppress(Exception):
                    shutil.rmtree(sleep_stages_jsons_output_folder)

                logger.debug(f"Generating manifest for sleep stages for {patient_id}")
                local_manifest.process_sleep(final_sleep_stages_output_folder)
                logger.info(f"Generated manifest for sleep stages for {patient_id}")

                for root, dirs, files in os.walk(final_sleep_stages_output_folder):
                    for file in files:
                        file_path = os.path.join(root, file)

                        logger.info(
                            f"Adding {file_path} to the output files for {patient_id}"
                        )

                        output_files.append(
                            {
                                "file_to_upload": file_path,
                                "uploaded_file_path": os.path.join(
                                    processed_data_output_folder,
                                    "sleep",
                                    "garmin_vivosmart5",
                                    patient_id,
                                    file,
                                ),
                            }
                        )
            except Exception:
                logger.error(f"Failed to standardize sleep stages for {patient_id}")
                error_exception = format_exc()
                error_exception = "".join(error_exception.splitlines())

                logger.error(error_exception)

                file_processing_info["errors"].append(error_exception)

                logger.time(local_time_estimator.step())
                continue

            try:
                logger.info(f"Standardizing stress for {patient_id}")

                stress_jsons_output_folder = os.path.join(
                    temp_folder_path, "stress_jsons"
                )
                final_stress_output_folder = os.path.join(
                    temp_folder_path, "final_stress"
                )

                garmin_standardize_stress.standardize_stress(
                    temp_conversion_output_folder_path,
                    patient_id,
                    stress_jsons_output_folder,
                    final_stress_output_folder,
                    timezone,
                )

                logger.info(f"Standardized stress for {patient_id}")
                with contextlib.suppress(Exception):
                    shutil.rmtree(stress_jsons_output_folder)

                logger.debug(f"Generating manifest for stress for {patient_id}")
                local_manifest.process_stress(final_stress_output_folder)
                logger.info(f"Generated manifest for stress for {patient_id}")

                # list the contents of the final stress folder
                for root, dirs, files in os.walk(final_stress_output_folder):
                    for file in files:
                        file_path = os.path.join(root, file)

                        logger.info(
                            f"Adding {file_path} to the output files for {patient_id}"
                        )

                        output_files.append(
                            {
                                "file_to_upload": file_path,
                                "uploaded_file_path": os.path.join(
                                    processed_data_output_folder,
                                    "stress",
                                    "garmin_vivosmart5",
                                    patient_id,
                                    file,
                                ),
                            }
                        )
            except Exception:
                logger.error(f"Failed to standardize stress for {patient_id}")
                error_exception = format_exc()
                error_exception = "".join(error_exception.splitlines())

                logger.error(error_exception)

                file_processing_info["errors"].append(error_exception)

                logger.time(local_time_estimator.step())
                continue

            patient_folder["convert_error"] = False
            patient_folder["processed"] = True

            workflow_output_files = []

            outputs_uploaded = True

            # Note: File deletion will be handled by the main process

            total_output_files = len(output_files)

            logger.info(f"Uploading {total_output_files} output files for {patient_id}")

            summary_list = []

            for idx3, file in enumerate(output_files):
                _ = idx3 + 1

                f_path = file["file_to_upload"]
                f_name = os.path.basename(f_path)

                output_file_path = file["uploaded_file_path"]

                logger.debug(f"Sanity checking {f_name}")

                # logger.debug(
                #     f"Uploading {f_name} to {output_file_path} - ({log_idx}/{total_output_files})"
                # )

                try:
                    # Check if the file exists on the file system
                    if not os.path.exists(f_path):
                        logger.error(f"File {f_path} does not exist")
                        continue

                    summary = sanity_check_garmin_file(f_path)

                    summary_list.append(
                        {
                            "file_name": f_name,
                            "summary": summary,
                        }
                    )

                    # Check if the file already exists in the output folder
                    # output_file_client = file_system_client.get_file_client(
                    #    file_path=output_file_path
                    # )

                    if os.path.exists(output_file_path):
                        logger.error(f"File {output_file_path} already exists")
                        raise Exception(f"File {output_file_path} already exists")

                    # Create the output folder if it doesn't exist
                    os.makedirs(os.path.dirname(output_file_path), exist_ok=True)

                    # Copy the file to the output folder
                    shutil.copy(f_path, output_file_path)

                except Exception:
                    outputs_uploaded = False
                    logger.error(f"Failed to copy {file} to {output_file_path}")
                    error_exception = format_exc()
                    error_exception = "".join(error_exception.splitlines())

                    logger.error(error_exception)

                    file_processing_info["errors"].append(error_exception)
                    continue

                patient_folder["output_files"].append(output_file_path)
                workflow_output_files.append(output_file_path)

            # Store additional data for later processing
            file_processing_info["additional_data"] = summary_list

            # Store output files info for later processing
            file_processing_info["output_files"] = [
                file["uploaded_file_path"] for file in output_files
            ]

            if outputs_uploaded:
                patient_folder["output_uploaded"] = True
                patient_folder["status"] = "success"
                logger.info(
                    f"Uploaded outputs of {original_file_name} to {processed_data_output_folder}"
                )
            else:
                logger.error(
                    f"Failed to upload outputs of {original_file_name} to {processed_data_output_folder}"
                )

            local_workflow_dependencies.add_dependency(
                workflow_input_files, workflow_output_files
            )

            logger.time(local_time_estimator.step())

            # Add processed file info to results
            worker_results["processed_files"].append(file_processing_info)

            # Update overall progress counter
            if progress_file is not None:
                try:
                    # Read current progress
                    if os.path.exists(progress_file):
                        with open(progress_file, "r") as f:
                            progress_data = json.load(f)
                    else:
                        progress_data = {"completed": 0}

                    # Update progress
                    progress_data["completed"] = progress_data.get("completed", 0) + 1

                    # Write back to file
                    with open(progress_file, "w") as f:
                        json.dump(progress_data, f)

                    logger.info(f"Completed file - Worker {worker_id}")
                except Exception:
                    # Ignore errors updating progress file
                    pass

    # Return results from this worker process
    worker_results["dependencies"] = local_workflow_dependencies.dependencies
    return worker_results


def pipeline(study_id: str, workers: int = 4, args: list = None):
    """The function contains the work done by
    the main thread, which runs only once for each operation."""

    if args is None:
        args = []

    global overall_time_estimator

    # Process cirrus data files for a study. Args:study_id (str): the study id
    if study_id is None or not study_id:
        raise ValueError("study_id is required")

    # input_folder = f"{study_id}/pooled-data/FitnessTracker"
    input_folder = os.path.join(os.path.expanduser("~"), "Downloads", "FitnessTracker")
    # processed_data_output_folder = f"{study_id}/pooled-data/FitnessTracker-processed"
    processed_data_output_folder = os.path.join(
        os.path.expanduser("~"), "Downloads", "FitnessTracker-processed"
    )
    manifest_folder = f"{study_id}/pooled-data/FitnessTracker-manifest"
    dependency_folder = f"{study_id}/dependency/FitnessTracker"

    pipeline_workflow_log_folder = f"{study_id}/logs/FitnessTracker"
    ignore_file = f"{study_id}/ignore/fitnessTracker.ignore"
    manual_input_folder = f"{study_id}/pooled-data/FitnessTracker-manual"
    red_cap_export_file = (
        f"{study_id}/pooled-data/REDCap/AIREADiPilot-2024Sep13_EnviroPhysSensorInfo.csv"
    )
    participant_filter_list_file = f"{study_id}/dependency/PatientID/AllParticipantIDs07-01-2023through05-01-2025.csv"
    logger = logging.Logwatch("fitness_tracker", print=True)

    # dev_allowed_list = ["1025", "7060", "4233", "1081", "4033", "4077", "7352"]

    # Get the list of blobs in the input folder
    file_system_client = azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_CONNECTION_STRING,
        file_system_name="stage-1-container",
    )

    # with contextlib.suppress(Exception):
    #     file_system_client.delete_directory(processed_data_output_folder)

    if os.path.exists(processed_data_output_folder):
        shutil.rmtree(processed_data_output_folder)

    with contextlib.suppress(Exception):
        file_system_client.delete_file(f"{dependency_folder}/file_map.json")

    file_paths = []
    participant_filter_list = []

    # Create a temporary folder on the local machine
    meta_temp_folder_path = tempfile.mkdtemp(prefix="garmin_pipeline_meta_")

    # Get the participant filter list file
    with contextlib.suppress(Exception):
        file_client = file_system_client.get_file_client(
            file_path=participant_filter_list_file
        )

        temp_participant_filter_list_file = os.path.join(
            meta_temp_folder_path, "filter_file.csv"
        )

        with open(file=temp_participant_filter_list_file, mode="wb") as f:
            f.write(file_client.download_file().readall())

        with open(file=temp_participant_filter_list_file, mode="r") as f:
            reader = csv.reader(f)
            for row in reader:
                participant_filter_list.append(row[0])

        # remove the first row
        participant_filter_list.pop(0)

    # paths = file_system_client.get_paths(path=input_folder)
    paths = os.listdir(input_folder)

    logger.debug(f"Getting file paths in {input_folder}")

    file_processor = FileMapProcessor(dependency_folder, ignore_file, args)

    for path in paths:
        t = str(path)

        folder_name = t
        folder_path = os.path.join(input_folder, folder_name)

        # Check if the folder name is in the format FIT-patientID or FitnessTracker-patientID
        if not folder_name.startswith("FIT-") and not folder_name.startswith(
            "FitnessTracker-"
        ):
            logger.debug(
                f"Skipping {folder_name} because it is not a FIT or FitnessTracker folder"
            )
            continue

        if len(folder_name.split("-")) != 2:
            logger.debug(
                f"Skipping {folder_name} because it does not have the expected format"
            )
            continue

        if file_processor.is_file_ignored(folder_name, t):
            logger.debug(f"Skipping {folder_name}")
            continue

        patient_id = folder_name.split("-")[1].strip()

        # if str(patient_id) not in dev_allowed_list:
        #     print(
        #         f"dev-Participant ID {patient_id} not in the allowed list. Skipping {file_name}"
        #     )
        #     continue

        if str(patient_id) not in participant_filter_list:
            print(
                f"Participant ID {patient_id} not in the allowed list. Skipping {folder_name}"
            )
            continue

        file_paths.append(
            {
                "file_name": folder_name,
                "folder_path": folder_path,
                "status": "failed",
                "processed": False,
                "convert_error": True,
                "output_uploaded": False,
                "output_files": [],
                "patient_folder_name": folder_name,
                "patient_id": patient_id,
            }
        )

    # dev - only process a random 8 files
    # import random

    # file_paths = random.sample(file_paths, 20)

    total_files = len(file_paths)

    logger.info(f"Found {total_files} items in {input_folder}")

    # Create the output folder
    # file_system_client.create_directory(processed_data_output_folder)
    os.makedirs(processed_data_output_folder, exist_ok=True)

    workflow_file_dependencies = deps.WorkflowFileDependencies()

    # Download the redcap export file
    red_cap_export_file_path = os.path.join(meta_temp_folder_path, "redcap_export.tsv")

    red_cap_export_file_client = file_system_client.get_file_client(
        file_path=red_cap_export_file
    )

    with open(red_cap_export_file_path, "wb") as data:
        red_cap_export_file_client.download_file().readinto(data)

    total_files = len(file_paths)

    manifest = garmin_metadata.GarminManifest(processed_data_output_folder)

    manifest.read_redcap_file(red_cap_export_file_path)

    overall_time_estimator = TimeEstimator(total_files)

    # Create progress tracking file
    progress_file = os.path.join(meta_temp_folder_path, "progress.json")
    with open(progress_file, "w") as f:
        json.dump({"completed": 0}, f)

    # Guarantees that all paths are considered, even if the number of items is not evenly divisible by workers.
    chunk_size = (len(file_paths) + workers - 1) // workers
    # Comprehension that fills out and pass to worker func final 2 args: chunks and worker_id
    chunks = [file_paths[i : i + chunk_size] for i in range(0, total_files, chunk_size)]
    args = [(chunk, index + 1, progress_file) for index, chunk in enumerate(chunks)]
    pipe = partial(
        worker,
        processed_data_output_folder,
    )

    # Start progress monitoring
    start_time = time.time()
    stop_event = threading.Event()
    progress_thread = threading.Thread(
        target=progress_monitor,
        args=(progress_file, total_files, start_time, stop_event),
    )
    progress_thread.daemon = True
    progress_thread.start()

    print(f"\n🚀 Starting processing with {workers} workers for {total_files} files...")
    print("📊 Progress updates every 10 seconds\n")

    # Process pool created
    pool = Pool(workers)
    try:
        # Distributes the pipe function across the processes in the pool
        worker_results = pool.starmap(pipe, args)
    finally:
        pool.close()
        pool.join()
        stop_event.set()  # Stop progress monitoring
        progress_thread.join(timeout=1)

    # Merge results from all worker processes
    for result in worker_results:
        # Merge dependencies
        for dep in result["dependencies"]:
            workflow_file_dependencies.add_dependency(
                dep["input_files"], dep["output_files"]
            )

        # Update file processor with results from workers
        for file_info in result["processed_files"]:
            file_processor.add_entry(
                file_info["patient_folder_path"],
                file_info["input_last_modified"],
                file_info.get("additional_data"),
            )

            # Add any errors that occurred
            for error in file_info.get("errors", []):
                file_processor.append_errors(error, file_info["patient_folder_path"])

            # Confirm output files
            if "output_files" in file_info:
                file_processor.confirm_output_files(
                    file_info["patient_folder_path"],
                    file_info["output_files"],
                    file_info["input_last_modified"],
                )

    # Note: Manifest data is handled per-process and doesn't need merging
    # as each process works on different files and the manifest is written
    # to the same output folder

    # Print final summary
    total_elapsed_time = time.time() - start_time
    print("\n✅ Processing completed!")
    print(f"⏱️  Total time: {total_elapsed_time / 60:.1f} minutes")
    print(f"📈 Average rate: {total_files / total_elapsed_time * 60:.1f} files/min")
    print(f"👥 Used {workers} worker processes")

    file_processor.delete_out_of_date_output_files()
    file_processor.remove_seen_flag_from_map()

    # Write the manifest to a file
    manifest_file_path = os.path.join(meta_temp_folder_path, "manifest.tsv")

    manifest.write_tsv(manifest_file_path)

    logger.debug(
        f"Uploading manifest file to {processed_data_output_folder}/manifest.tsv"
    )

    # Upload the manifest file
    with open(manifest_file_path, "rb") as data:
        output_file_client = file_system_client.get_file_client(
            file_path=f"{manifest_folder}/manifest.tsv"
        )

        output_file_client.upload_data(data, overwrite=True)

    logger.info(
        f"Uploaded manifest file to {processed_data_output_folder}/manifest.tsv"
    )

    # Move any manual files to the destination folder
    logger.debug(f"Getting manual file paths in {manual_input_folder}")

    manual_input_folder_contents = file_system_client.get_paths(
        path=manual_input_folder, recursive=True
    )

    with tempfile.TemporaryDirectory(
        prefix="FitnessTracker_pipeline_manual_"
    ) as manual_temp_folder_path:
        for item in manual_input_folder_contents:
            item_path = str(item.name)

            file_name = os.path.basename(item_path)

            # Remove the manual input folder prefix from the path
            if item_path.startswith(f"{manual_input_folder}/"):
                clipped_path = item_path[len(f"{manual_input_folder}/") :]
            else:
                clipped_path = os.path.basename(item_path)

            manual_input_file_client = file_system_client.get_file_client(
                file_path=item_path
            )

            file_properties = manual_input_file_client.get_file_properties().metadata

            # Check if the file is a directory
            if file_properties.get("hdi_isfolder"):
                continue

            logger.debug(f"Moving {item_path} to {processed_data_output_folder}")

            # Download the file to the temp folder
            download_path = os.path.join(manual_temp_folder_path, file_name)

            logger.debug(f"Downloading {item_path} to {download_path}")

            with open(file=download_path, mode="wb") as f:
                f.write(manual_input_file_client.download_file().readall())

            # Upload the file to the processed data output folder
            upload_path = f"{processed_data_output_folder}/{clipped_path}"

            logger.debug(f"Uploading {item_path} to {upload_path}")

            output_file_client = file_system_client.get_file_client(
                file_path=upload_path,
            )

            # Check if the file already exists. If it does, throw an exception
            if output_file_client.exists():
                raise Exception(
                    f"File {upload_path} already exists. Throwing exception"
                )

            with open(file=download_path, mode="rb") as f:
                output_file_client.upload_data(f, overwrite=True)
                logger.info(f"Uploaded {item_path} to {upload_path}")

            os.remove(download_path)

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
            "folder_path",
            "status",
            "processed",
            "convert_error",
            "output_uploaded",
            "output_files",
            "patient_folder_name",
            "patient_id",
            "file_name",
        ]

        writer = csv.DictWriter(f, fieldnames=fieldnames)

        for file_item in file_paths:
            file_item["output_files"] = ";".join(file_item["output_files"])
            writer.writerow(file_item)

        writer.writeheader()
        writer.writerows(file_paths)

    with open(workflow_log_file_path, mode="rb") as data:
        logger.debug(
            f"Uploading workflow log to {pipeline_workflow_log_folder}/{file_name}"
        )

        output_blob_client = file_system_client.get_file_client(
            file_path=f"{pipeline_workflow_log_folder}/{file_name}"
        )

        output_blob_client.upload_data(data, overwrite=True)

        logger.info(
            f"Uploaded workflow log to {pipeline_workflow_log_folder}/{file_name}"
        )

    # Write the dependencies to a file
    deps_output = workflow_file_dependencies.write_to_file(meta_temp_folder_path)

    json_file_path = deps_output["file_path"]
    json_file_name = deps_output["file_name"]

    logger.debug(
        f"Uploading dependencies to {dependency_folder}/file_dependencies/{json_file_name}"
    )

    with open(json_file_path, "rb") as data:

        output_blob_client = file_system_client.get_file_client(
            file_path=f"{dependency_folder}/file_dependencies/{json_file_name}"
        )

        output_blob_client.upload_data(data, overwrite=True)

        logger.info(
            f"Uploaded dependencies to {dependency_folder}/file_dependencies/{json_file_name}"
        )

    # Clean up the temporary folder
    shutil.rmtree(meta_temp_folder_path)


if __name__ == "__main__":
    sys_args = sys.argv

    workers = 8

    parser = argparse.ArgumentParser(description="Process garmin data files")
    parser.add_argument(
        "--workers", type=int, default=workers, help="Number of workers to use"
    )
    args = parser.parse_args()

    workers = args.workers

    print(f"Using {workers} workers to process garmin data files")

    pipeline(study_id="AI-READI", workers=workers, args=sys_args)
