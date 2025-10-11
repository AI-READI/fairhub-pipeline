"""Process Fitness tracker data files locally with Azure logging"""

import zipfile
import os
import tempfile
import shutil
import contextlib
import time
from traceback import format_exc
import sys
import csv
import argparse
from functools import partial
from multiprocessing.pool import ThreadPool

import azure.storage.filedatalake as azurelake
import config
import utils.dependency as deps
import utils.logwatch as logging
from utils.file_map_processor import FileMapProcessor
from utils.time_estimator import TimeEstimator

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
from garmin.garmin_deduplicate import deduplicate_garmin_zip


overall_time_estimator = TimeEstimator(1)  # default to 1 for now


def worker(
    workflow_file_dependencies,
    file_processor,
    processed_data_output_folder,
    manifest,
    file_paths: list,
    worker_id: int,
):  # sourcery skip: low-code-quality
    """This function handles the work done by the worker threads,
    and contains core operations: processing files locally with Azure logging."""

    logger = logging.Logwatch(
        "fitness_tracker",
        print=True,
        thread_id=worker_id,
        local=True,
        overall_time_estimator=overall_time_estimator,
    )

    # Azure file system client available for logging if needed
    # file_system_client = azurelake.FileSystemClient.from_connection_string(
    #     config.AZURE_STORAGE_CONNECTION_STRING,
    #     file_system_name="stage-1-container",
    # )

    # Only do 100 files for testing
    file_paths = file_paths[:100]

    total_files = len(file_paths)
    time_estimator = TimeEstimator(total_files)

    for patient_folder in file_paths:
        patient_id = patient_folder["patient_id"]

        logger.info(f"Processing {patient_id}")

        timezone = "pst"
        if patient_id.startswith("7"):
            timezone = "cst"

        patient_folder_path = patient_folder["file_path"]
        patient_folder_name = patient_folder["patient_folder_name"]

        workflow_input_files = [patient_folder_path]

        # Check if the input file exists
        if not os.path.exists(patient_folder_path):
            logger.error(f"Input file {patient_folder_path} does not exist")
            continue

        # Get file modification time
        input_last_modified = os.path.getmtime(patient_folder_path)

        should_process = file_processor.file_should_process(
            patient_folder_path, input_last_modified
        )

        if not should_process:
            logger.debug(
                f"The file {patient_folder_path} has not been modified since the last time it was processed",
            )
            logger.debug(f"Skipping {patient_folder_path} - File has not been modified")
            logger.time(time_estimator.step())
            continue

        file_processor.add_entry(patient_folder_path, input_last_modified)
        file_processor.clear_errors(patient_folder_path)

        # Create a temporary folder on the local machine
        with tempfile.TemporaryDirectory(prefix="garmin_pipeline_") as temp_folder_path:
            temp_input_folder = os.path.join(temp_folder_path, patient_folder_name)
            os.makedirs(temp_input_folder, exist_ok=True)

            # Copy the zip file to temp folder
            download_path = os.path.join(temp_input_folder, "raw_data.zip")
            shutil.copy2(patient_folder_path, download_path)

            logger.info(f"Copied {patient_folder_path} to {download_path}")

            # Deduplicate Monitor FIT files in the ZIP before unzipping
            logger.info(f"Deduplicating Monitor FIT files in {download_path}")
            try:
                success = deduplicate_garmin_zip(download_path, logger)
                if success:
                    logger.info(f"Successfully deduplicated {download_path}")
                else:
                    logger.warning(
                        f"Deduplication failed for {download_path}, continuing with original file"
                    )
            except Exception as e:
                logger.error(f"Error during deduplication of {download_path}: {e}")
                logger.warning("Continuing with original file")

            logger.debug(f"Unzipping {download_path} to {temp_input_folder}")

            with zipfile.ZipFile(download_path, "r") as zip_ref:
                zip_ref.extractall(temp_input_folder)

            logger.info(f"Unzipped {download_path} to {temp_input_folder}")

            # Delete the copied file
            os.remove(download_path)

            # Create a modality list
            patient_files = []
            total_patient_files = 0

            for root, dirs, files in os.walk(temp_input_folder):
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

                        file_processor.append_errors(
                            error_exception, patient_folder_path
                        )
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

                        file_processor.append_errors(
                            error_exception, patient_folder_path
                        )
                        continue
                else:
                    logger.info(
                        f"Skipping {file_modality}/{original_file_name} - ({file_idx}/{total_patient_files})"
                    )
                    continue

            output_files = []

            # Process heart rate
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
                manifest.process_heart_rate(final_heart_rate_output_folder)
                logger.info(f"Generated manifest for heart rate for {patient_id}")

                logger.debug(f"Calculating sensor sampling duration for {patient_id}")
                manifest.calculate_sensor_sampling_duration(
                    final_heart_rate_output_folder
                )
                logger.info(f"Calculated sensor sampling duration for {patient_id}")

                # Copy files to final output location
                for root, dirs, files in os.walk(final_heart_rate_output_folder):
                    for file in files:
                        file_path = os.path.join(root, file)
                        output_path = os.path.join(
                            processed_data_output_folder,
                            "heart_rate",
                            "garmin_vivosmart5",
                            patient_id,
                            file,
                        )
                        os.makedirs(os.path.dirname(output_path), exist_ok=True)
                        shutil.copy2(file_path, output_path)

                        logger.info(f"Copied {file_path} to {output_path}")
                        output_files.append(output_path)

            except Exception:
                logger.error(f"Failed to process heart rate for {patient_id} ")
                error_exception = format_exc()
                error_exception = "".join(error_exception.splitlines())
                logger.error(error_exception)
                file_processor.append_errors(error_exception, patient_folder_path)
                continue

            # Process oxygen saturation
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
                manifest.process_oxygen_saturation(
                    final_oxygen_saturation_output_folder
                )
                logger.info(
                    f"Generated manifest for oxygen saturation for {patient_id}"
                )

                # Copy files to final output location
                for root, dirs, files in os.walk(final_oxygen_saturation_output_folder):
                    for file in files:
                        file_path = os.path.join(root, file)
                        output_path = os.path.join(
                            processed_data_output_folder,
                            "oxygen_saturation",
                            "garmin_vivosmart5",
                            patient_id,
                            file,
                        )
                        os.makedirs(os.path.dirname(output_path), exist_ok=True)
                        shutil.copy2(file_path, output_path)

                        logger.info(f"Copied {file_path} to {output_path}")
                        output_files.append(output_path)

            except Exception:
                logger.error(
                    f"Failed to standardize oxygen saturation for {patient_id}"
                )
                error_exception = format_exc()
                error_exception = "".join(error_exception.splitlines())
                logger.error(error_exception)
                file_processor.append_errors(error_exception, patient_folder_path)
                continue

            # Process physical activities
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
                manifest.process_activity(final_physical_activities_output_folder)
                logger.info(
                    f"Generated manifest for physical activities for {patient_id}"
                )

                # Copy files to final output location
                for root, dirs, files in os.walk(
                    final_physical_activities_output_folder
                ):
                    for file in files:
                        file_path = os.path.join(root, file)
                        output_path = os.path.join(
                            processed_data_output_folder,
                            "physical_activity",
                            "garmin_vivosmart5",
                            patient_id,
                            file,
                        )
                        os.makedirs(os.path.dirname(output_path), exist_ok=True)
                        shutil.copy2(file_path, output_path)

                        logger.info(f"Copied {file_path} to {output_path}")
                        output_files.append(output_path)

            except Exception:
                logger.error(
                    f"Failed to standardize physical activities for {patient_id}"
                )
                error_exception = format_exc()
                error_exception = "".join(error_exception.splitlines())
                logger.error(error_exception)
                file_processor.append_errors(error_exception, patient_folder_path)
                continue

            # Process physical activity calories
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
                manifest.process_calories(
                    final_physical_activity_calories_output_folder
                )
                logger.info(
                    f"Generated manifest for physical activity calories for {patient_id}"
                )

                # Copy files to final output location
                for root, dirs, files in os.walk(
                    final_physical_activity_calories_output_folder
                ):
                    for file in files:
                        file_path = os.path.join(root, file)
                        output_path = os.path.join(
                            processed_data_output_folder,
                            "physical_activity_calorie",
                            "garmin_vivosmart5",
                            patient_id,
                            file,
                        )
                        os.makedirs(os.path.dirname(output_path), exist_ok=True)
                        shutil.copy2(file_path, output_path)

                        logger.info(f"Copied {file_path} to {output_path}")
                        output_files.append(output_path)

            except Exception:
                logger.error(
                    f"Failed to standardize physical activity calories for {patient_id}"
                )
                error_exception = format_exc()
                error_exception = "".join(error_exception.splitlines())
                logger.error(error_exception)
                file_processor.append_errors(error_exception, patient_folder_path)
                continue

            # Process respiratory rate
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
                manifest.process_respiratory_rate(final_respiratory_rate_output_folder)
                logger.info(f"Generated manifest for respiratory rate for {patient_id}")

                # Copy files to final output location
                for root, dirs, files in os.walk(final_respiratory_rate_output_folder):
                    for file in files:
                        file_path = os.path.join(root, file)
                        output_path = os.path.join(
                            processed_data_output_folder,
                            "respiratory_rate",
                            "garmin_vivosmart5",
                            patient_id,
                            file,
                        )
                        os.makedirs(os.path.dirname(output_path), exist_ok=True)
                        shutil.copy2(file_path, output_path)

                        logger.info(f"Copied {file_path} to {output_path}")
                        output_files.append(output_path)

            except Exception:
                logger.error(f"Failed to standardize respiratory rate for {patient_id}")
                error_exception = format_exc()
                error_exception = "".join(error_exception.splitlines())
                logger.error(error_exception)
                file_processor.append_errors(error_exception, patient_folder_path)
                continue

            # Process sleep stages
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
                manifest.process_sleep(final_sleep_stages_output_folder)
                logger.info(f"Generated manifest for sleep stages for {patient_id}")

                # Copy files to final output location
                for root, dirs, files in os.walk(final_sleep_stages_output_folder):
                    for file in files:
                        file_path = os.path.join(root, file)
                        output_path = os.path.join(
                            processed_data_output_folder,
                            "sleep",
                            "garmin_vivosmart5",
                            patient_id,
                            file,
                        )
                        os.makedirs(os.path.dirname(output_path), exist_ok=True)
                        shutil.copy2(file_path, output_path)

                        logger.info(f"Copied {file_path} to {output_path}")
                        output_files.append(output_path)

            except Exception:
                logger.error(f"Failed to standardize sleep stages for {patient_id}")
                error_exception = format_exc()
                error_exception = "".join(error_exception.splitlines())
                logger.error(error_exception)
                file_processor.append_errors(error_exception, patient_folder_path)
                continue

            # Process stress
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
                manifest.process_stress(final_stress_output_folder)
                logger.info(f"Generated manifest for stress for {patient_id}")

                # Copy files to final output location
                for root, dirs, files in os.walk(final_stress_output_folder):
                    for file in files:
                        file_path = os.path.join(root, file)
                        output_path = os.path.join(
                            processed_data_output_folder,
                            "stress",
                            "garmin_vivosmart5",
                            patient_id,
                            file,
                        )
                        os.makedirs(os.path.dirname(output_path), exist_ok=True)
                        shutil.copy2(file_path, output_path)

                        logger.info(f"Copied {file_path} to {output_path}")
                        output_files.append(output_path)

            except Exception:
                logger.error(f"Failed to standardize stress for {patient_id}")
                error_exception = format_exc()
                error_exception = "".join(error_exception.splitlines())
                logger.error(error_exception)
                file_processor.append_errors(error_exception, patient_folder_path)
                continue

            patient_folder["convert_error"] = False
            patient_folder["processed"] = True

            workflow_output_files = []

            # Perform sanity checks and log to Azure
            summary_list = []
            for idx3, file_path in enumerate(output_files):
                f_name = os.path.basename(file_path)

                logger.debug(f"Sanity checking {f_name}")

                try:
                    # Check if the file exists on the file system
                    if not os.path.exists(file_path):
                        logger.error(f"File {file_path} does not exist")
                        continue

                    summary = sanity_check_garmin_file(file_path, logger)
                    summary_list.append(
                        {
                            "file_name": f_name,
                            "summary": summary,
                        }
                    )

                except Exception:
                    logger.error(f"Failed to sanity check {file_path}")
                    error_exception = format_exc()
                    error_exception = "".join(error_exception.splitlines())
                    logger.error(error_exception)
                    continue

                workflow_output_files.append(file_path)

            file_processor.add_additional_data(patient_folder_path, summary_list)

            file_processor.confirm_output_files(
                patient_folder_path,
                output_files,
                input_last_modified,
            )

            patient_folder["output_files"] = output_files
            patient_folder["status"] = "success"

            workflow_file_dependencies.add_dependency(
                workflow_input_files, workflow_output_files
            )

            logger.info(
                f"Successfully processed {patient_id} with {len(output_files)} output files"
            )
            logger.time(time_estimator.step())


# Using the real FileMapProcessor from utils


def pipeline_local(
    input_folder: str, output_folder: str, workers: int = 4, study_id: str = "AI-READI"
):
    """Process Garmin data files locally from input folder to output folder with Azure logging"""

    global overall_time_estimator

    # Azure paths for logging and dependencies
    manifest_folder = f"{study_id}/pooled-data/FitnessTracker-manifest"
    dependency_folder = f"{study_id}/dependency/FitnessTracker"
    pipeline_workflow_log_folder = f"{study_id}/logs/FitnessTracker"
    ignore_file = f"{study_id}/ignore/fitnessTracker.ignore"
    red_cap_export_file = (
        f"{study_id}/pooled-data/REDCap/AIREADiPilot-2024Sep13_EnviroPhysSensorInfo.csv"
    )
    participant_filter_list_file = f"{study_id}/dependency/PatientID/AllParticipantIDs07-01-2023through05-01-2025.csv"

    logger = logging.Logwatch("fitness_tracker", print=True)

    # Get the Azure file system client for logging
    file_system_client = azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_CONNECTION_STRING,
        file_system_name="stage-1-container",
    )

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
            participant_filter_list.extend([row[0] for row in reader])

        # remove the first row
        participant_filter_list.pop(0)

    # Scan input folder for zip files
    logger.info(f"Scanning input folder: {input_folder}")

    if not os.path.exists(input_folder):
        logger.error(f"Input folder {input_folder} does not exist")
        return

    for file_name in os.listdir(input_folder):
        if not file_name.endswith(".zip"):
            logger.debug(f"Skipping {file_name} because it is not a .zip file")
            continue

        if len(file_name.split("-")) != 2:
            logger.debug(
                f"Skipping {file_name} because it does not have the expected format"
            )
            continue

        cleaned_file_name = file_name.replace(".zip", "")
        patient_id = cleaned_file_name.split("-")[1]

        if str(patient_id) not in participant_filter_list:
            print(
                f"Participant ID {patient_id} not in the allowed list. Skipping {file_name}"
            )
            continue

        file_path = os.path.join(input_folder, file_name)

        file_paths.append(
            {
                "file_path": file_path,
                "status": "failed",
                "processed": False,
                "convert_error": True,
                "output_uploaded": False,
                "output_files": [],
                "patient_folder_name": cleaned_file_name,
                "patient_id": patient_id,
            }
        )

    total_files = len(file_paths)
    logger.info(f"Found {total_files} files to process in {input_folder}")

    if total_files == 0:
        logger.warning("No files found to process")
        return

    # Create the output folder
    workflow_file_dependencies = deps.WorkflowFileDependencies()

    # Download the redcap export file
    red_cap_export_file_path = os.path.join(meta_temp_folder_path, "redcap_export.tsv")

    red_cap_export_file_client = file_system_client.get_file_client(
        file_path=red_cap_export_file
    )

    with open(red_cap_export_file_path, "wb") as data:
        red_cap_export_file_client.download_file().readinto(data)

    # Create manifest
    manifest = garmin_metadata.GarminManifest(output_folder)
    manifest.read_redcap_file(red_cap_export_file_path)

    # Create file processor
    file_processor = FileMapProcessor(dependency_folder, ignore_file, [])

    overall_time_estimator = TimeEstimator(total_files)

    # Process files using thread pool
    chunk_size = (len(file_paths) + workers - 1) // workers
    chunks = [file_paths[i : i + chunk_size] for i in range(0, total_files, chunk_size)]
    args = [(chunk, index + 1) for index, chunk in enumerate(chunks)]

    pipe = partial(
        worker,
        workflow_file_dependencies,
        file_processor,
        output_folder,
        manifest,
    )

    # Thread pool created
    pool = ThreadPool(workers)
    # Distributes the pipe function across the threads in the pool
    pool.starmap(pipe, args)

    file_processor.delete_out_of_date_output_files()
    file_processor.remove_seen_flag_from_map()

    # Write manifest
    manifest_file_path = os.path.join(meta_temp_folder_path, "manifest.tsv")
    manifest.write_tsv(manifest_file_path)
    logger.info(f"Written manifest to {manifest_file_path}")

    # Upload the manifest file to Azure
    logger.debug(f"Uploading manifest file to {manifest_folder}/manifest.tsv")

    with open(manifest_file_path, "rb") as data:
        output_file_client = file_system_client.get_file_client(
            file_path=f"{manifest_folder}/manifest.tsv"
        )
        output_file_client.upload_data(data, overwrite=True)

    logger.info(f"Uploaded manifest file to {manifest_folder}/manifest.tsv")

    # Write status report
    timestr = time.strftime("%Y%m%d-%H%M%S")
    status_file_name = f"status_report_{timestr}.csv"
    status_file_path = os.path.join(meta_temp_folder_path, status_file_name)

    with open(status_file_path, mode="w", newline="") as f:
        fieldnames = [
            "file_path",
            "status",
            "processed",
            "convert_error",
            "output_uploaded",
            "output_files",
            "patient_folder_name",
            "patient_id",
        ]

        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(file_paths)

    # Upload status report to Azure
    with open(status_file_path, mode="rb") as data:
        logger.debug(
            f"Uploading workflow log to {pipeline_workflow_log_folder}/{status_file_name}"
        )

        output_blob_client = file_system_client.get_file_client(
            file_path=f"{pipeline_workflow_log_folder}/{status_file_name}"
        )

        output_blob_client.upload_data(data, overwrite=True)

        logger.info(
            f"Uploaded workflow log to {pipeline_workflow_log_folder}/{status_file_name}"
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

    # Upload file map to Azure
    logger.debug(f"Uploading file map to {dependency_folder}/file_map.json")

    try:
        file_processor.upload_json()
        logger.info(f"Uploaded file map to {dependency_folder}/file_map.json")
    except Exception as e:
        logger.error(f"Failed to upload file map to {dependency_folder}/file_map.json")
        raise e

    # Clean up the temporary folder
    shutil.rmtree(meta_temp_folder_path)

    logger.info(f"Processing complete. Output saved to {output_folder}")


if __name__ == "__main__":
    sys_args = sys.argv

    workers = 8

    parser = argparse.ArgumentParser(description="Process garmin data files locally")
    parser.add_argument(
        "--workers", type=int, default=workers, help="Number of workers to use"
    )
    parser.add_argument(
        "--input",
        type=str,
        default=r"C:\Users\sanjay\Downloads\FitnessTracker",
        help="Input folder path",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=r"C:\Users\sanjay\Downloads\FitnessTracker-processed",
        help="Output folder path",
    )
    args = parser.parse_args()

    workers = args.workers
    input_folder = args.input
    output_folder = args.output

    with contextlib.suppress(Exception):
        os.remove(output_folder)
        os.makedirs(output_folder, exist_ok=True)

    print(f"Using {workers} workers to process garmin data files")
    print(f"Input folder: {input_folder}")
    print(f"Output folder: {output_folder}")

    pipeline_local(input_folder, output_folder, workers, "AI-READI")
