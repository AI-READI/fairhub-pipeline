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
from garmin.garmin_deduplicate_parallel import deduplicate_and_extract_garmin_zip


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

            # Extract and deduplicate the zip file directly to temp folder
            logger.info(f"Extracting and deduplicating {patient_folder_path}")
            try:
                extracted_folder = deduplicate_and_extract_garmin_zip(
                    patient_folder_path, extract_to=temp_input_folder, logger=logger
                )
                logger.info(
                    f"Successfully extracted and deduplicated to {extracted_folder}"
                )
            except Exception as e:
                logger.error(
                    f"Error during extraction and deduplication of {patient_folder_path}: {e}"
                )
                # Fallback to original extraction method
                logger.warning("Falling back to original extraction method")
                download_path = os.path.join(temp_input_folder, "raw_data.zip")
                shutil.copy2(patient_folder_path, download_path)

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

                # logger.debug(
                #     f"Converting {file_modality}/{original_file_name} - ({file_idx}/{total_patient_files})"
                # )

                if file_modality == "Sleep":
                    try:
                        garmin_read_sleep.convert(
                            patient_file_path, converted_output_folder_path
                        )

                        # logger.info(
                        #     f"Converted {file_modality}/{original_file_name} - ({file_idx}/{total_patient_files})"
                        # )

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

                        # logger.info(
                        #     f"Converted {file_modality}/{original_file_name} - ({file_idx}/{total_patient_files})"
                        # )

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

    # process IDS
    ids_to_process = [
        "1075",
        "1076",
        "1077",
        "1079",
        "1080",
        "1081",
        "1083",
        "1084",
        "1085",
        "1086",
        "1087",
        "1088",
        "1089",
        "1092",
        "1093",
        "1094",
        "1095",
        "1096",
        "1097",
        "1098",
        "1099",
        "1100",
        "1101",
        "1103",
        "1104",
        "1105",
        "1106",
        "1109",
        "1110",
        "1111",
        "1112",
        "1113",
        "1114",
        "1115",
        "1116",
        "1117",
        "1118",
        "1119",
        "1120",
        "1121",
        "1122",
        "1123",
        "1124",
        "1125",
        "1126",
        "1128",
        "1129",
        "1131",
        "1132",
        "1133",
        "1134",
        "1135",
        "1136",
        "1137",
        "1138",
        "1139",
        "1140",
        "1141",
        "1143",
        "1144",
        "1145",
        "1146",
        "1148",
        "1149",
        "1151",
        "1152",
        "1153",
        "1154",
        "1155",
        "1156",
        "1157",
        "1158",
        "1159",
        "1160",
        "1161",
        "1163",
        "1164",
        "1166",
        "1167",
        "1168",
        "1169",
        "1170",
        "1171",
        "1172",
        "1173",
        "1174",
        "1175",
        "1176",
        "1177",
        "1178",
        "1179",
        "1180",
        "1181",
        "1182",
        "1183",
        "1184",
        "1185",
        "1186",
        "1187",
        "1188",
        "1189",
        "1192",
        "1193",
        "1194",
        "1195",
        "1196",
        "1197",
        "1198",
        "1199",
        "1200",
        "1201",
        "1202",
        "1203",
        "1204",
        "1205",
        "1206",
        "1207",
        "1208",
        "1209",
        "1210",
        "1211",
        "1212",
        "1213",
        "1214",
        "1215",
        "1216",
        "1217",
        "1218",
        "1219",
        "1220",
        "1221",
        "1222",
        "1223",
        "1224",
        "1225",
        "1226",
        "1227",
        "1228",
        "1229",
        "1230",
        "1231",
        "1232",
        "1233",
        "1234",
        "1235",
        "1236",
        "1237",
        "1238",
        "1239",
        "1240",
        "1241",
        "1242",
        "1243",
        "1244",
        "1245",
        "1246",
        "1247",
        "1248",
        "1249",
        "1250",
        "1251",
        "1252",
        "1253",
        "1254",
        "1255",
        "1256",
        "1257",
        "1258",
        "1259",
        "1260",
        "1261",
        "1262",
        "1263",
        "1264",
        "1266",
        "1267",
        "1268",
        "1269",
        "1270",
        "1271",
        "1272",
        "1273",
        "1274",
        "1275",
        "1276",
        "1277",
        "1278",
        "1280",
        "1281",
        "1282",
        "1283",
        "1284",
        "1285",
        "1286",
        "1287",
        "1288",
        "1289",
        "1290",
        "1291",
        "1292",
        "1293",
        "1294",
        "1295",
        "1297",
        "1298",
        "1299",
        "1300",
        "1301",
        "1302",
        "1303",
        "1304",
        "1305",
        "1306",
        "1307",
        "1308",
        "1309",
        "1310",
        "1311",
        "1312",
        "1313",
        "1314",
        "1315",
        "1316",
        "1317",
        "1318",
        "1320",
        "1321",
        "1322",
        "1323",
        "1324",
        "1325",
        "1326",
        "1327",
        "1328",
        "1329",
        "1330",
        "1331",
        "1332",
        "1333",
        "1334",
        "1335",
        "1336",
        "1337",
        "1338",
        "1339",
        "1340",
        "1341",
        "1344",
        "1345",
        "1346",
        "1347",
        "1348",
        "1349",
        "1350",
        "1351",
        "1352",
        "1353",
        "1354",
        "1355",
        "1356",
        "1357",
        "1359",
        "1361",
        "1362",
        "1363",
        "1364",
        "1365",
        "1366",
        "1367",
        "1368",
        "1372",
        "1373",
        "1374",
        "1376",
        "1377",
        "1378",
        "1379",
        "1380",
        "1381",
        "1383",
        "1384",
        "1385",
        "4041",
        "4045",
        "4052",
        "4058",
        "4059",
        "4060",
        "4061",
        "4062",
        "4064",
        "4065",
        "4066",
        "4067",
        "4068",
        "4072",
        "4073",
        "4074",
        "4075",
        "4076",
        "4077",
        "4078",
        "4082",
        "4087",
        "4088",
        "4089",
        "4091",
        "4095",
        "4100",
        "4101",
        "4103",
        "4104",
        "4105",
        "4106",
        "4107",
        "4108",
        "4109",
        "4110",
        "4111",
        "4112",
        "4113",
        "4114",
        "4115",
        "4116",
        "4117",
        "4118",
        "4119",
        "4120",
        "4121",
        "4122",
        "4123",
        "4124",
        "4125",
        "4127",
        "4128",
        "4130",
        "4131",
        "4132",
        "4133",
        "4134",
        "4135",
        "4136",
        "4138",
        "4139",
        "4140",
        "4141",
        "4142",
        "4143",
        "4145",
        "4146",
        "4147",
        "4148",
        "4149",
        "4150",
        "4151",
        "4153",
        "4154",
        "4155",
        "4156",
        "4157",
        "4158",
        "4159",
        "4160",
        "4161",
        "4162",
        "4163",
        "4164",
        "4165",
        "4166",
        "4167",
        "4168",
        "4169",
        "4170",
        "4171",
        "4172",
        "4175",
        "4177",
        "4178",
        "4179",
        "4180",
        "4181",
        "4182",
        "4183",
        "4184",
        "4185",
        "4186",
        "4187",
        "4188",
        "4189",
        "4190",
        "4191",
        "4192",
        "4193",
        "4196",
        "4200",
        "4201",
        "4202",
        "4203",
        "4205",
        "4206",
        "4207",
        "4208",
        "4210",
        "4211",
        "4212",
        "4215",
        "4216",
        "4219",
        "4220",
        "4221",
        "4222",
        "4224",
        "4225",
        "4226",
        "4227",
        "4228",
        "4229",
        "4230",
        "4231",
        "4232",
        "4234",
        "4235",
        "4236",
        "4237",
        "4239",
        "4240",
        "4241",
        "4244",
        "4245",
        "4246",
        "4247",
        "4248",
        "4249",
        "4250",
        "4251",
        "4252",
        "4253",
        "4254",
        "4255",
        "4256",
        "4257",
        "4261",
        "4263",
        "4264",
        "4265",
        "4266",
        "4267",
        "4268",
        "4269",
        "4270",
        "4271",
        "4273",
        "4274",
        "4275",
        "4278",
        "4279",
        "4280",
        "4281",
        "4282",
        "4283",
        "4284",
        "4285",
        "4286",
        "4287",
        "4289",
        "4290",
        "4291",
        "4292",
        "4294",
        "4296",
        "4297",
        "4298",
        "4299",
        "4301",
        "4302",
    ]

    file_paths = [file for file in file_paths if file["patient_id"] in ids_to_process]

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

    workers = 10

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

    # delete the output folder
    if os.path.exists(output_folder):
        shutil.rmtree(output_folder)
    os.makedirs(output_folder, exist_ok=True)

    print(f"Using {workers} workers to process garmin data files")
    print(f"Input folder: {input_folder}")
    print(f"Output folder: {output_folder}")

    pipeline_local(input_folder, output_folder, workers, "AI-READI")
