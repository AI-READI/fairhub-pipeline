"""Process ecg data files"""

import contextlib
import os
import tempfile
import shutil
import ecg.ecg_root as ecg
import ecg.ecg_metadata as ecg_metadata
import azure.storage.filedatalake as azurelake
import config
import utils.dependency as deps
import time
import csv
from traceback import format_exc
import utils.logwatch as logging
from utils.file_map_processor import FileMapProcessor
from utils.time_estimator import TimeEstimator


def pipeline(study_id: str):  # sourcery skip: low-code-quality
    """Process spectralis data files for a study
    Args:
        study_id (str): the study id
    """

    if study_id is None or not study_id:
        raise ValueError("study_id is required")

    input_folder = f"{study_id}/pooled-data/Spectralis"
    processed_data_output_folder = f"{study_id}/pooled-data/Spectralis-processed"
    dependency_folder = f"{study_id}/dependency/Spectralis"
    pipeline_workflow_log_folder = f"{study_id}/logs/Spectralis"
    ignore_file = f"{study_id}/ignore/spectralis.ignore"

    logger = logging.Logwatch("spectralis", print=True)

    # Get the list of blobs in the input folder
    file_system_client = azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_CONNECTION_STRING,
        file_system_name="stage-1-container",
    )

    with contextlib.suppress(Exception):
        file_system_client.delete_directory(processed_data_output_folder)

    with contextlib.suppress(Exception):
        file_system_client.delete_file(f"{dependency_folder}/file_map.json")

    batch_folder_paths = file_system_client.get_paths(path=input_folder, recursive=False)

    file_paths = []

    logger.debug(f"Getting file paths in {input_folder}")

    for batch_folder_path in batch_folder_paths:
        t = str(batch_folder_path.name)

        batch_folder = t.split("/")[-1]

        logger.debug(f"Getting files in {batch_folder}")

        # Check if the folder name is in the format siteName_dataType_startDate-endDate
        if len(batch_folder.split("_")) != 3:
            continue

        site_name, data_type, start_date_end_date = batch_folder.split("_")

        start_date, end_date = start_date_end_date.split("-")

        # For each batch folder, get the list of files in the /DICOM folder

        dicom_folder_path = f"{input_folder}/{batch_folder}/DICOM"

        dicom_folder_paths = file_system_client.get_paths(
            path=dicom_folder_path, recursive=True
        )

        for dicom_folder_path in dicom_folder_paths:
            q = str(dicom_folder_path.name)

            original_file_name = q.split("/")[-1]

            # continue if the file has an extension
            if len(original_file_name.split(".")) > 1:
                continue

            file_paths.append(
                {
                    "file_path": t,
                    "status": "failed",
                    "processed": False,
                    "batch_folder": batch_folder,
                    "site_name": site_name,
                    "data_type": data_type,
                    "start_date": start_date,
                    "end_date": end_date,
                    "convert_error": True,
                    "output_uploaded": False,
                    "output_files": [],
                }
            )
        

    logger.info(f"Found {len(file_paths)} items in {input_folder}")

    # Create a temporary folder on the local machine
    temp_folder_path = tempfile.mkdtemp()

    # Create a temporary folder on the local machine
    meta_temp_folder_path = tempfile.mkdtemp()

    total_files = len(file_paths)

    file_processor = FileMapProcessor(dependency_folder, ignore_file)

    workflow_file_dependencies = deps.WorkflowFileDependencies()

    time_estimator = TimeEstimator(len(file_paths))

    
    for idx, file_item in enumerate(file_paths):
        log_idx = idx + 1

        # if log_idx == 5:
        #     break

        path = file_item["file_path"]

        workflow_input_files = [path]

        # get the file name from the path
        original_file_name = path.split("/")[-1]

        should_file_be_ignored = file_processor.is_file_ignored(file_item, path)

        if should_file_be_ignored:
            logger.info(f"Ignoring {original_file_name} - ({log_idx}/{total_files})")

            logger.time(time_estimator.step())
            continue

        file_client = file_system_client.get_file_client(file_path=path)

        file_properties = file_client.get_file_properties().metadata

        # Check if item is a directory
        if file_properties.get("hdi_isfolder"):
            logger.debug("file path is a directory. Skipping")

            logger.time(time_estimator.step())
            continue

        input_last_modified = file_client.get_file_properties().last_modified

        should_process = file_processor.file_should_process(path, input_last_modified)

        if not should_process:
            logger.debug(
                f"The file {path} has not been modified since the last time it was processed",
            )
            logger.debug(
                f"Skipping {path} - ({log_idx}/{total_files}) - File has not been modified"
            )

            logger.time(time_estimator.step())
            continue

        file_processor.add_entry(path, input_last_modified)

        file_processor.clear_errors(path)

        logger.debug(f"Processing {path} - ({log_idx}/{total_files})")

        x= path.split(os.path.join(input_folder, file_item["batch_folder"]))[1]

        download_path = os.path.join(temp_folder_path, 
            x.lstrip("/").replace("/", "\\"))

        with open(file=download_path, mode="wb") as f:
            f.write(file_client.download_file().readall())

        logger.info(
            f"Downloaded {original_file_name} to {download_path} - ({log_idx}/{total_files})"
        )

 

 


if __name__ == "__main__":
    pipeline("AI-READI")

    # delete the ecg.log file
    if os.path.exists("ecg.log"):
        os.remove("ecg.log")
