"""Process flio data files"""

import contextlib
import os
import tempfile
import shutil
import azure.storage.filedatalake as azurelake
import config
import time
import json
import csv
import imaging.imaging_utils as imaging_utils
import utils.dependency as deps
import utils.logwatch as logging
from imaging.imaging_flio_root import Flio
from traceback import format_exc
from utils.file_map_processor import FileMapProcessor
from utils.time_estimator import TimeEstimator

JSON_PATH = os.path.join(os.path.dirname(__file__), "flio", "flio_uid_data.json")


def pipeline(study_id: str):  # sourcery skip: low-code-quality
    """Process flio data files for a study
    Args:
        study_id (str): the study id
    """

    if study_id is None or not study_id:
        raise ValueError("study_id is required")

    input_folder = f"{study_id}/pooled-data/Flio"
    processed_data_output_folder = f"{study_id}/pooled-data/Flio-processed"
    dependency_folder = f"{study_id}/dependency/Flio"
    pipeline_workflow_log_folder = f"{study_id}/logs/Flio"
    ignore_file = f"{study_id}/ignore/flio.ignore"

    logger = logging.Logwatch("flio", print=True)

    # Get the list of blobs in the input folder
    file_system_client = azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_CONNECTION_STRING,
        file_system_name="stage-1-container",
    )

    with contextlib.suppress(Exception):
        file_system_client.delete_directory(processed_data_output_folder)

    with contextlib.suppress(Exception):
        file_system_client.delete_file(f"{dependency_folder}/file_map.json")

    batch_folder_paths = file_system_client.get_paths(
        path=input_folder, recursive=False
    )

    file_paths = []

    logger.debug(f"Getting batch folder paths in {input_folder}")

    exit_flag = False

    logger.debug(f"Getting file paths in {input_folder}")

    for batch_folder_path in batch_folder_paths:
        if exit_flag:
            break
        else:
            exit_flag = True

        t = str(batch_folder_path.name)

        batch_folder = t.split("/")[-1]

        # Check if the folder name is in the format siteName_dataType_startDate-endDate
        if len(batch_folder.split("_")) != 3:
            logger.debug(f"Skipping {batch_folder}")
            continue

        site_name, data_type, start_date_end_date = batch_folder.split("_")

        start_date, end_date = start_date_end_date.split("-")

        # For each batch folder, get the list of patient folders in the batch folder
        patient_folder_paths = file_system_client.get_paths(
            path=f"{input_folder}/{batch_folder}", recursive=False
        )

        for patient_folder_path in patient_folder_paths:
            q = str(patient_folder_path.name)

            patient_folder = q.split("/")[-1]

            # Check if the folder name is in the format xx_AIREADI_patientID
            if len(patient_folder.split("_")) != 3:
                logger.debug(f"Skipping {patient_folder}")
                continue

            file_paths.append(
                {
                    "file_path": q,
                    "status": "failed",
                    "processed": False,
                    "batch_folder": batch_folder,
                    "patient_folder": patient_folder,
                    "site_name": site_name,
                    "data_type": data_type,
                    "start_date": start_date,
                    "end_date": end_date,
                    "organize_error": True,
                    "organize_result": "",
                    "output_uploaded": False,
                    "output_files": [],
                }
            )

    logger.info(f"Found {len(file_paths)} items in {input_folder}")

    # Create a temporary folder on the local machine
    temp_folder_path = tempfile.mkdtemp(prefix="flio_")

    # Create a temporary folder on the local machine
    meta_temp_folder_path = tempfile.mkdtemp(prefix="flio_meta_")

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

        file_processor.add_entry(path, time.time())

        file_processor.clear_errors(path)

        # get the patient folder name from the path
        patient_folder_name = path.split("/")[-1]

        step1_folder = os.path.join(temp_folder_path, "step1")
        os.makedirs(step1_folder, exist_ok=True)

        logger.debug(
            f"Downloading {patient_folder_name} to {step1_folder}"
        )

        # Download the contents of the patient folder to the step1 folder
        folder_contents = file_system_client.get_paths(path=path, recursive=True)

        for item in folder_contents:
            ip = item_path = str(item.name)

            file_client = file_system_client.get_file_client(file_path=item_path)

            file_properties = file_client.get_file_properties().metadata

            item_path_split = item_path.split(f"{patient_folder_name}/")

            if len(item_path_split) != 2:
                continue
            else:
                ip = item_path_split[1]

            download_path = os.path.join(step1_folder, patient_folder_name, ip)

            # Check if item is a directory
            if file_properties.get("hdi_isfolder"):
                # Create the directory if it doesn't exist
                logger.debug(
                    f"file path `{item_path}` is a directory. Creating directory {download_path}"
                )

                if not os.path.exists(download_path):
                    os.makedirs(download_path, exist_ok=True)

                continue

            logger.debug(
                f"Downloading {item_path} to {download_path}"
            )

            with open(file=download_path, mode="wb") as f:
                f.write(file_client.download_file().readall())
                logger.debug(
                    f"Downloaded {item_path} to {download_path}"
                )

        logger.info(
            f"Downloaded {patient_folder_name} to {step1_folder}"
        )

        flio_instance = Flio()

        # Organize flio files by scan

        step2_folder = os.path.join(temp_folder_path, "step2")
        os.makedirs(step2_folder, exist_ok=True)

        try:
            organize_result = flio_instance.organize(step1_folder, step2_folder)

            file_item["organize_result"] = json.dumps(organize_result)
        except Exception:
            logger.error(
                f"Failed to organize {patient_folder_name}"
            )

            error_exception = "".join(format_exc().splitlines())

            logger.error(error_exception)

            file_processor.append_errors(error_exception, path)

            logger.time(time_estimator.step())
            continue

        step3_folder = os.path.join(temp_folder_path, "step3")
        os.makedirs(step3_folder, exist_ok=True)

        flio_folderlist = imaging_utils.list_subfolders(step2_folder)

        for flio_folder in flio_folderlist:
            if "flio" in flio_folder:
                flio_instance.convert1(flio_folder, step3_folder, JSON_PATH)

        step4_folder = os.path.join(temp_folder_path, "step4")
        os.makedirs(step4_folder, exist_ok=True)

        filtered_list = imaging_utils.get_filtered_file_names(step3_folder)

        for file_name in filtered_list:
            if "flio" in file_name:
                flio_instance.convert2(file_name, step4_folder)

        step5_folder = os.path.join(temp_folder_path, "step5")
        os.makedirs(step5_folder, exist_ok=True)

        filtered_list = imaging_utils.get_filtered_file_names(step4_folder)

        for file_name in filtered_list:
            if "flio" in file_name:
                full_file_path = imaging_utils.format_file(file_name, step5_folder)

                flio_instance.metadata(full_file_path, step5_folder)

        # Upload the processed files to the output folder

        workflow_output_files = []

        outputs_uploaded = True
        upload_exception = ""

        file_processor.delete_preexisting_output_files(path)

        for root, dirs, files in os.walk(step5_folder):
            for file in files:
                full_file_path = os.path.join(root, file)

                logger.debug(f"Found file {full_file_path}")

                # Check if is a json metadata file
                if full_file_path.endswith(".json"):
                    logger.debug(
                        f"Skipping {full_file_path} for now"
                    )
                    continue

                f2 = full_file_path.split("/")[-5:]

                combined_file_name = "/".join(f2)

                output_file_path = (
                    f"{processed_data_output_folder}/{combined_file_name}"
                )

                logger.debug(
                    f"Uploading {full_file_path} to {output_file_path}"
                )

                try:
                    output_file_client = file_system_client.get_file_client(
                        output_file_path
                    )

                    # Check if the file already exists. If it does, throw an exception
                    if output_file_client.exists():
                        raise Exception(
                            f"File {output_file_path} already exists. Throwing exception"
                        )

                    with open(full_file_path, "rb") as f:
                        output_file_client.upload_data(f, overwrite=True)
                        logger.info(
                            f"Uploaded {combined_file_name}"
                        )
                except Exception:
                    outputs_uploaded = False
                    logger.error(
                        f"Failed to upload {combined_file_name}"
                    )

                    upload_exception = "".join(format_exc().splitlines())

                    logger.error(upload_exception)

                    file_processor.append_errors(error_exception, path)
                    continue

                file_item["output_files"].append(output_file_path)
                workflow_output_files.append(output_file_path)

        # Add the new output files to the file map
        file_processor.confirm_output_files(path, workflow_output_files, "")

        if outputs_uploaded:
            file_item["output_uploaded"] = True
            file_item["status"] = "success"
            logger.info(
                f"Uploaded outputs of {patient_folder_name} to {processed_data_output_folder}"
            )
        else:
            file_item["output_uploaded"] = upload_exception
            logger.error(
                f"Failed to upload outputs of {patient_folder_name} to {processed_data_output_folder}"
            )

        workflow_file_dependencies.add_dependency(
            workflow_input_files, workflow_output_files
        )

        logger.time(time_estimator.step())

        logger.debug("Cleaning up temp folders")

        shutil.rmtree(step5_folder)
        shutil.rmtree(step4_folder)
        shutil.rmtree(step3_folder)
        shutil.rmtree(step2_folder)
        shutil.rmtree(step1_folder)

    file_processor.delete_out_of_date_output_files()
    file_processor.remove_seen_flag_from_map()

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

    with open(workflow_log_file_path, "w", newline="") as csvfile:
        fieldnames = [
            "file_path",
            "status",
            "processed",
            "batch_folder",
            "site_name",
            "data_type",
            "start_date",
            "end_date",
            "convert_error",
            "output_uploaded",
            "output_files",
        ]

        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        writer.writerows(file_paths)

    # Upload the workflow log file to the pipeline_workflow_log_folder
    with open(workflow_log_file_path, mode="rb") as data:
        logger.debug(
            f"Uploading workflow log to {pipeline_workflow_log_folder}/{file_name}"
        )

        output_file_client = file_system_client.get_file_client(
            file_path=f"{pipeline_workflow_log_folder}/{file_name}"
        )

        output_file_client.upload_data(data, overwrite=True)

    deps_output = workflow_file_dependencies.write_to_file(meta_temp_folder_path)

    json_file_path = deps_output["file_path"]
    json_file_name = deps_output["file_name"]

    with open(json_file_path, "rb") as data:
        output_file_client = file_system_client.get_file_client(
            file_path=f"{dependency_folder}/{json_file_name}"
        )

        output_file_client.upload_data(data, overwrite=True)

    shutil.rmtree(meta_temp_folder_path)


if __name__ == "__main__":
    pipeline("AI-READI")
