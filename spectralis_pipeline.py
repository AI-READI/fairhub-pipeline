"""Process spectralis data files"""

import contextlib
import os
import tempfile
import shutil
from imaging.imaging_spectralis_root import Spectralis
import azure.storage.filedatalake as azurelake
import config
import time
import csv
import json
import imaging.imaging_utils as imaging_utils
import utils.dependency as deps
from traceback import format_exc
import utils.logwatch as logging
from utils.file_map_processor import FileMapProcessor
from utils.time_estimator import TimeEstimator


def pipeline(
    study_id: str,
):  # sourcery skip: low-code-quality, move-assign, use-named-expression
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

    batch_folder_paths = file_system_client.get_paths(
        path=input_folder, recursive=False
    )

    file_paths = []

    logger.debug(f"Getting batch folder paths in {input_folder}")

    exit_flag = False

    for batch_folder_path in batch_folder_paths:
        if exit_flag:
            break
        else:
            exit_flag = True

        t = str(batch_folder_path.name)

        batch_folder = t.split("/")[-1]

        # Check if the folder name is in the format siteName_dataType_startDate-endDate
        if len(batch_folder.split("_")) != 3:
            continue

        site_name, data_type, start_date_end_date = batch_folder.split("_")

        start_date, end_date = start_date_end_date.split("-")

        # For each batch folder, get the list of files in the /DICOM folder

        dicom_folder_path = f"{input_folder}/{batch_folder}/DICOM"

        dicom_file_paths = file_system_client.get_paths(
            path=dicom_folder_path, recursive=True
        )

        for dicom_file_path in dicom_file_paths:
            q = str(dicom_file_path.name)

            original_file_name = q.split("/")[-1]

            file_paths.append(
                {
                    "file_path": q,
                    "status": "failed",
                    "processed": False,
                    "batch_folder": batch_folder,
                    "site_name": site_name,
                    "data_type": data_type,
                    "start_date": start_date,
                    "end_date": end_date,
                    "organize_error": True,
                    "organize_result": "",
                    "convert_error": True,
                    "output_uploaded": False,
                    "output_files": [],
                }
            )

    logger.info(f"Found {len(file_paths)} items in {input_folder}")

    # Create a temporary folder on the local machine
    temp_folder_path = tempfile.mkdtemp(prefix="spectralis_")

    # Create a temporary folder on the local machine
    meta_temp_folder_path = tempfile.mkdtemp(prefix="spectralis_meta_")

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

        step1_folder = os.path.join(temp_folder_path, "step1")
        os.makedirs(step1_folder, exist_ok=True)

        should_file_be_ignored = file_processor.is_file_ignored(file_item, path)

        if should_file_be_ignored:
            logger.info(f"Ignoring {original_file_name} - ({log_idx}/{total_files})")

            logger.time(time_estimator.step())
            continue

        file_client = file_system_client.get_file_client(file_path=path)

        file_properties = file_client.get_file_properties().metadata

        # Check if item is a directory
        if file_properties.get("hdi_isfolder"):
            logger.debug(f"file path `{path}` is a directory. Skipping")

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

        batch_folder = step1_folder

        download_path = os.path.join(step1_folder, original_file_name)

        logger.debug(
            f"Downloading {original_file_name} to {download_path} - ({log_idx}/{total_files})"
        )

        with open(file=download_path, mode="wb") as f:
            f.write(file_client.download_file().readall())

        logger.info(
            f"Downloaded {original_file_name} to {download_path} - ({log_idx}/{total_files})"
        )

        spectralis_instance = Spectralis()

        # Organize spectralis files by protocol

        step2_folder = os.path.join(temp_folder_path, "step2")
        os.makedirs(step2_folder, exist_ok=True)

        filtered_list = imaging_utils.spectralis_get_filtered_file_names(batch_folder)

        try:
            for file_name in filtered_list:

                organize_result = spectralis_instance.organize(file_name, step2_folder)

                file_item["organize_result"] = json.dumps(organize_result)
        except Exception:
            logger.error(
                f"Failed to organize {original_file_name} - ({log_idx}/{total_files})"
            )

            error_exception = "".join(format_exc().splitlines())

            logger.error(error_exception)

            file_processor.append_errors(error_exception, path)

            logger.time(time_estimator.step())
            continue

        # convert dicom files to nema compliant dicom files
        protocols = [
            "spectralis_onh_rc_hr_oct",
            "spectralis_onh_rc_hr_retinal_photography",
            "spectralis_ppol_mac_hr_oct",
            "spectralis_ppol_mac_hr_oct_small",
            "spectralis_ppol_mac_hr_retinal_photography",
            "spectralis_ppol_mac_hr_retinal_photography_small",
        ]

        step3_folder = os.path.join(temp_folder_path, "step3")
        os.makedirs(step3_folder, exist_ok=True)

        logger.debug(f"Converting to nema compliant dicom files")

        for protocol in protocols:
            output = f"{step3_folder}/{protocol}"
            if not os.path.exists(output):
                os.makedirs(output)

            files = imaging_utils.get_filtered_file_names(f"{step2_folder}/{protocol}")

            for file in files:
                spectralis_instance.convert(file, output)

        step4_folder = os.path.join(temp_folder_path, "step4")
        os.makedirs(step4_folder, exist_ok=True)

        metadata_folder = os.path.join(temp_folder_path, "metadata")
        os.makedirs(metadata_folder, exist_ok=True)

        logger.debug(f"Formatting files and generating metadata")

        for folder in [step3_folder]:
            filelist = imaging_utils.get_filtered_file_names(folder)

            for file in filelist:
                full_file_path = imaging_utils.format_file(file, step4_folder)

                spectralis_instance.metadata(full_file_path, metadata_folder)

        # Upload the processed files to the output folder

        workflow_output_files = []

        outputs_uploaded = True
        upload_exception = ""

        file_processor.delete_preexisting_output_files(path)

        for root, dirs, files in os.walk(step4_folder):
            for file in files:
                full_file_path = os.path.join(root, file)

                logger.debug(f"Found file {full_file_path} - ({log_idx}/{total_files})")

                f2 = full_file_path.split("/")[-5:]

                combined_file_name = "/".join(f2)

                output_file_path = (
                    f"{processed_data_output_folder}/{combined_file_name}"
                )

                logger.debug(
                    f"Uploading {full_file_path} to {output_file_path} - ({log_idx}/{total_files})"
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
                            f"Uploaded {combined_file_name} - ({log_idx}/{total_files})"
                        )
                except Exception:
                    outputs_uploaded = False
                    logger.error(
                        f"Failed to upload {combined_file_name} - ({log_idx}/{total_files})"
                    )
                    error_exception = format_exc()
                    e = "".join(error_exception.splitlines())

                    logger.error(e)

                    file_processor.append_errors(e, path)
                    continue

                file_item["output_files"].append(output_file_path)
                workflow_output_files.append(output_file_path)

        # Add the new output files to the file map

        file_processor.confirm_output_files(
            path, workflow_output_files, input_last_modified
        )

        if outputs_uploaded:
            file_item["output_uploaded"] = True
            file_item["status"] = "success"
            logger.info(
                f"Uploaded outputs of {original_file_name} to {processed_data_output_folder} - ({log_idx}/{total_files})"
            )
        else:
            file_item["output_uploaded"] = upload_exception
            logger.error(
                f"Failed to upload outputs of {original_file_name} to {processed_data_output_folder} - ({log_idx}/{total_files})"
            )

        workflow_file_dependencies.add_dependency(
            workflow_input_files, workflow_output_files
        )

        logger.time(time_estimator.step())

        shutil.rmtree(metadata_folder)
        shutil.rmtree(step4_folder)
        shutil.rmtree(step3_folder)
        shutil.rmtree(step2_folder)
        os.remove(download_path)

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
