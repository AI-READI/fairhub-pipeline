"""Process cgm data files"""

import contextlib
import os
import tempfile
import shutil
import argparse
from traceback import format_exc
from datetime import datetime

import cgm.cgm as cgm
import cgm.cgm_manifest as cgm_manifest
from cgm.cgm_sanity import sanity_check_cgm_file
import azure.storage.filedatalake as azurelake
import config
import utils.dependency as deps
import time
import csv
from utils.file_map_processor import FileMapProcessor
import utils.logwatch as logging
from utils.time_estimator import TimeEstimator
from functools import partial
from multiprocessing.pool import ThreadPool
import sys

overall_time_estimator = TimeEstimator(1)


def worker(
    workflow_file_dependencies,
    file_processor,
    manifest,
    participant_filter_list: list,
    processed_data_qc_folder,
    processed_data_output_folder,
    file_paths: list,
    worker_id: int,
):
    logger = logging.Logwatch(
        "cgm",
        print=True,
        thread_id=worker_id,
        overall_time_estimator=overall_time_estimator,
    )

    # Azure client for QC upload only
    file_system_client = azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_CONNECTION_STRING,
        file_system_name="stage-1-container",
    )

    total_files = len(file_paths)
    time_estimator = TimeEstimator(total_files)

    for file_item in file_paths:
        path = file_item["file_path"]

        workflow_input_files = [path]

        file_name = os.path.basename(path)
        file_name_only = file_name.split(".")[0]
        patient_id = "Unknown"

        if file_name_only.split("-")[0] == "DEX":
            patient_id = file_name_only.split("-")[1]
        elif file_name_only.split("_")[0] == "Clarity":
            patient_id = file_name_only.split("_")[3]

        logger.info(f"Processing {patient_id}")

        if file_processor.is_file_ignored(file_name, path):
            logger.info(f"Ignoring {file_name} because it is in the ignore file")
            logger.time(time_estimator.step())
            continue

        if str(patient_id) not in participant_filter_list:
            logger.debug(
                f"Participant ID {patient_id} not in the allowed list. Skipping {file_name}"
            )
            continue

        input_last_modified = datetime.fromtimestamp(os.path.getmtime(path))

        should_process = file_processor.file_should_process(path, input_last_modified)

        if not should_process:
            logger.debug(
                f"The file {path} has not been modified since the last time it was processed",
            )
            logger.debug(f"Skipping {path} - File has not been modified")
            logger.time(time_estimator.step())
            continue

        file_processor.add_entry(path, input_last_modified)
        file_processor.clear_errors(path)

        with tempfile.TemporaryDirectory(prefix="cgm_pipeline_") as temp_folder_path:
            download_path = os.path.join(temp_folder_path, f"DEX_{patient_id}.csv")

            logger.debug(f"Copying {file_name} to {download_path}")

            shutil.copy(path, download_path)

            logger.info(f"Copied {file_name} to {download_path}")

            cgm_path = download_path

            with tempfile.TemporaryDirectory(
                prefix="cgm_pipeline_temp_"
            ) as cgm_temp_folder_path:
                cgm_output_file_path = os.path.join(
                    cgm_temp_folder_path, f"DEX-{patient_id}.json"
                )
                cgm_final_output_file_path = os.path.join(
                    cgm_temp_folder_path,
                    f"DEX-{patient_id}/DEX-{patient_id}.json",
                )
                cgm_final_output_qc_file_path = os.path.join(
                    cgm_temp_folder_path,
                    f"DEX-{patient_id}/QC_results.txt",
                )

            uuid = f"AIREADI-{patient_id}"

            timezone = "pst"

            if patient_id.startswith("7"):
                timezone = "cst"

            logger.debug(f"Converting {file_name}")

            try:
                cgm.convert(
                    input_path=cgm_path,
                    output_path=cgm_output_file_path,
                    effective_time_frame=1,
                    event_type=2,
                    source_device_id=3,
                    blood_glucose=4,
                    transmitter_time=5,
                    transmitter_id=6,
                    uuid=uuid,
                    timezone=timezone,
                )
            except Exception:
                logger.error(f"Failed to convert {file_name}")

                error_exception = "".join(format_exc().splitlines())

                logger.error(error_exception)

                file_processor.append_errors(error_exception, path)

                logger.time(time_estimator.step())

                continue

            logger.info(f"Converted {file_name}")

            check_path = (
                cgm_final_output_file_path
                if os.path.exists(cgm_final_output_file_path)
                else cgm_output_file_path
            )

            summary = sanity_check_cgm_file(check_path, logger)

            file_processor.add_additional_data(path, summary)

            file_item["convert_error"] = False
            file_item["processed"] = True

            logger.debug(
                f"Copying outputs of {file_name} to {processed_data_output_folder}"
            )

            output_files = [cgm_final_output_file_path]

            workflow_output_files = []

            outputs_uploaded = True

            file_processor.delete_preexisting_output_files(path)

            for file in output_files:
                f2 = os.path.basename(file)

                output_file_path = os.path.join(
                    processed_data_output_folder,
                    "wearable_blood_glucose",
                    "continuous_glucose_monitoring",
                    "dexcom_g6",
                    patient_id,
                    f"{patient_id}_DEX.json"
                )

                logger.debug(f"Copying {f2} to {output_file_path}")

                try:
                    os.makedirs(os.path.dirname(output_file_path), exist_ok=True)

                    shutil.copy(file, output_file_path)

                    logger.info(f"Copied {f2} to {output_file_path}")
                except Exception:
                    outputs_uploaded = False

                    logger.error(f"Failed to copy {file}")

                    error_exception = format_exc()
                    error_exception = "".join(error_exception.splitlines())

                    logger.error(error_exception)

                    file_processor.append_errors(error_exception, path)
                    continue

                file_item["output_files"].append(output_file_path)
                workflow_output_files.append(output_file_path)

                manifest_glucose_file_path = f"/wearable_blood_glucose/continuous_glucose_monitoring/dexcom_g6/{patient_id}/{patient_id}_DEX.json"

                logger.debug(f"Generating manifest for {f2}")

                manifest.calculate_file_sampling_extent(
                    cgm_final_output_file_path, manifest_glucose_file_path
                )

                logger.info(f"Generated manifest for {f2}")

            logger.info(
                f"Copied the outputs of {file_name} to {processed_data_output_folder}"
            )

            file_processor.confirm_output_files(
                path, workflow_output_files, input_last_modified
            )

            if outputs_uploaded:
                file_item["output_uploaded"] = True
                file_item["status"] = "success"
                logger.info(
                    f"Copied outputs of {file_name} to {processed_data_output_folder}"
                )
            else:
                logger.error(
                    f"Failed to copy outputs of {file_name} to {processed_data_output_folder})"
                )

            workflow_file_dependencies.add_dependency(
                workflow_input_files, workflow_output_files
            )

            logger.debug(f"Uploading QC file for {file_name}")

            output_qc_file_path = (
                f"{processed_data_qc_folder}/{patient_id}/QC_results.txt"
            )

            try:
                with open(cgm_final_output_qc_file_path, "rb") as data:
                    output_blob_client = file_system_client.get_file_client(
                        file_path=output_qc_file_path
                    )

                    output_blob_client.upload_data(data, overwrite=True)
            except Exception:
                file_item["qc_uploaded"] = False

                error_exception = "".join(format_exc().splitlines())

                logger.error(error_exception)

                file_processor.append_errors(error_exception, path)

                logger.time(time_estimator.step())

                continue

            logger.info(f"Uploaded QC file for {file_name}")

            logger.time(time_estimator.step())

            os.remove(download_path)


def pipeline(study_id: str, workers: int = 4, args: list = None):
    if args is None:
        args = []

    global overall_time_estimator

    if study_id is None or not study_id:
        raise ValueError("study_id is required")

    input_folder = os.path.join(os.path.expanduser("~"), "Downloads", "CGM")
    processed_data_output_folder = os.path.join(
        os.path.expanduser("~"), "Downloads", "CGM-processed"
    )
    dependency_folder = f"{study_id}/dependency/CGM"
    processed_data_qc_folder = f"{study_id}/pooled-data/CGM2-qc"
    ignore_file = f"{study_id}/ignore/cgm.ignore"
    manifest_folder = f"{study_id}/pooled-data/CGM2-manifest"
    participant_filter_list_file = f"{study_id}/dependency/PatientID/AllParticipantIDs_year_3_12-31-2025.csv"

    logger = logging.Logwatch("cgm", print=True)

    file_system_client = azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_CONNECTION_STRING,
        file_system_name="stage-1-container",
    )

    if os.path.exists(processed_data_output_folder):
        shutil.rmtree(processed_data_output_folder)

    file_paths = []
    participant_filter_list = []

    meta_temp_folder_path = tempfile.mkdtemp(prefix="cgm_pipeline_meta_")

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

        participant_filter_list.pop(0)

        print("Participant filter loaded:", len(participant_filter_list))

    paths = os.listdir(input_folder)

    for t in paths:

        file_name = t.split("/")[-1]
        if file_name.split(".")[-1] != "csv":
            continue

        file_paths.append({
            "file_path": os.path.join(input_folder, t),
            "status": "failed",
            "processed": False,
            "convert_error": True,
            "output_uploaded": False,
            "qc_uploaded": True,
            "output_files": [],
            }
        )

    total_files = len(file_paths)
    overall_time_estimator = TimeEstimator(total_files)

    logger.debug(f"Found {total_files} files in {input_folder}")

    workflow_file_dependencies = deps.WorkflowFileDependencies()
    file_processor = FileMapProcessor(dependency_folder, ignore_file, args)

    manifest = cgm_manifest.CGMManifest()

    chunk_size = (len(file_paths) + workers - 1) // workers
    chunks = [file_paths[i : i + chunk_size] for i in range(0, total_files, chunk_size)]
    args = [(chunk, index + 1) for index, chunk in enumerate(chunks)]
    pipe = partial(
        worker,
        workflow_file_dependencies,
        file_processor,
        manifest,
        participant_filter_list,
        processed_data_qc_folder,
        processed_data_output_folder,
    )
    pool = ThreadPool(workers)
    pool.starmap(pipe, args)

    file_processor.delete_out_of_date_output_files()
    file_processor.remove_seen_flag_from_map()

    pipeline_workflow_log_folder = f"{study_id}/logs/local_CGM/CGM"

    manifest_file_path = os.path.join(meta_temp_folder_path, "manifest_cgm_v2.tsv")
    manifest.write_tsv(manifest_file_path)

    logger.info(f"Uploading manifest file to {manifest_folder}/manifest_cgm_v2.tsv")

    with open(manifest_file_path, "rb") as data:
        output_blob_client = file_system_client.get_file_client(
            file_path=f"{manifest_folder}/manifest_cgm_v2.tsv"
        )

        output_blob_client.upload_data(data, overwrite=True)
        logger.info(f"Uploaded manifest file to {manifest_folder}/manifest_cgm_v2.tsv")

    logger.debug(f"Uploading file map to {dependency_folder}/file_map.json")

    try:
        file_processor.upload_json()
        logger.info(f"Uploaded file map to {dependency_folder}/file_map.json")
    except Exception as e:
        logger.error(f"Failed to upload file map to {dependency_folder}/file_map.json")
        raise e

    timestr = time.strftime("%Y%m%d-%H%M%S")
    file_name = f"status_report_{timestr}.csv"
    workflow_log_file_path = os.path.join(meta_temp_folder_path, file_name)

    with open(workflow_log_file_path, "w", newline="") as csvfile:
        fieldnames = [
            "file_path",
            "status",
            "processed",
            "convert_error",
            "output_uploaded",
            "qc_uploaded",
            "output_files",
            "patient_id",
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        for file_item in file_paths:
            file_item["output_files"] = ";".join(file_item["output_files"])

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

    deps_output = workflow_file_dependencies.write_to_file(meta_temp_folder_path)

    json_file_path = deps_output["file_path"]
    json_file_name = deps_output["file_name"]

    logger.info(
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

    shutil.rmtree(meta_temp_folder_path)


if __name__ == "__main__":
    sys_args = sys.argv

    workers = 4

    parser = argparse.ArgumentParser(description="Process cgm data files")
    parser.add_argument(
        "--workers", type=int, default=workers, help="Number of workers to use"
    )
    args = parser.parse_args()

    workers = args.workers

    print(f"Using {workers} workers to process cgm data files")

    pipeline("AI-READI", workers, sys_args)