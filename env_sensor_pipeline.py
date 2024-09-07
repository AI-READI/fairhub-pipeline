"""Process env sensor data files"""

import contextlib
import datetime
import os
import tempfile
import shutil
import env_sensor.es_root as es
import azure.storage.filedatalake as azurelake
import config
import utils.dependency as deps
import time
import csv
from traceback import format_exc
import utils.logwatch as logging
from utils.file_map_processor import FileMapProcessor
from utils.time_estimator import TimeEstimator


def pipeline(
    study_id: str,
):  # sourcery skip: collection-builtin-to-comprehension, comprehension-to-generator, low-code-quality
    """Process env sensor data files for a study
    Args:
        study_id (str): the study id
    """

    if study_id is None or not study_id:
        raise ValueError("study_id is required")

    input_folder = f"{study_id}/pooled-data/EnvSensor"
    processed_data_output_folder = f"{study_id}/pooled-data/EnvSensor-processed"
    dependency_folder = f"{study_id}/dependency/EnvSensor"
    pipeline_workflow_log_folder = f"{study_id}/logs/EnvSensor"
    data_plot_output_folder = f"{study_id}/pooled-data/EnvSensor-dataplot"
    ignore_file = f"{study_id}/ignore/envSensor.ignore"
    red_cap_export_file = f"{study_id}/pooled-data/REDCap/AIREADiPilot-2024Sep05_EnviroPhysSensorInfoNoPilot.csv"

    logger = logging.Logwatch("env_sensor", print=True)

    # Get the list of blobs in the input folder
    file_system_client = azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_CONNECTION_STRING,
        file_system_name="stage-1-container",
    )

    with contextlib.suppress(Exception):
        file_system_client.delete_directory(processed_data_output_folder)

    with contextlib.suppress(Exception):
        file_system_client.delete_file(f"{dependency_folder}/file_map.json")

    patient_folder_paths = file_system_client.get_paths(
        path=input_folder, recursive=False
    )

    file_paths = []

    logger.debug(f"Getting folder paths in {input_folder}")

    for patient_folder_path in patient_folder_paths:
        t = str(patient_folder_path.name)

        patient_folder = t.split("/")[-1]

        # Check if the folder name is in the format dataType-patientID-someOtherID
        if len(patient_folder.split("-")) != 3:
            logger.debug(f"Skipping {patient_folder}")
            continue

        patient_id = patient_folder.split("-")[1]

        file_paths.append(
            {
                "file_path": t,
                "status": "failed",
                "processed": False,
                "patient_folder": patient_folder,
                "patient_id": patient_id,
                "convert_error": True,
                "output_uploaded": False,
                "output_files": [],
            }
        )

    # Create a temporary folder on the local machine
    temp_folder_path = tempfile.mkdtemp(prefix="env_sensor_")

    # Create a temporary folder on the local machine
    meta_temp_folder_path = tempfile.mkdtemp(prefix="env_sensor_meta_")

    # Download the redcap export file
    red_cap_export_file_path = os.path.join(meta_temp_folder_path, "redcap_export.csv")

    red_cap_export_file_client = file_system_client.get_file_client(
        file_path=red_cap_export_file
    )

    with open(red_cap_export_file_path, "wb") as data:
        red_cap_export_file_client.download_file().readinto(data)

    total_files = len(file_paths)

    logger.info(f"Found {total_files} items in {input_folder}")

    file_processor = FileMapProcessor(dependency_folder, ignore_file)

    workflow_file_dependencies = deps.WorkflowFileDependencies()

    time_estimator = TimeEstimator(total_files)

    for idx, file_item in enumerate(file_paths):
        log_idx = idx + 1

        # if log_idx == 5:
        #     break

        path = file_item["file_path"]

        workflow_input_files = [path]

        file_processor.add_entry(path, time.time())

        file_processor.clear_errors(path)

        # get the patient folder name from the path
        patient_folder_name = file_item["patient_folder"]

        input_folder = os.path.join(temp_folder_path, patient_folder_name)
        os.makedirs(input_folder, exist_ok=True)

        logger.debug(
            f"Downloading {patient_folder_name} to {input_folder} - ({log_idx}/{total_files})"
        )

        folder_contents = file_system_client.get_paths(path=path, recursive=True)

        for item in folder_contents:
            item_path = str(item.name)

            file_name = item_path.split("/")[-1]

            if not file_name.endswith(".csv"):
                continue

            input_file_client = file_system_client.get_file_client(file_path=item_path)

            download_path = os.path.join(input_folder, file_name)

            with open(file=download_path, mode="wb") as f:
                f.write(input_file_client.download_file().readall())
                logger.debug(
                    f"Downloaded {file_name} to {input_folder} - ({log_idx}/{total_files})"
                )

        logger.info(
            f"Downloaded {patient_folder_name} to {input_folder} - ({log_idx}/{total_files})"
        )

        output_folder = os.path.join(temp_folder_path, "output")
        os.makedirs(output_folder, exist_ok=True)

        env_sensor = es.EnvironmentalSensor()

        logger.debug(f"Converting {patient_folder_name} - ({log_idx}/{total_files})")

        try:
            conversion_dict = env_sensor.convert(
                input_folder,
                output_folder,
                visit_file=red_cap_export_file_path,
            )

        except Exception:
            logger.error(
                f"Failed to convert {patient_folder_name} - ({log_idx}/{total_files})"
            )
            error_exception = format_exc()
            error_exception = "".join(error_exception.splitlines())

            logger.error(error_exception)

            file_processor.append_errors(error_exception, path)
            continue

        logger.info(f"Converted {patient_folder_name} - ({log_idx}/{total_files})")

        logger.debug(
            f"Uploading outputs of {patient_folder_name} to {processed_data_output_folder} - ({log_idx}/{total_files})"
        )
        output_file = conversion_dict["output_file"]

        workflow_output_files = []

        outputs_uploaded = True

        file_processor.delete_preexisting_output_files(path)

        with open(f"{output_file}", "rb") as data:
            f2 = output_file.split("/")[-1]

            output_file_path = f"{processed_data_output_folder}/environmental_sensor/leelab_anura/{patient_id}/{f2}"

            logger.debug(
                f"Uploading {output_file} to {output_file_path} - ({log_idx}/{total_files})"
            )

            try:
                output_file_client = file_system_client.get_file_client(
                    file_path=output_file_path
                )

                # Check if the file already exists. If it does, throw an exception
                if output_file_client.exists():
                    raise Exception(
                        f"File {output_file_path} already exists. Throwing exception"
                    )

                output_file_client.upload_data(data, overwrite=True)

                logger.info(f"Uploaded {output_file_path} - ({log_idx}/{total_files})")
            except Exception:
                outputs_uploaded = False
                logger.error(
                    f"Failed to upload {output_file_path} - ({log_idx}/{total_files})"
                )
                error_exception = format_exc()
                error_exception = "".join(error_exception.splitlines())

                logger.error(error_exception)

                file_processor.append_errors(error_exception, path)
                continue

            file_item["output_files"].append(output_file_path)
            workflow_output_files.append(output_file_path)

        file_processor.confirm_output_files(path, workflow_output_files, "")

        if outputs_uploaded:
            file_item["output_uploaded"] = True
            file_item["status"] = "success"
            logger.info(
                f"Uploaded outputs of {patient_folder_name} to {processed_data_output_folder} - ({log_idx}/{total_files})"
            )
        else:
            logger.error(
                f"Failed to upload outputs of {patient_folder_name} to {processed_data_output_folder} - ({log_idx}/{total_files})"
            )

        workflow_file_dependencies.add_dependency(
            workflow_input_files, workflow_output_files
        )

        logger.time(time_estimator.step())

        print(f"Cleaning up temp folders - ({log_idx}/{total_files})")
        shutil.rmtree(output_folder)
        shutil.rmtree(input_folder)

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

    with open(workflow_log_file_path, mode="w") as f:
        fieldnames = [
            "file_path",
            "status",
            "processed",
            "patient_folder",
            "patient_id",
            "convert_error",
            "output_uploaded",
            "output_files",
        ]

        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=",")

        for file_item in file_paths:
            file_item["output_files"] = ";".join(file_item["output_files"])

        writer.writeheader()
        writer.writerows(file_paths)

        logger.debug(
            f"Uploading workflow log to {pipeline_workflow_log_folder}/{file_name}"
        )

    with open(workflow_log_file_path, mode="rb") as data:
        workflow_output_file_Client = file_system_client.get_file_client(
            file_path=f"{pipeline_workflow_log_folder}/{file_name}"
        )

        workflow_output_file_Client.upload_data(data, overwrite=True)

        logger.info(
            f"Uploaded workflow log to {pipeline_workflow_log_folder}/{file_name}"
        )

    # Write the dependencies to a file
    deps_output = workflow_file_dependencies.write_to_file(meta_temp_folder_path)

    json_file_path = deps_output["file_path"]
    json_file_name = deps_output["file_name"]

    logger.debug(f"Uploading dependencies to {dependency_folder}/{json_file_name}")

    with open(json_file_path, "rb") as data:
        dependency_output_file_Client = file_system_client.get_file_client(
            file_path=f"{dependency_folder}/{json_file_name}"
        )

        dependency_output_file_Client.upload_data(data, overwrite=True)

        logger.info(f"Uploaded dependencies to {dependency_folder}/{json_file_name}")

    shutil.rmtree(meta_temp_folder_path)


if __name__ == "__main__":
    pipeline("AI-READI")
