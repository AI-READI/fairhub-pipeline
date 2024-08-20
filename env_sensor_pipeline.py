"""Process ecg data files"""

import contextlib
import datetime
import os
import tempfile
import shutil
import env_sensor.es_root as es
import azure.storage.blob as azureblob
import azure.storage.filedatalake as azurelake
import config
import utils.dependency as deps
import time
import csv
from traceback import format_exc
import utils.logwatch as logging
from utils.file_map_processor import FileMapProcessor


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
    red_cap_export_file = (
        f"{study_id}/pooled-data/REDCap/AIREADiPilot-2024Aug06_EnviroPhysSensorInfo.csv"
    )

    logger = logging.Logwatch("env_sensor", print=True)

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

    # Dev only
    with contextlib.suppress(Exception):
        file_system_client.delete_directory(processed_data_output_folder)

    with contextlib.suppress(Exception):
        file_system_client.delete_directory(data_plot_output_folder)

    paths = file_system_client.get_paths(path=input_folder)

    file_paths = []

    # Create a unique set of patient ids
    patient_ids = set()

    file_count = 0

    for path in paths:
        t = str(path.name)

        file_count += 1

        if (file_count % 100) == 0:
            logger.debug(f"Processed {file_count} files")

        if file_count > 1000:
            break

        original_file_name = t.split("/")[-1]

        # Check if the item is a xml file
        if original_file_name.split(".")[-1] != "csv":
            continue

        # Get the parent folder of the file.
        # The name of this folder is in the format ENV-patientID-someOtherID
        patientFolder = t.split("/")[-2]

        # Check if the folder name is in the format ENV-patientID-someOtherID
        if len(patientFolder.split("-")) != 3:
            continue

        _, patient_id, _ = patientFolder.split("-")

        file_paths.append(
            {
                "file_path": t,
                "status": "failed",
                "processed": False,
                "patient_folder": patientFolder,
                "patient_id": patient_id,
                "convert_error": True,
                "output_uploaded": False,
                "output_files": [],
            }
        )

        patient_ids.add(patient_id)

    logger.debug(f"Found {len(file_paths)} files in {input_folder}")

    # Create a temporary folder on the local machine
    temp_folder_path = tempfile.mkdtemp()

    # Create the output folder
    file_system_client.create_directory(processed_data_output_folder)

    # Create a temporary folder on the local machine
    meta_temp_folder_path = tempfile.mkdtemp()

    file_processor = FileMapProcessor(dependency_folder, ignore_file)

    total_files = len(patient_ids)

    # Download the redcap export file
    red_cap_export_file_path = os.path.join(meta_temp_folder_path, "redcap_export.csv")

    blob_client = blob_service_client.get_blob_client(
        container="stage-1-container", blob=red_cap_export_file
    )

    with open(red_cap_export_file_path, "wb") as data:
        blob_client.download_blob().readinto(data)

    # for idx, file_item in enumerate(file_paths):
    for idx, patient_id in enumerate(patient_ids):
        log_idx = idx + 1

        # if log_idx == 2:
        #     break

        # Get all the files for the patient
        patient_files = [
            file_item
            for file_item in file_paths
            if file_item["patient_id"] == patient_id
        ]

        # Verify that all the files are from the same patient folder
        patient_folders = set(
            [file_item["patient_folder"] for file_item in patient_files]
        )

        if len(patient_folders) != 1:
            logger.error("Patient files are not from the same patient folder")
            continue

        patient_folder = patient_folders.pop()

        # Recreate the patient folder
        temp_patient_folder_path = os.path.join(temp_folder_path, patient_folder)

        os.mkdir(temp_patient_folder_path)

        workflow_input_files = []

        logger.debug(f"Processing {patient_folder} - ({log_idx}/{total_files})")

        logger.debug(f"found {len(patient_files)} files for {patient_folder}")

        for file_idx, file_item in enumerate(patient_files):
            path = file_item["file_path"]

            workflow_input_files.append(path)

            original_file_name = path.split("/")[-1]

            logger.debug(f"Downloading {path} - ({file_idx}/{len(patient_files)})")

            download_path = os.path.join(temp_patient_folder_path, original_file_name)

            blob_client = blob_service_client.get_blob_client(
                container="stage-1-container", blob=path
            )

            with open(download_path, "wb") as data:
                blob_client.download_blob().readinto(data)

            file_item["download_path"] = download_path

        env_sensor_temp_folder_path = tempfile.mkdtemp()

        env_sensor = es.EnvironmentalSensor()

        try:
            conversion_dict = env_sensor.convert(
                temp_patient_folder_path,
                env_sensor_temp_folder_path,
                visit_file=red_cap_export_file_path,
            )

            print(f"Keys: {conversion_dict.keys()}")
            print(f'Participant ID: {conversion_dict["r"]["pppp"]}')
            print(f'Success: {conversion_dict["conversion_success"]}')
            print(f'Output file: {conversion_dict["output_file"]}')
        except Exception:
            logger.error(
                f"Failed to convert {patient_folder} - ({log_idx}/{total_files})"
            )
            error_exception = format_exc()
            error_exception = "".join(error_exception.splitlines())

            logger.error(error_exception)

            file_processor.append_errors(error_exception, path)
            continue

        logger.debug(f"Converted {patient_folder} - ({log_idx}/{total_files})")

        output_file = conversion_dict["output_file"]

        # get file size in mb
        file_size = os.path.getsize(output_file) / (1024 * 1024)

        logger.debug(f"File size: {file_size} MB")

        if conversion_dict["conversion_success"]:
            meta_dict = env_sensor.metadata(output_file)

            for k, v in meta_dict.items():
                print(f"{k}\t:  {v}")

            # Optionally make a plot for visual QA
            # dataplot_dict = env_sensor.dataplot(
            #     conversion_dict, data_plot_output_folder
            # )

        logger.debug(f"Uploading {output_file} - ({log_idx}/{total_files})")

        with open(f"{output_file}", "rb") as data:
            file_name2 = output_file.split("/")[-1]

            output_file_path = f"{processed_data_output_folder}/environmental_sensor/leelab_anura/{patient_id}/{file_name2}"

            try:
                output_blob_client = blob_service_client.get_blob_client(
                    container="stage-1-container",
                    blob=output_file_path,
                )
                output_blob_client.upload_blob(data)
            except Exception:
                logger.error(
                    f"Failed to upload {output_file} - ({log_idx}/{total_files})"
                )
                error_exception = format_exc()
                error_exception = "".join(error_exception.splitlines())

                logger.error(error_exception)

                continue

            file_item["output_files"].append(output_file_path)

        logger.debug(f"Uploaded {output_file} - ({log_idx}/{total_files})")

        file_item["convert_error"] = False
        file_item["processed"] = True

        shutil.rmtree(env_sensor_temp_folder_path)
        shutil.rmtree(temp_patient_folder_path)

    try:
        # file_processor.upload_json()
        logger.info(f"Uploaded file map to {dependency_folder}/file_map.json")
    except Exception as e:
        logger.error(f"Failed to upload file map to {dependency_folder}/file_map.json")
        raise e

    shutil.rmtree(meta_temp_folder_path)


if __name__ == "__main__":
    pipeline("AI-READI")
