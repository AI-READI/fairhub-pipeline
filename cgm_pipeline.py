"""Process ecg data files"""

import contextlib
import datetime
import os
import tempfile
import shutil
from traceback import format_exc

import cgm.cgm as cgm
import cgm.cgm_manifest as cgm_manifest
import azure.storage.blob as azureblob
import azure.storage.filedatalake as azurelake
import config
import utils.dependency as deps
import time
import csv
from utils.file_map_processor import FileMapProcessor
import utils.logwatch as logging

"""
SCRIPT_PATH=""
FOLDER_PATH="CGM/input/UCSD-CGM/"  # Replace with the path to your CSV files
TIME_ZONE="pst"  # Set your desired timezone here
for file in ${FOLDER_PATH}DEX-*.csv; do ID=$(basename "$file" .csv | cut -d '-' -f 2) echo ${ID} python3 "${SCRIPT_PATH}CGM_API.py" "DEX-${ID}.csv" "DEX-${ID}.json" effective_time_frame=1,event_type=2,source_device_id=3,blood_glucose=4,transmitter_time=5,transmitter_id=6,uuid=AIREADI-${ID},timezone=${TIME_ZONE} --o foo=7,bar=8
done
"""


def pipeline(study_id: str):  # sourcery skip: low-code-quality
    """Process cgm data files for a study
    Args:
        study_id (str): the study id
    """

    if study_id is None or not study_id:
        raise ValueError("study_id is required")

    input_folder = f"{study_id}/Stanford-Test/PILOT-Aug29-2024/CGM-Pool"
    processed_data_output_folder = (
        f"{study_id}/Stanford-Test/PILOT-Aug29-2024/CGM-Processed"
    )
    processed_data_qc_folder = f"{study_id}/Stanford-Test/PILOT-Aug29-2024/CGM-qc"
    dependency_folder = f"{study_id}/Stanford-Test/PILOT-Aug29-2024/"
    manifest_folder = f"{study_id}/Stanford-Test/PILOT-Aug29-2024/manifest"
    pipeline_workflow_log_folder = f"{study_id}/Stanford-Test/PILOT-Aug29-2024/CGM-logs"
    ignore_file = f"{study_id}/ignore/cgm.ignore"

    # input_folder = f"{study_id}/pooled-data/CGM"
    # processed_data_output_folder = f"{study_id}/pooled-data/CGM-processed"
    # dependency_folder = f"{study_id}/dependency/CGM"
    # manifest_folder = f"{study_id}/manifest/CGM"

    logger = logging.Logwatch("cgm", print=True)

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

    # Delete the qc folder if it exists
    with contextlib.suppress(Exception):
        file_system_client.delete_directory(processed_data_qc_folder)
    with contextlib.suppress(Exception):
        file_system_client.delete_directory(processed_data_output_folder)

    paths = file_system_client.get_paths(path=input_folder)

    file_paths = []

    for path in paths:
        t = str(path.name)

        original_file_name = t.split("/")[-1]

        # Check if the item is a csv file
        if original_file_name.split(".")[-1] != "csv":
            continue

        file_paths.append(
            {
                "file_path": t,
                "status": "failed",
                "processed": False,
                "convert_error": True,
                "output_uploaded": False,
                "qc_uploaded": True,
                "output_files": [],
            }
        )

    logger.debug(f"Found {len(file_paths)} files in {input_folder}")

    # Create a temporary folder on the local machine
    temp_folder_path = tempfile.mkdtemp()

    # Create the output folder
    file_system_client.create_directory(processed_data_output_folder)

    workflow_file_dependencies = deps.WorkflowFileDependencies()

    file_processor = FileMapProcessor(dependency_folder, ignore_file)

    total_files = len(file_paths)

    manifest = cgm_manifest.CGMManifest()

    for idx, file_item in enumerate(file_paths):
        log_idx = idx + 1

        # if log_idx == 3:
        #     break

        path = file_item["file_path"]

        workflow_input_files = [path]

        # get the file name from the path. It's in the format Clarity_Export_AIREADI_{id}_*.csv
        original_file_name = path.split("/")[-1]

        # should_file_be_ignored = file_processor.is_file_ignored(file_item, path)
        should_file_be_ignored = False

        if should_file_be_ignored:
            logger.info(f"Ignoring {original_file_name} - ({log_idx}/{total_files})")
            continue

        file_name_only = original_file_name.split(".")[0]

        patient_id = "Unknown"

        if file_name_only.split("-")[0] == "DEX":
            patient_id = file_name_only.split("-")[1]
        elif file_name_only.split("_")[0] == "Clarity":
            patient_id = file_name_only.split("_")[3]

        # download the file to the temp folder
        blob_client = blob_service_client.get_blob_client(
            container="stage-1-container", blob=path
        )

        input_last_modified = blob_client.get_blob_properties().last_modified

        # should_process = file_processor.file_should_process(path, input_last_modified)
        should_process = True

        if not should_process:
            logger.debug(
                f"The file {path} has not been modified since the last time it was processed",
            )
            logger.debug(
                f"Skipping {path} - ({log_idx}/{total_files}) - File has not been modified"
            )

            continue

        file_processor.add_entry(path, input_last_modified)

        file_processor.clear_errors(path)

        logger.debug(f"Processing {path} - ({log_idx}/{total_files})")

        # File should be downloaded as DEX_{patient_id}.csv
        download_path = os.path.join(temp_folder_path, f"DEX-{patient_id}.csv")

        with open(download_path, "wb") as data:
            blob_client.download_blob().readinto(data)

        logger.info(
            f"Downloaded {original_file_name} to {download_path} - ({log_idx}/{total_files})"
        )

        cgm_path = download_path

        cgm_temp_folder_path = tempfile.mkdtemp()

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

        # if patient id starts with a 7xxx(UAB), set the timezone to "cst"
        if patient_id.startswith("7"):
            timezone = "cst"

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
            logger.error(
                f"Failed to convert {original_file_name} - ({log_idx}/{total_files})"
            )
            error_exception = format_exc()
            error_exception = "".join(error_exception.splitlines())

            logger.error(error_exception)

            file_processor.append_errors(error_exception, path)
            continue

        file_item["convert_error"] = False
        file_item["processed"] = True

        logger.debug(
            f"Uploading outputs of {original_file_name} to {processed_data_output_folder} - ({log_idx}/{total_files})"
        )

        # file is converted successfully. Upload the output file

        output_files = [cgm_final_output_file_path]

        workflow_output_files = []

        outputs_uploaded = True

        file_processor.delete_preexisting_output_files(path)

        for file in output_files:
            with open(f"{file}", "rb") as data:

                # file_name2 = file.split("/")[-1]

                logger.debug(f"Uploading {file} - ({log_idx}/{total_files})")

                output_file_path = f"wearable_blood_glucose/continuous_glucose_monitoring/dexcom_g6/{patient_id}/{patient_id}_DEX.json"

                try:
                    output_blob_client = blob_service_client.get_blob_client(
                        container="stage-1-container",
                        blob=f"{processed_data_output_folder}/{output_file_path}",
                    )
                    output_blob_client.upload_blob(data)
                except Exception:
                    outputs_uploaded = False
                    logger.error(f"Failed to upload {file} - ({log_idx}/{total_files})")
                    error_exception = format_exc()
                    error_exception = "".join(error_exception.splitlines())

                    logger.error(error_exception)

                    file_processor.append_errors(error_exception, path)
                    continue

                file_item["output_files"].append(output_file_path)
                workflow_output_files.append(output_file_path)

                manifest_glucose_file_path = output_file_path

                # Generate the manifest entry
                manifest.calculate_file_sampling_extent(
                    cgm_final_output_file_path, manifest_glucose_file_path
                )

        # Add the new output files to the file map
        file_processor.confirm_output_files(
            path, workflow_output_files, input_last_modified
        )

        # upload the QC file
        logger.debug(
            f"Uploading QC file for {original_file_name} - ({log_idx}/{total_files})"
        )
        output_qc_file_path = f"{processed_data_qc_folder}/{patient_id}/QC_results.txt"

        try:
            with open(cgm_final_output_qc_file_path, "rb") as data:
                output_blob_client = blob_service_client.get_blob_client(
                    container="stage-1-container",
                    blob=output_qc_file_path,
                )
                output_blob_client.upload_blob(data)
        except Exception:
            file_item["qc_uploaded"] = False
            logger.error(
                f"Failed to format {original_file_name} - ({log_idx}/{total_files})"
            )
            error_exception = format_exc()
            error_exception = "".join(error_exception.splitlines())

            logger.error(error_exception)

            file_processor.append_errors(error_exception, path)

            continue

        logger.debug(
            f"Uploaded QC file for {original_file_name} - ({log_idx}/{total_files})"
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

        shutil.rmtree(cgm_temp_folder_path)

        os.remove(download_path)

    file_processor.delete_out_of_date_output_files()

    file_processor.remove_seen_flag_from_map()

    # Write the manifest to a file
    manifest_file_path = os.path.join(temp_folder_path, "manifest_cgm_v2.tsv")

    manifest.write_tsv(manifest_file_path)

    logger.debug(f"Uploading file map to {dependency_folder}/file_map.json")

    try:
        file_processor.upload_json()
        logger.info(f"Uploaded file map to {dependency_folder}/file_map.json")
    except Exception as e:
        logger.error(f"Failed to upload file map to {dependency_folder}/file_map.json")
        raise e

    # Upload the manifest file
    with open(manifest_file_path, "rb") as data:
        output_blob_client = blob_service_client.get_blob_client(
            container="stage-1-container",
            blob=f"{manifest_folder}/manifest_cgm_v2.tsv",
        )

        # Delete the manifest file if it exists
        with contextlib.suppress(Exception):
            output_blob_client.delete_blob()

        output_blob_client.upload_blob(data)

    # Write the workflow log to a file
    timestr = time.strftime("%Y%m%d-%H%M%S")
    file_name = f"status_report_{timestr}.csv"
    workflow_log_file_path = os.path.join(temp_folder_path, file_name)

    with open(workflow_log_file_path, "w", newline="") as csvfile:
        fieldnames = [
            "file_path",
            "status",
            "processed",
            "convert_error",
            "output_uploaded",
            "qc_uploaded",
            "output_files",
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

        output_blob_client = blob_service_client.get_blob_client(
            container="stage-1-container",
            blob=f"{pipeline_workflow_log_folder}/{file_name}",
        )

        output_blob_client.upload_blob(data)

    deps_output = workflow_file_dependencies.write_to_file(temp_folder_path)

    json_file_path = deps_output["file_path"]
    json_file_name = deps_output["file_name"]

    with open(json_file_path, "rb") as data:
        output_blob_client = blob_service_client.get_blob_client(
            container="stage-1-container",
            blob=f"{dependency_folder}/{json_file_name}",
        )
        output_blob_client.upload_blob(data)

    shutil.rmtree(temp_folder_path)

    # dev
    # move the workflow log file and the json file to the current directory
    # shutil.move(workflow_log_file_path, "status.csv")
    # shutil.move(json_file_path, "file_map.json")


if __name__ == "__main__":
    pipeline("AI-READI")
