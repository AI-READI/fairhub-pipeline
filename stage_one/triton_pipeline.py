"""Process ecg data files"""

import datetime
import os
import tempfile
import shutil
from traceback import format_exc

import imaging.imaging_maestro2_triton_root as Maestro2_Triton
import imaging.imaging_utils as imaging_utils
import azure.storage.blob as azureblob
import azure.storage.filedatalake as azurelake
import config
import utils.dependency as deps
import time
import csv
import utils.logwatch as logging
from utils.file_map_processor import FileMapProcessor

# import pprint


def pipeline(study_id: str):  # sourcery skip: low-code-quality
    """Process ecg data files for a study
    Args:
        study_id (str): the study id
    """

    if study_id is None or not study_id:
        raise ValueError("study_id is required")

    input_folder = f"{study_id}/pooled-data/Triton"
    dependency_folder = f"{study_id}/dependency/Triton"
    pipeline_workflow_log_folder = f"{study_id}/logs/Triton"
    processed_data_output_folder = f"{study_id}/pooled-data/Triton-processed"
    ignore_file = f"{study_id}/ignore/triton.ignore"

    logger = logging.Logwatch("triton", print=True)

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

    paths = file_system_client.get_paths(path=input_folder)

    file_paths = []

    imaging_utils.update_pydicom_dicom_dictionary()

    for path in paths:
        t = str(path.name)

        original_file_name = t.split("/")[-1]

        # Check if the item is an .fda.zip file
        if not original_file_name.endswith(".zip"):
            continue

        # Get the parent folder of the file.
        # The name of this file is in the format siteName_dataType_siteName_dataType_startDate-endDate_*.fda.zip

        parts = original_file_name.split("_")

        if len(parts) != 6:
            continue

        site_name = parts[0]
        data_type = parts[1]
        # site_name_2 = parts[2]
        # data_type_2 = parts[3]
        start_date_end_date = parts[4]

        start_date = start_date_end_date.split("-")[0]
        end_date = start_date_end_date.split("-")[1]

        file_paths.append(
            {
                "file_path": t,
                "status": "failed",
                "processed": False,
                "site_name": site_name,
                "data_type": data_type,
                "start_date": start_date,
                "end_date": end_date,
                "organize_error": True,
                "convert_error": True,
                "format_error": False,
                "output_uploaded": False,
                "output_files": [],
            }
        )

    logger.debug(f"Found {len(file_paths)} files in {input_folder}")

    # Create the output folder
    file_system_client.create_directory(processed_data_output_folder)

    workflow_file_dependencies = deps.WorkflowFileDependencies()

    file_processor = FileMapProcessor(dependency_folder, ignore_file)

    total_files = len(file_paths)

    device = "Triton"

    for idx, file_item in enumerate(file_paths):
        log_idx = idx + 1

        # dev
        # if log_idx == 15:
        #     break

        # Create a temporary folder on the local machine
        temp_folder_path = tempfile.mkdtemp()

        path = file_item["file_path"]

        workflow_input_files = [path]

        # get the file name from the path
        original_file_name = path.split("/")[-1]

        should_file_be_ignored = file_processor.is_file_ignored(file_item, path)

        if should_file_be_ignored:
            logger.info(f"Ignoring {original_file_name} - ({log_idx}/{total_files})")
            continue

        # get the file name from the path
        original_file_name = path.split("/")[-1]

        # download the file to the temp folder
        blob_client = blob_service_client.get_blob_client(
            container="stage-1-container", blob=path
        )

        input_last_modified = blob_client.get_blob_properties().last_modified

        should_process = file_processor.file_should_process(path, input_last_modified)

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

        step1_folder = os.path.join(temp_folder_path, "step1")

        if not os.path.exists(step1_folder):
            os.makedirs(step1_folder)

        download_path = os.path.join(step1_folder, original_file_name)

        with open(download_path, "wb") as data:
            blob_client.download_blob().readinto(data)

        logger.info(
            f"Downloaded {original_file_name} to {download_path} - ({log_idx}/{total_files})"
        )

        zip_files = imaging_utils.list_zip_files(step1_folder)

        if len(zip_files) == 0:
            logger.warn(f"No zip files found in {step1_folder}")
            continue

        step2_folder = os.path.join(temp_folder_path, "step2")

        if not os.path.exists(step2_folder):
            os.makedirs(step2_folder)

        logger.debug(
            f"Unzipping {original_file_name} to {step2_folder} - ({log_idx}/{total_files})"
        )

        imaging_utils.unzip_fda_file(download_path, step2_folder)

        logger.info(
            f"Unzipped {original_file_name} to {step2_folder} - ({log_idx}/{total_files})"
        )

        step3_folder = os.path.join(temp_folder_path, "step3")

        step2_data_folders = imaging_utils.list_subfolders(
            os.path.join(step2_folder, device)
        )

        # process the files
        triton_instance = Maestro2_Triton.Maestro2_Triton()

        try:
            for step2_data_folder in step2_data_folders:
                logger.debug(step2_data_folder)
                # organize_result = triton_instance.organize(
                #     step2_data_folder, step3_folder
                # )
                triton_instance.organize(
                    step2_data_folder, os.path.join(step3_folder, device)
                )
        except Exception:
            logger.error(
                f"Failed to organize {original_file_name} - ({log_idx}/{total_files})"
            )
            error_exception = format_exc()
            error_exception = "".join(error_exception.splitlines())

            logger.error(error_exception)

            file_processor.append_errors(error_exception, path)
            continue

        file_item["organize_error"] = False

        step4_folder = os.path.join(temp_folder_path, "step4")

        if not os.path.exists(step4_folder):
            os.makedirs(step4_folder)

        protocols = [
            "triton_3d_radial_oct",
            "triton_macula_6x6_octa",
            "triton_macula_12x12_octa",
        ]

        try:
            for protocol in protocols:
                output_folder_path = os.path.join(step4_folder, device, protocol)

                if not os.path.exists(output_folder_path):
                    os.makedirs(output_folder_path)

                folders = imaging_utils.list_subfolders(
                    os.path.join(step3_folder, device, protocol)
                )

                for folder in folders:
                    triton_instance.convert(folder, output_folder_path)

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

        device_list = [os.path.join(step4_folder, device)]

        destination_folder = os.path.join(temp_folder_path, "step5")

        for device_folder in device_list:
            file_list = imaging_utils.get_filtered_file_names(device_folder)

            for file_name in file_list:

                try:
                    imaging_utils.format_file(file_name, destination_folder)
                except Exception:
                    file_item["format_error"] = True
                    logger.error(
                        f"Failed to format {file_name} - ({log_idx}/{total_files})"
                    )
                    error_exception = format_exc()
                    error_exception = "".join(error_exception.splitlines())

                    logger.error(error_exception)

                    file_processor.append_errors(error_exception, path)
                    continue

        file_item["processed"] = True

        logger.debug(
            f"Uploading outputs of {original_file_name} to {processed_data_output_folder} - ({log_idx}/{total_files})"
        )

        workflow_output_files = []

        outputs_uploaded = True

        file_processor.delete_preexisting_output_files(path)

        for root, dirs, files in os.walk(destination_folder):
            for file in files:
                file_path = os.path.join(root, file)

                with open(f"{file_path}", "rb") as data:
                    file_name2 = file_path.split("/")[-5:]

                    combined_file_name = "/".join(file_name2)

                    logger.debug(
                        f"Uploading {combined_file_name} - ({log_idx}/{total_files})"
                    )

                    output_file_path = (
                        f"{processed_data_output_folder}/{combined_file_name}"
                    )

                    try:
                        output_blob_client = blob_service_client.get_blob_client(
                            container="stage-1-container",
                            blob=output_file_path,
                        )
                        output_blob_client.upload_blob(data)
                    except Exception:
                        outputs_uploaded = False
                        logger.error(
                            f"Failed to upload {combined_file_name} - ({log_idx}/{total_files})"
                        )
                        error_exception = format_exc()
                        error_exception = "".join(error_exception.splitlines())

                        logger.error(error_exception)

                        file_processor.append_errors(error_exception, path)
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
            logger.error(
                f"Failed to upload outputs of {original_file_name} to {processed_data_output_folder} - ({log_idx}/{total_files})"
            )

        workflow_file_dependencies.add_dependency(
            workflow_input_files, workflow_output_files
        )

        shutil.rmtree(temp_folder_path)

    file_processor.delete_out_of_date_output_files()

    file_processor.remove_seen_flag_from_map()

    logger.debug(f"Uploading file map to {dependency_folder}/file_map.json")

    try:
        file_processor.upload_json()
        logger.info(f"Uploaded file map to {dependency_folder}/file_map.json")
    except Exception as e:
        logger.error(f"Failed to upload file map to {dependency_folder}/file_map.json")
        raise e

    temp_folder_path = tempfile.mkdtemp()

    # Write the workflow log to a file
    timestr = time.strftime("%Y%m%d-%H%M%S")
    file_name = f"status_report_{timestr}.csv"
    workflow_log_file_path = os.path.join(temp_folder_path, file_name)

    with open(workflow_log_file_path, mode="w") as f:
        fieldnames = [
            "file_path",
            "status",
            "processed",
            "batch_folder",
            "site_name",
            "data_type",
            "start_date",
            "end_date",
            "organize_error",
            "convert_error",
            "format_error",
            "output_uploaded",
            "output_files",
        ]

        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=",")

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

        logger.info(
            f"Uploaded workflow log to {pipeline_workflow_log_folder}/{file_name}"
        )

    # Write the dependencies to a file
    deps_output = workflow_file_dependencies.write_to_file(temp_folder_path)

    json_file_path = deps_output["file_path"]
    json_file_name = deps_output["file_name"]

    logger.debug(f"Uploading dependencies to {dependency_folder}/{json_file_name}")

    with open(json_file_path, "rb") as data:
        output_blob_client = blob_service_client.get_blob_client(
            container="stage-1-container",
            blob=f"{dependency_folder}/{json_file_name}",
        )
        output_blob_client.upload_blob(data)

        logger.info(f"Uploaded dependencies to {dependency_folder}/{json_file_name}")

    shutil.rmtree(temp_folder_path)

    # dev
    # move the workflow log file and the json file to the current directory
    # shutil.move(workflow_log_file_path, "status.csv")
    # shutil.move(json_file_path, "file_map.json")


if __name__ == "__main__":
    pipeline("AI-READI")
