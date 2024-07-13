"""Process ecg data files"""

import contextlib
import datetime
import os
import tempfile
import shutil
import imaging.imaging_eidon_retinal_photography_root as EIDON
import imaging.imaging_utils as imaging_utils
import azure.storage.blob as azureblob
import azure.storage.filedatalake as azurelake
import config
import utils.dependency as deps
import time
import csv
import utils.logwatch as logging
import json

# import pprint


def pipeline(study_id: str):  # sourcery skip: low-code-quality
    """Process ecg data files for a study
    Args:
        study_id (str): the study id
    """

    if study_id is None or not study_id:
        raise ValueError("study_id is required")

    input_folder = f"{study_id}/pooled-data/Eidon"
    dependency_folder = f"{study_id}/dependency/Eidon"
    pipeline_workflow_log_folder = f"{study_id}/logs/Eidon"
    processed_data_output_folder = f"{study_id}/pooled-data/Eidon-processed"

    logger = logging.Logwatch("eidon", print=True)

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

    # Delete the output folder if it exists
    # with contextlib.suppress(Exception):
    #     file_system_client.delete_directory(processed_data_output_folder)

    paths = file_system_client.get_paths(path=input_folder)

    file_paths = []

    for path in paths:
        t = str(path.name)

        file_name = t.split("/")[-1]

        # Check if the item is an dicom file
        if file_name.split(".")[-1] != "dcm":
            continue

        # Get the parent folder of the file.
        # The name of this folder is in the format siteName_dataType_startDate-endDate
        batch_folder = t.split("/")[-2]

        # Check if the folder name is in the format siteName_dataType_startDate-endDate
        if len(batch_folder.split("_")) != 3:
            continue

        site_name, data_type, start_date_end_date = batch_folder.split("_")

        start_date = start_date_end_date.split("-")[0]
        end_date = start_date_end_date.split("-")[1]

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
                "organize_error": True,
                "convert_error": True,
                "format_error": True,
                "output_uploaded": False,
                "output_files": [],
            }
        )

    logger.debug(f"Found {len(file_paths)} files in {input_folder}")

    # Create the output folder
    file_system_client.create_directory(processed_data_output_folder)

    # Create a temporary folder on the local machine
    meta_temp_folder_path = tempfile.mkdtemp()

    # Download the meta file for the pipeline
    file_map_download_path = os.path.join(meta_temp_folder_path, "file_map.json")

    file_map = []

    meta_blob_client = blob_service_client.get_blob_client(
        container="stage-1-container", blob=f"{dependency_folder}/file_map.json"
    )

    with contextlib.suppress(Exception):
        with open(file_map_download_path, "wb") as data:
            meta_blob_client.download_blob().readinto(data)

        # Load the meta file
        with open(file_map_download_path, "r") as f:
            file_map = json.load(f)

    for entry in file_map:
        # This is to delete the output files of files that are no longer in the input folder
        entry["seen"] = False

    workflow_file_dependencies = deps.WorkflowFileDependencies()

    total_files = len(file_paths)

    for idx, file_item in enumerate(file_paths):
        log_idx = idx + 1

        # dev
        # if log_idx == 6:
        #     break

        # Create a temporary folder on the local machine
        temp_folder_path = tempfile.mkdtemp()

        path = file_item["file_path"]

        input_path = path
        workflow_input_files = [path]

        # download the file to the temp folder
        blob_client = blob_service_client.get_blob_client(
            container="stage-1-container", blob=path
        )

        should_process = True
        input_last_modified = blob_client.get_blob_properties().last_modified

        # Check if the input file is in the file map
        for entry in file_map:
            if entry["input_file"] == path:
                entry["seen"] = True

                t = input_last_modified.strftime("%Y-%m-%d %H:%M:%S+00:00")

                # Check if the file has been modified since the last time it was processed
                if t == entry["input_last_modified"]:
                    logger.debug(
                        f"The file {path} has not been modified since the last time it was processed",
                    )
                    should_process = False

                break

        if not should_process:
            logger.debug(
                f"Skipping {path} - ({log_idx}/{total_files}) - File has not been modified"
            )

            continue

        file_map.append(
            {
                "input_file": path,
                "output_files": [],
                "input_last_modified": input_last_modified,
                "seen": True,
            }
        )

        logger.debug(f"Processing {path} - ({log_idx}/{total_files})")

        # get the file name from the path
        original_file_name = path.split("/")[-1]

        step1_folder = os.path.join(temp_folder_path, "step1")

        if not os.path.exists(step1_folder):
            os.makedirs(step1_folder)

        download_path = os.path.join(step1_folder, original_file_name)

        with open(download_path, "wb") as data:
            blob_client.download_blob().readinto(data)

        logger.info(
            f"Downloaded {file_name} to {download_path} - ({log_idx}/{total_files})"
        )

        filtered_file_names = imaging_utils.get_filtered_file_names(step1_folder)

        step2_folder = os.path.join(temp_folder_path, "step2")

        eidon_instance = EIDON.Eidon()

        try:
            for file in filtered_file_names:
                # organize_result = eidon_instance.organize(download_path, organize_temp_folder_path)
                eidon_instance.organize(file, step2_folder)
        except Exception:
            logger.error(
                f"Failed to organize {original_file_name} - ({log_idx}/{total_files})"
            )
            continue

        file_item["organize_error"] = False

        step3_folder = os.path.join(temp_folder_path, "step3")

        protocols = [
            "eidon_mosaic_cfp",
            "eidon_uwf_central_cfp",
            "eidon_uwf_central_faf",
            "eidon_uwf_central_ir",
            "eidon_uwf_nasal_cfp",
            "eidon_uwf_temporal_cfp",
        ]

        try:
            for protocol in protocols:
                output = os.path.join(step3_folder, protocol)

                if not os.path.exists(output):
                    os.makedirs(output)

                files = imaging_utils.get_filtered_file_names(
                    os.path.join(step2_folder, protocol)
                )

                for file in files:
                    eidon_instance.convert(file, output)
        except Exception:
            logger.error(
                f"Failed to convert {original_file_name} - ({log_idx}/{total_files})"
            )
            continue

        file_item["convert_error"] = False

        device_list = [step3_folder]

        destination_folder = os.path.join(temp_folder_path, "step4")

        try:
            for folder in device_list:
                filelist = imaging_utils.get_filtered_file_names(folder)

                for file in filelist:
                    imaging_utils.format_file(file, destination_folder)
        except Exception:
            logger.error(
                f"Failed to format {original_file_name} - ({log_idx}/{total_files})"
            )
            continue

        file_item["format_error"] = False
        file_item["processed"] = True

        logger.debug(
            f"Uploading outputs of {file_name} to {processed_data_output_folder} - ({log_idx}/{total_files})"
        )

        workflow_output_files = []

        outputs_uploaded = True

        # Delete the output files associated with the input file
        # We are doing a file level replacement
        for entry in file_map:
            if entry["input_file"] == input_path:
                for output_file in entry["output_files"]:
                    with contextlib.suppress(Exception):
                        output_blob_client = blob_service_client.get_blob_client(
                            container="stage-1-container", blob=output_file
                        )
                        output_blob_client.delete_blob()

                break

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

                    with contextlib.suppress(Exception):
                        output_blob_client = blob_service_client.get_blob_client(
                            container="stage-1-container", blob=output_file_path
                        )
                        output_blob_client.delete_blob()

                    try:
                        output_blob_client = blob_service_client.get_blob_client(
                            container="stage-1-container", blob=output_file_path
                        )
                        output_blob_client.upload_blob(data)
                    except Exception:
                        outputs_uploaded = False
                        logger.error(
                            f"Failed to upload {combined_file_name} - ({log_idx}/{total_files})"
                        )
                        continue

                    file_item["output_files"].append(output_file_path)
                    workflow_output_files.append(output_file_path)

        # Add the new output files to the file map
        for entry in file_map:
            if entry["input_file"] == input_path:
                entry["output_files"] = workflow_output_files
                entry["input_last_modified"] = input_last_modified
                break

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

    # Delete the output files of files that are no longer in the input folder
    for entry in file_map:
        if not entry["seen"]:
            for output_file in entry["output_files"]:
                with contextlib.suppress(Exception):
                    output_blob_client = blob_service_client.get_blob_client(
                        container="stage-1-container", blob=output_file
                    )
                    output_blob_client.delete_blob()

    # Remove the entries that are no longer in the input folder
    file_map = [entry for entry in file_map if entry["seen"]]

    # Remove the seen flag from the file map
    for entry in file_map:
        del entry["seen"]

    # Write the file map to a file
    file_map_file_path = os.path.join(meta_temp_folder_path, "file_map.json")

    with open(file_map_file_path, "w") as f:
        json.dump(file_map, f, indent=4, sort_keys=True, default=str)

    with open(file_map_file_path, "rb") as data:
        logger.debug(f"Uploading file map to {dependency_folder}/file_map.json")

        output_blob_client = blob_service_client.get_blob_client(
            container="stage-1-container",
            blob=f"{dependency_folder}/file_map.json",
        )

        # delete the existing file map
        with contextlib.suppress(Exception):
            output_blob_client.delete_blob()

        output_blob_client.upload_blob(data)

        logger.info(f"Uploaded file map to {dependency_folder}/file_map.json")

    # Write the workflow log to a file
    timestr = time.strftime("%Y%m%d-%H%M%S")
    file_name = f"status_report_{timestr}.csv"
    workflow_log_file_path = os.path.join(meta_temp_folder_path, file_name)

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
    deps_output = workflow_file_dependencies.write_to_file(meta_temp_folder_path)

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

    shutil.rmtree(meta_temp_folder_path)

    # dev
    # move the workflow log file and the json file to the current directory
    # shutil.move(workflow_log_file_path, "status.csv")
    # shutil.move(json_file_path, "file_map.json")


if __name__ == "__main__":
    pipeline("AI-READI")
