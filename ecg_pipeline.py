"""Process ecg data files"""

import contextlib
import datetime
import os
import tempfile
import shutil
import ecg.ecg_root as ecg
import azure.storage.blob as azureblob
import azure.storage.filedatalake as azurelake
import config
import utils.dependency as deps
import time
import csv
from traceback import format_exc
import utils.logwatch as logging
from utils.file_map_processor import FileMapProcessor


def pipeline(study_id: str):  # sourcery skip: low-code-quality
    """Process ecg data files for a study
    Args:
        study_id (str): the study id
    """

    if study_id is None or not study_id:
        raise ValueError("study_id is required")

    input_folder = f"{study_id}/pooled-data/ECG"
    processed_data_output_folder = f"{study_id}/pooled-data/ECG-processed"
    dependency_folder = f"{study_id}/dependency/ECG"
    pipeline_workflow_log_folder = f"{study_id}/logs/ECG"
    data_plot_output_folder = f"{study_id}/pooled-data/ECG-dataplot"
    ignore_file = f"{study_id}/ignore/ecg"

    logger = logging.Logwatch("ecg", print=True)

    sas_token = azureblob.generate_account_sas(
        account_name="b2aistaging",
        account_key=config.AZURE_STORAGE_ACCESS_KEY,
        resource_types=azureblob.ResourceTypes(container=True, object=True),
        permission=azureblob.AccountSasPermissions(read=True, write=True, list=True),
        expiry=datetime.datetime.now(datetime.timezone.utc)
        + datetime.timedelta(hours=1),
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
    with contextlib.suppress(Exception):
        file_system_client.delete_directory(processed_data_output_folder)

    with contextlib.suppress(Exception):
        file_system_client.delete_directory(data_plot_output_folder)

    paths = file_system_client.get_paths(path=input_folder)

    file_paths = []
    for path in paths:
        t = str(path.name)

        file_name = t.split("/")[-1]

        # Check if the item is an xml file
        if file_name.split(".")[-1] != "xml":
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
                "convert_error": True,
                "output_uploaded": False,
                "output_files": [],
            }
        )

    logger.debug(f"Found {len(file_paths)} files in {input_folder}")

    # Create a temporary folder on the local machine
    temp_folder_path = tempfile.mkdtemp()

    # Create the output folder
    file_system_client.create_directory(processed_data_output_folder)

    # Create a temporary folder on the local machine
    meta_temp_folder_path = tempfile.mkdtemp()

    file_processor = FileMapProcessor(dependency_folder, ignore_file)

    workflow_file_dependencies = deps.WorkflowFileDependencies()

    total_files = len(file_paths)

    for idx, file_item in enumerate(file_paths):
        log_idx = idx + 1

        if log_idx == 60:
            break

        path = file_item["file_path"]

        workflow_input_files = [path]

        logger.debug(f"Processing {path} - ({log_idx}/{total_files})")

        # get the file name from the path
        file_name = path.split("/")[-1]

        should_files_ignored = file_processor.is_file_ignored(file_item, path)

        if should_files_ignored:
            logger.info(f"Ignoring {file_name} - ({log_idx}/{total_files})")
            continue

        # download the file to the temp folder
        blob_client = blob_service_client.get_blob_client(
            container="stage-1-container", blob=path
        )

        # should_process = True
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

        download_path = os.path.join(temp_folder_path, file_name)

        with open(download_path, "wb") as data:
            blob_client.download_blob().readinto(data)

        logger.info(f"Downloaded {file_name} to {download_path} - ({log_idx}/{total_files})")

        ecg_path = download_path

        ecg_temp_folder_path = tempfile.mkdtemp()
        wfdb_temp_folder_path = tempfile.mkdtemp()

        xecg = ecg.ECG()

        try:
            conv_retval_dict = xecg.convert(
                ecg_path, ecg_temp_folder_path, wfdb_temp_folder_path
            )
        except Exception:
            logger.error(
                f"Failed to convert {file_name} - ({log_idx}/{total_files})"
            )
            error_exception = format_exc()
            error_exception = "".join(error_exception.splitlines())

            file_processor.append_errors(error_exception, path)
            continue

        file_item["convert_error"] = False
        file_item["processed"] = True

        logger.debug(f"Converted {file_name} - ({log_idx}/{total_files})")

        output_files = conv_retval_dict["output_files"]
        participant_id = conv_retval_dict["participantID"]

        logger.debug(
            f"Uploading outputs of {file_name} to {processed_data_output_folder} - ({log_idx}/{total_files})"
        )

        # file is in the format 1001_ecg_25aafb4b.dat

        workflow_output_files = []

        outputs_uploaded = True
        upload_exception = ""

        file_processor.delete_preexisting_output_files(path)

        for file in output_files:

            with open(f"{file}", "rb") as data:

                file_name2 = file.split("/")[-1]

                output_file_path = f"{processed_data_output_folder}/ecg_12lead/philips_tc30/{participant_id}/{file_name2}"

                # output_blob_client = blob_service_client.get_blob_client(
                #     container="stage-1-container",
                #     blob=output_file_path,
                # )
                # output_blob_client.upload_blob(data)
                try:
                    output_blob_client = blob_service_client.get_blob_client(
                        container="stage-1-container",
                        blob=output_file_path,
                    )
                    output_blob_client.upload_blob(data)
                except Exception as e:
                    logger.error(f"Failed to upload {file} - ({log_idx}/{total_files})")
                    error_exception = format_exc()
                    error_exception = "".join(error_exception.splitlines())

                    file_processor.append_errors(error_exception, path)

                    continue

                file_item["output_files"].append(output_file_path)
                workflow_output_files.append(output_file_path)

        # Add the new output files to the file map
        file_processor.confirm_output_files(path, workflow_output_files, input_last_modified)

        if outputs_uploaded:
            file_item["output_uploaded"] = True
            file_item["status"] = "success"
            logger.info(
                f"Uploaded outputs of {file_name} to {processed_data_output_folder} - ({log_idx}/{total_files})"
            )
        else:
            file_item["output_uploaded"] = upload_exception
            logger.error(
                f"Failed to upload outputs of {file_name} to {processed_data_output_folder} - ({log_idx}/{total_files})"
            )

        workflow_file_dependencies.add_dependency(
            workflow_input_files, workflow_output_files
        )

        # Do the data plot
        # logger.debug(f"Data plotting {file_name} - ({log_idx}/{total_files})")

        # dataplot_retval_dict = xecg.dataplot(conv_retval_dict, ecg_temp_folder_path)

        # logger.debug(f"Data plotted {file_name} - ({log_idx}/{total_files})")

        # dataplot_pngs = dataplot_retval_dict["output_files"]

        # logger.debug(
        #     f"Uploading {file_name} to {data_plot_output_folder} - ({log_idx}/{total_files}"
        # )

        # for file in dataplot_pngs:
        #     with open(f"{file}", "rb") as data:
        #         file_name = file.split("/")[-1]
        #         output_blob_client = blob_service_client.get_blob_client(
        #             container="stage-1-container",
        #             blob=f"{data_plot_output_folder}/{file_name}",
        #         )
        #         output_blob_client.upload_blob(data)

        # logger.debug(
        #     f"Uploaded {file_name} to {data_plot_output_folder} - ({log_idx}/{total_files}"
        # )

        # Create the file metadata

        # logger.debug(f"Creating metadata for {file_name} - ({log_idx}/{total_files})")

        # output_hea_file = conv_retval_dict["output_hea_file"]

        # hea_metadata = xecg.metadata(output_hea_file)
        # logger.debug(hea_metadata)

        # logger.debug(f"Metadata created for {file_name} - ({log_idx}/{total_files})")

        shutil.rmtree(ecg_temp_folder_path)
        shutil.rmtree(wfdb_temp_folder_path)
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
    workflow_log_file_path = os.path.join(temp_folder_path, file_name)

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

        for file_item in file_paths:
            file_item["output_files"] = ";".join(file_item["output_files"])

        writer.writeheader()
        writer.writerows(file_paths)

    with open(workflow_log_file_path, mode="rb") as data:
        logger.debug(f"Uploading workflow log to {pipeline_workflow_log_folder}/{file_name}")

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

    shutil.rmtree(meta_temp_folder_path)

    # dev
    # move the workflow log file and the json file to the current directory
    # shutil.move(workflow_log_file_path, "status.csv")
    # shutil.move(json_file_path, "file_map.json")


if __name__ == "__main__":
    pipeline("AI-READI")

    # delete the ecg.log file
    if os.path.exists("ecg.log"):
        os.remove("ecg.log")
