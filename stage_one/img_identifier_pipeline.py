"""Process environmental sensor data files"""
import datetime
import json
import os
import tempfile
import uuid

import azure.storage.blob as azureblob
import azure.storage.filedatalake as azurelake

import config
from utils.image_classifying_rules import extract_env_info, process_dicom_zip


def pipeline():
    """Does something"""

    def data_identifier(zip_file_path):
        if not zip_file_path.endswith(".zip"):
            return "Not a zip file"

        elif "ENV" in zip_file_path:
            return extract_env_info(zip_file_path)

        elif any(
            word in zip_file_path
            for word in [
                "Optomed",
                "Eidon",
                "Maestro",
                "Triton",
                "FLIO",
                "Cirrus",
                "Spectralis",
            ]
        ):
            return process_dicom_zip(zip_file_path)

        else:
            return "Unknown file type"

    input_folder = "AI-READI/pooled-data"
    logs_folder = "AI-READI/logs/"

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

    # Create a temporary folder for this workflow
    workflow_id = uuid.uuid4()

    # Get the list of blobs in the input folder
    file_system_client = azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_CONNECTION_STRING,
        file_system_name="stage-1-container",
    )

    paths = file_system_client.get_paths(path=input_folder)

    str_paths = []

    for path in paths:
        t = str(path.name)
        str_paths.append(t)

    # generate temp file for logs
    temp_log_file, temp_log_file_path = tempfile.mkstemp(suffix=".n.test.log")

    # Create a temporary folder on the local machine
    temp_folder_path = tempfile.mkdtemp()

    extracted_metadata = []

    for path in str_paths:
        print(path)

        # get the file name from the path
        file_name = path.split("/")[-1]

        # skip if the path is a folder (check if extension is empty)
        if not file_name.split(".")[-1]:
            continue

        # download the file to the temp folder
        blob_client = blob_service_client.get_blob_client(
            container="stage-1-container", blob=path
        )

        download_path = os.path.join(temp_folder_path, file_name)

        with open(download_path, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())

            # process the file

            file_info = data_identifier(download_path)

            extracted_metadata.append(
                {
                    "file_name": file_name,
                    "file_info": file_info,
                }
            )

        # remove the file from the temp folder
        os.remove(download_path)

    # remove the temp folder
    os.rmdir(temp_folder_path)

    # write the paths to the log file
    with open(temp_log_file_path, mode="w", encoding="utf-8") as f:
        formatted_text = json.dumps(extracted_metadata, indent=4)
        f.write(formatted_text)

    # upload the log file to the logs folder
    log_blob_client = blob_service_client.get_blob_client(
        container="stage-1-container", blob=f"{logs_folder}{workflow_id}.n.test.log"
    )

    with open(temp_log_file_path, "rb") as data:
        log_blob_client.upload_blob(data)
