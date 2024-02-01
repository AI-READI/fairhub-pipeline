"""Process environmental sensor data files"""

import datetime
import json
import os
import tempfile
import uuid

import azure.storage.blob as azureblob
import azure.storage.filedatalake as azurelake

import config


def pipeline():
    """Reads the data in the stage-1-container. Each file name is added to a log file in the logs folder for the study.
    Will also create an output file with a modified name to simulate a processing step.
    POC so this is just a test to see if we can read the files in the stage-1-container.
    """

    def data_identifier(file):
        if "ENV" in file and file.endswith(".zip"):
            return "Environmental Sensor File"
        else:
            return "Unknown File Type"

    input_folder = "AI-READI/pooled-data/EnvSensor"
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
    temp_log_file, temp_log_file_path = tempfile.mkstemp(suffix=".env.log")

    extracted_metadata = []

    for path in str_paths:
        # get the file name from the path
        file_name = path.split("/")[-1]

        env_sensor_file = data_identifier(file_name)

        if env_sensor_file == "Environmental Sensor File":
            # split the path name by the - character
            components = file_name.split("_")
            # extract the metadata from the file name
            extracted_metadata.append(
                {
                    "file_name": file_name,
                    "site_name": components[0],
                    "data_type": components[1],
                    "site_name_2": components[2],
                    "data_type_2": components[3],
                    "date_range": components[4],
                    "prefix": components[5].split("-")[0],
                    "patient_id": components[5].split("-")[1],
                    "sensor_id": os.path.splitext(components[5].split("-")[2])[0],
                }
            )

    # write the paths to the log file
    with open(temp_log_file_path, mode="w", encoding="utf-8") as f:
        formatted_text = json.dumps(extracted_metadata, indent=4)
        f.write(formatted_text)

    # upload the log file to the logs folder
    log_blob_client = blob_service_client.get_blob_client(
        container="stage-1-container", blob=f"{logs_folder}{workflow_id}.env.log"
    )

    with open(temp_log_file_path, "rb") as data:
        log_blob_client.upload_blob(data)
