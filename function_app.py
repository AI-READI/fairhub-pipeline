# flake8: noqa
# type: ignore
# pylint: disable=unused-argument
# pylint: disable=unused-variable
# pylint: disable=unused-import
# pylint: disable=missing-function-docstring
# pylint: disable=missing-module-docstring
# pylint: disable=wrong-import-order
# pylint: disable=wrong-import-position
# pylint: disable=line-too-long
# pylint: disable=import-error
import azure.functions as func
import datetime
import json
import logging
from pathlib import Path
from os import environ
import azure.storage.blob as azureblob
from dotenv import dotenv_values

logging.Logger.root.level = 10
logging.debug("Debug message here")


# Check if `.env` file exists
env_path = Path(".") / ".env"

# Load environment variables from .env
config = dotenv_values(".env")

app = func.FunctionApp()

logging.debug("Function app created")


def get_env(key):
    """Return environment variable from .env or native environment."""
    return config.get(key) if env_path.exists() else environ.get(key)


@app.route(route="test", auth_level=func.AuthLevel.FUNCTION)
def test2(req: func.HttpRequest) -> func.HttpResponse:
    # Move a folder from the `raw-storage` container to the `stage-1-container` container
    # input folder: https://b2aistaging.blob.core.windows.net/raw-storage/AI-READI/pooled_data/UW/CGM

    input_folder = "AI-READI/pooled_data/UW/CGM/"

    # output folder: https://b2aistaging.blob.core.windows.net/stage-1-container/AI-READI/pooled_data/UW/CGM

    output_folder = "AI-READI/pooled_data/UW/CGM/"

    logging.debug("Getting the SAS token")
    logging.debug(get_env("AZURE_STORAGE_CONNECTION_STRING"))

    sas_token = azureblob.generate_account_sas(
        account_name="b2aistaging",
        account_key=get_env("AZURE_STORAGE_CONNECTION_STRING"),
        resource_types=azureblob.ResourceTypes(container=True, object=True),
        permission=azureblob.AccountSasPermissions(read=True, write=True, list=True),
        expiry=datetime.datetime.utcnow() + datetime.timedelta(hours=1),
    )

    logging.debug(sas_token)

    logging.debug("Starting the blob copy operation")
    logging.debug("Input folder: %s", input_folder)
    logging.debug("Output folder: %s", output_folder)

    logging.debug("Creating the blob service client")

    # Get the blob service client
    blob_service_client = azureblob.BlobServiceClient(
        account_url="https://b2aistaging.blob.core.windows.net/",
        credential=sas_token,
    )

    logging.debug("Blob service client created")
    logging.debug("Getting the container clients")

    logging.debug("Getting the input container client")

    # Get the container client for the input folder
    input_container_client = blob_service_client.get_container_client("raw-storage")

    logging.debug("Input container client created")
    logging.debug("Getting the output container client")

    # Get the container client for the output folder
    output_container_client = blob_service_client.get_container_client(
        "stage-1-container"
    )

    logging.debug("Output container client created")

    logging.debug("Getting the list of blobs in the input folder")

    # get the list of blobs in the input folder
    blob_list = input_container_client.list_blobs(name_starts_with=input_folder)

    logging.debug("List of blobs in the input folder created")

    logging.debug("blob_list: %s", blob_list)

    logging.debug("Starting the blob copy operation")

    # move each blob to the output folder
    for blob in blob_list:
        input_blob_client = blob_service_client.get_blob_client(
            container="raw-storage", blob=blob.name
        )
        output_blob_client = blob_service_client.get_blob_client(
            container="stage-1-container", blob=blob.name
        )

        # download the blob from the input folder
        download_stream = input_blob_client.download_blob()

        # upload the blob to the output folder
        output_blob_client.upload_blob(download_stream.readall())

        # input_blob_client.delete_blob() # uncomment this line to delete the blob from the input folder

        print(f"Moved {blob.name} from raw-storage to stage-1-container")

    return func.HttpResponse(
        f"Moved {input_folder} from raw-storage to stage-1-container"
    )
