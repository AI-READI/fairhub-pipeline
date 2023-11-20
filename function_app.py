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
import logging
from pathlib import Path
from os import environ
import azure.storage.blob as azureblob
from dotenv import dotenv_values
import uuid
import io


# Check if `.env` file exists
env_path = Path(".") / ".env"

# Load environment variables from .env
config = dotenv_values(".env")

app = func.FunctionApp()

logging.debug("Function app created")


def get_env(key):
    """Return environment variable from .env or native environment."""
    return config.get(key) if env_path.exists() else environ.get(key)

@app.route(route='hello', auth_level=func.AuthLevel.ANONYMOUS)
def hello(req: func.HttpRequest) -> func.HttpResponse:
    """Return a simple hello world."""
    return func.HttpResponse("Hello world!")

@app.route(route='echo', auth_level=func.AuthLevel.ANONYMOUS)'
def echo(req: func.HttpRequest) -> func.HttpResponse:
    """Echo the request body back as a response."""
    return func.HttpResponse(req.get_body(), status_code=200, mimetype='text/plain')


@app.route(route="preprocess-stage-one", auth_level=func.AuthLevel.FUNCTION)
def preprocess_stage_one(req: func.HttpRequest) -> func.HttpResponse:
    """Reads the data in the stage-1-container. Each file name is added to a log file in the logs folder for the study.
    Will also create an output file with a modified name to simulate a processing step.
    POC so this is just a test to see if we can read the files in the stage-1-container.
    """

    # https://3.basecamp.com/3686773/buckets/29105173/todos/6722990109#__recording_6742515899
    # conversation of the folder structure
    input_folder = "AI-READI/pooled_data/"
    output_folder = "AI-READI/processed_data/"

    logs_folder = "AI-READI/logs/"
    temp_folder = "AI-READI/temp/"

    sas_token = azureblob.generate_account_sas(
        account_name="b2aistaging",
        account_key=get_env("AZURE_STORAGE_CONNECTION_STRING"),
        resource_types=azureblob.ResourceTypes(container=True, object=True),
        permission=azureblob.AccountSasPermissions(read=True, write=True, list=True),
        expiry=datetime.datetime.utcnow() + datetime.timedelta(hours=1),
    )

    # Get the blob service client
    blob_service_client = azureblob.BlobServiceClient(
        account_url="https://b2aistaging.blob.core.windows.net/",
        credential=sas_token,
    )

    # Get the container client for the sources and destinations
    input_container_client = blob_service_client.get_container_client(
        "stage-1-container"
    )

    # Create a temporary folder for this workflow
    workflow_id = uuid.uuid4()

    # Create a temp file in the temp folder on the blob storage
    temp_blob_client = blob_service_client.get_blob_client(
        container="stage-1-container", blob=f"{temp_folder}{workflow_id}.temp"
    )
    temp_file = io.BytesIO()
    temp_blob_client.upload_blob(temp_file)

    # get the list of blobs in the input folder
    data_type_list = input_container_client.list_blobs(name_starts_with=input_folder)

    for data_type in data_type_list:
        logging.debug("data_type: %s", data_type)

        # get the list of blobs in the input folder
        blob_list = input_container_client.list_blobs(name_starts_with=data_type)

        for blob in blob_list:
            # get the blob client for the blob
            input_blob_client = blob_service_client.get_blob_client(
                container="stage-1-container", blob=blob.name
            )

            print(f"Processing blob: {blob.name}")

            # if the blob is a folder, skip it
            # will need to recurse for this probably
            # if blob.name.endswith("/"):
            #     continue

            download_stream = input_blob_client.download_blob().readall()

            # Upload the blob to the output folder
            output_blob_client = blob_service_client.get_blob_client(
                container="stage-1-container", blob=f"{output_folder}{blob.name}"
            )
            output_blob_client.upload_blob(download_stream.readall())

    return func.HttpResponse(
        f"Processed blobs.\n", status_code=200, mimetype="text/plain"
    )
