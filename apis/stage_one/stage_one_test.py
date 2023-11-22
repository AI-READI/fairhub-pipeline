"""API routes for study arm metadata"""
import datetime
import logging
import tempfile
import uuid

import azure.storage.blob as azureblob
import azure.storage.filedatalake as azurelake
from flask_restx import Resource, fields

import config
from apis.stage_one_namespace import api


test_model = api.model(
    "Test",
    {
        "id": fields.String(
            required=True,
            description="The unique identifier for the test",
            example="12345",
        ),
    },
)


@api.route("/stage-one-test")
class StageOneTest(Resource):
    """Testing POC for stage one API"""

    @api.response(200, "Success")
    @api.response(500, "Internal Server Error")
    def get(self):
        """Reads the data in the stage-1-container. Each file name is added to a log file in the logs folder for the study.
        Will also create an output file with a modified name to simulate a processing step.
        POC so this is just a test to see if we can read the files in the stage-1-container.
        """

        input_folder = "AI-READI/pooled-data/"
        output_folder = "AI-READI/processed_data/"

        logs_folder = "AI-READI/logs/"
        temp_folder = "AI-READI/temp/"

        sas_token = azureblob.generate_account_sas(
            account_name="b2aistaging",
            account_key=config.AZURE_STORAGE_ACCESS_KEY,
            resource_types=azureblob.ResourceTypes(container=True, object=True),
            permission=azureblob.AccountSasPermissions(
                read=True, write=True, list=True
            ),
            expiry=datetime.datetime.now(datetime.timezone.utc)
            + datetime.timedelta(hours=1),
        )

        # Get the blob service client
        blob_service_client = azureblob.BlobServiceClient(
            account_url="https://b2aistaging.blob.core.windows.net/",
            credential=sas_token,
        )

        # Get the container client for the sources and destinations
        # input_container_client = blob_service_client.get_container_client(
        #     "stage-1-container"
        # )

        # Create a temporary folder for this workflow
        workflow_id = uuid.uuid4()

        temp_file_name = f"{workflow_id}.temp"
        temp_file_path = f"{temp_folder}{temp_file_name}"

        # Generate a temp file locally
        temp_file, temp_file_path = tempfile.mkstemp(suffix=".temp")

        with open(temp_file_path, mode="w", encoding="utf-8") as f:
            f.write("This is a test")

        # Create a temp file in the temp folder on the blob storage
        temp_blob_client = blob_service_client.get_blob_client(
            container="stage-1-container", blob=f"{temp_folder}{temp_file_name}"
        )

        print(f"Uploading temp file to {temp_file_path}")

        # Upload the temp file to the temp folder
        with open(temp_file_path, "rb") as data:
            temp_blob_client.upload_blob(data)

        # service = azurelake.DataLakeServiceClient(
        #     account_url="https://b2aistaging.dfs.core.windows.net/",
        #     credential=sas_token,
        # )

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
        temp_log_file, temp_log_file_path = tempfile.mkstemp(suffix=".log")

        # write the paths to the log file
        with open(temp_log_file_path, mode="w", encoding="utf-8") as f:
            for path in str_paths:
                f.write(f"{path}\n")

            print(f"temp_log_file_path: {temp_log_file_path}")

        # upload the log file to the logs folder
        log_blob_client = blob_service_client.get_blob_client(
            container="stage-1-container", blob=f"{logs_folder}{workflow_id}.log"
        )

        with open(temp_log_file_path, "rb") as data:
            log_blob_client.upload_blob(data)

        return "Success", 200

        # # get the list of blobs in the input folder
        # data_type_list = input_container_client.list_blobs(
        #     name_starts_with=input_folder
        # )

        # for data_type in data_type_list:
        #     logging.debug("data_type: %s", data_type)

        #     # get the list of blobs in the input folder
        #     blob_list = input_container_client.list_blobs(name_starts_with=data_type)

        #     for blob in blob_list:
        #         # get the blob client for the blob
        #         input_blob_client = blob_service_client.get_blob_client(
        #             container="stage-1-container", blob=blob.name
        #         )

        #         print(f"Processing blob: {blob.name}")

        #         # if the blob is a folder, skip it
        #         # will need to recurse for this probably
        #         # if blob.name.endswith("/"):
        #         #     continue

        #         download_stream = input_blob_client.download_blob().readall()

        #         # Upload the blob to the output folder
        #         output_blob_client = blob_service_client.get_blob_client(
        #             container="stage-1-container", blob=f"{output_folder}{blob.name}"
        #         )
        #         output_blob_client.upload_blob(download_stream.readall())
