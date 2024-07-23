
import contextlib
import datetime
import os
import tempfile
import azure.storage.blob as azureblob
from .. import config

import json


class FileMapProcessor:
    """ Class for processing files """

    def __init__(self, study_id: str, logger, blob_service_client):

        self.study_id = study_id
        self.logger = logger
        self.blob_service_client = blob_service_client

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

    file_map = []
    for entry in file_map:
        if not entry["seen"]:
            for output_file in entry["output_files"]:
                with contextlib.suppress(Exception):
                    output_blob_client = blob_service_client.get_blob_client(
                        container="stage-1-container", blob=output_file
                    )
                    output_blob_client.delete_blob()

    def adding_files(self, blob_service_client):
        file_map = []
        for entry in file_map:
            if not entry["seen"]:
                for output_file in entry["output_files"]:
                    with contextlib.suppress(Exception):
                        output_blob_client = blob_service_client.get_blob_client(
                            container="stage-1-container", blob=output_file
                        )
                        output_blob_client.delete_blob()

        file_map = []

        file_map = [entry for entry in file_map if entry["seen"]]

        # Remove the seen flag from the file map
        for entry in file_map:
            del entry["seen"]

    def render_json(self, study_id, logger, blob_service_client):

        # Write the file map to a file
        dependency_folder = f"{study_id}/dependency/Eidon"
        file_map = []
        meta_temp_folder_path = tempfile.mkdtemp()
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

