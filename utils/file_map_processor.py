
import contextlib
import datetime
import os
import tempfile
import azure.storage.blob as azureblob
import config

import json


class FileMapProcessor:
    """ Class for handling file processing """

    def __init__(self, study_id: str, logger, dependency_folder: str):

        self.study_id = study_id
        self.logger = logger

        # Initiate an empty file map
        self.file_map = []
        self.dependency_folder = dependency_folder
        # Establish azure connection
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
        self.blob_service_client = azureblob.BlobServiceClient(
            account_url="https://b2aistaging.blob.core.windows.net/",
            credential=sas_token,
        )

        # Create a temporary folder on the local machine
        meta_temp_folder_path = tempfile.mkdtemp()

        # Download the meta file for the pipeline
        file_map_download_path = os.path.join(meta_temp_folder_path, "file_map.json")

        meta_blob_client = self.blob_service_client.get_blob_client(
            container="stage-1-container", blob=f"{dependency_folder}/file_map.json"
        )

        with contextlib.suppress(Exception):
            with open(file_map_download_path, "wb") as data:
                meta_blob_client.download_blob().readinto(data)

            # Load the meta file
            with open(file_map_download_path, "r") as f:
                self.file_map = json.load(f)

        for entry in self.file_map:
            # This is to delete the output files of files that are no longer in the input folder
            entry["seen"] = False

    def add_entry(self, file_item):
        pass

    def delete_entry(self ):
        # Delete the output files that are no longer in the input folder
        for entry in self.file_map:
            if not entry["seen"]:
                for output_file in entry["output_files"]:
                    with contextlib.suppress(Exception):
                        output_blob_client = self.blob_service_client.get_blob_client(
                            container="stage-1-container", blob=output_file
                        )
                        output_blob_client.delete_blob()

            # Remove the entries that are no longer in the input folder
        file_map = [entry for entry in self.file_map if entry["seen"]]

        # Remove the seen flag from the file map
        for entry in file_map:
            del entry["seen"]

    def delete_output_file(self):
        pass

    def add_output_file(self, input_path, workflow_output_files, input_last_modified):
        for entry in self.file_map:
            if entry["input_file"] == input_path:
                entry["output_files"] = workflow_output_files
                entry["input_last_modified"] = input_last_modified
                break

    def mark_items_seen(self, path):
        pass

    def write_json(self):

        # Write the file map to a file
        dependency_folder = f"{self.study_id}/dependency/Eidon"
        meta_temp_folder_path = tempfile.mkdtemp()
        file_map_file_path = os.path.join(meta_temp_folder_path, "file_map.json")

        with open(file_map_file_path, "w") as f:
            json.dump(self.file_map, f, indent=4, sort_keys=True, default=str)

        with open(file_map_file_path, "rb") as data:
            self.logger.debug(f"Uploading file map to {dependency_folder}/file_map.json")

            output_blob_client = self.blob_service_client.get_blob_client(
                container="stage-1-container",
                blob=f"{dependency_folder}/file_map.json",
            )

            # delete the existing file map
            with contextlib.suppress(Exception):
                output_blob_client.delete_blob()

            output_blob_client.upload_blob(data)

            self.logger.info(f"Uploaded file map to {dependency_folder}/file_map.json")

