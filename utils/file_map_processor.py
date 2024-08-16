import contextlib
import datetime
import os
import tempfile

import azure.storage.blob as azureblob
import config
import shutil

import json


class FileMapProcessor:
    """Class for handling file processing"""

    def __init__(self, dependency_folder: str, ignore_file=None):

        self.file_map = []
        # where actually ignored files are stored in the array
        self.ignore_files = []
        self.dependency_folder = dependency_folder
        # ignored file path on the azure

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
        self.meta_temp_folder_path = tempfile.mkdtemp()

        file_map_download_path = os.path.join(
            self.meta_temp_folder_path, "file_map.json"
        )

        meta_blob_client = self.blob_service_client.get_blob_client(
            container="stage-1-container", blob=f"{dependency_folder}/file_map.json"
        )
        if ignore_file:
            # ignore File name coming from the ignore file path
            ignored_file_name = ignore_file.split("/")[-1]

            ignore_file_download_path = os.path.join(
                self.meta_temp_folder_path, ignored_file_name
            )
            ignore_meta_blob_client = self.blob_service_client.get_blob_client(
                container="stage-1-container", blob=ignore_file
            )
            # Download the meta file for the pipeline
            with contextlib.suppress(Exception):
                with open(ignore_file_download_path, "wb") as data:
                    ignore_meta_blob_client.download_blob().readinto(data)

                # Read the ignore file
                with open(ignore_file_download_path, "r") as f:
                    self.ignore_files = f.readlines()
            # Save trimmed file names
            self.ignore_files = [x.strip() for x in self.ignore_files]

        # Downloading file map
        with contextlib.suppress(Exception):
            with open(file_map_download_path, "wb") as data:
                meta_blob_client.download_blob().readinto(data)

            # Load the meta file
            with open(file_map_download_path, "r") as f:
                self.file_map = json.load(f)

        for entry in self.file_map:
            # This is to delete the output files of files that are no longer in the input folder
            entry["seen"] = False

    def add_entry(self, path, input_last_modified):
        # Add files that do not exist in the array
        entry = [entry for entry in self.file_map if entry["input_file"] == path]
        if len(entry) == 0:
            self.file_map.append(
                {
                    "input_file": path,
                    "output_files": [],
                    "input_last_modified": input_last_modified,
                    "seen": True,
                    "error": [],
                }
            )

    def file_should_process(self, path, input_last_modified) -> bool:
        """Check if the file has been modified since the last time it was
        processed and no errors exist during processing"""
        for entry in self.file_map:
            if entry["input_file"] == path:
                entry["seen"] = True

                t = input_last_modified.strftime("%Y-%m-%d %H:%M:%S+00:00")
                count_error = len(entry["error"])

                return t != entry["input_last_modified"] or count_error > 0

        return True

    def confirm_output_files(self, path, workflow_output_files, input_last_modified):
        # Add the new output files to the file map
        for entry in self.file_map:
            if entry["input_file"] == path:
                entry["output_files"] = workflow_output_files
                entry["input_last_modified"] = input_last_modified
                break

    def delete_preexisting_output_files(self, path):
        input_path = path
        # Delete the output files associated with the input file
        # We are doing a file level replacement
        for entry in self.file_map:
            if entry["input_file"] == input_path:
                for output_file in entry["output_files"]:
                    with contextlib.suppress(Exception):
                        output_blob_client = self.blob_service_client.get_blob_client(
                            container="stage-1-container", blob=output_file
                        )
                        output_blob_client.delete_blob()
                break

    def delete_out_of_date_output_files(self):
        # Delete the output files that are no longer in the input folder
        for entry in self.file_map:
            if not entry["seen"]:
                for output_file in entry["output_files"]:
                    with contextlib.suppress(Exception):
                        output_blob_client = self.blob_service_client.get_blob_client(
                            container="stage-1-container", blob=output_file
                        )
                        output_blob_client.delete_blob()

    def append_errors(self, error_exception, path):
        # This function appends errors to the json
        entry = [entry for entry in self.file_map if entry["input_file"] == path][0]
        entry["error"].append(error_exception)

    def clear_errors(self, path):
        # This function clear errors to the json
        entry = [entry for entry in self.file_map if entry["input_file"] == path][0]
        entry["error"] = []

    def remove_seen_flag_from_map(self):
        # Remove the entries that are no longer in the input folder
        self.file_map = [entry for entry in self.file_map if entry["seen"]]

        # Remove the seen flag from the file map
        for entry in self.file_map:
            del entry["seen"]

    def upload_json(self):
        # Write the file map to a file
        file_map_file_path = os.path.join(self.meta_temp_folder_path, "file_map.json")

        with open(file_map_file_path, "w") as f:
            json.dump(self.file_map, f, indent=4, sort_keys=True, default=str)
        with open(file_map_file_path, "rb") as data:
            output_blob_client = self.blob_service_client.get_blob_client(
                container="stage-1-container",
                blob=f"{self.dependency_folder}/file_map.json",
            )
            # delete the existing file map
            with contextlib.suppress(Exception):
                output_blob_client.delete_blob()

            output_blob_client.upload_blob(data)
        shutil.rmtree(self.meta_temp_folder_path)

    def is_file_ignored(self, file_name, path) -> bool:
        return file_name in self.ignore_files or path in self.ignore_files
