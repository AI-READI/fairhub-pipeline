import contextlib
import datetime
import os
import tempfile
import azure.storage.blob as azureblob
import config

import utils.logwatch as logging
import json


class FileProcessor:
    def __init__(self, study_id):
        if study_id is None or not study_id:
            raise ValueError("study_id is required")

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
        blob_service_client = azureblob.BlobServiceClient(
            account_url="https://b2aistaging.blob.core.windows.net/",
            credential=sas_token,
        )
        dependency_folder = f"{study_id}/dependency/Eidon"

        file_map = []
        meta_temp_folder_path = tempfile.mkdtemp()
        logger = logging.Logwatch("eidon", print=True)

        # Delete the output files that are no longer in the input folder
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
