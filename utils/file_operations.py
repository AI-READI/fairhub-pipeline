import os
from typing import Callable, List
from azure.storage.filedatalake import FileSystemClient
import azure.functions as func
import datetime

import azure.storage.blob as azureblob
import psycopg2

import config


class FileException(Exception):
    pass


class FileStructure:
    label: str

    def __init__(self, label: str):
        self.label = label

    def to_dict(self):
        return {
            "label": self.label
        }


class FolderStructure(FileStructure):
    children: List['FileStructure']

    def __init__(self, label: str, children: List['FileStructure']):
        super().__init__(label)
        self.children = children

    def to_dict(self):
        return {
            "children": [child.to_dict() for child in self.children],
            "label": self.label
        }


def move_directory(file_system: FileSystemClient, source: str, destination: str, overwrite_permitted: bool):
    source_client = file_system.get_directory_client(source)
    destination_client = file_system.get_directory_client(destination)

    if not source_client.exists():
        raise FileException("source directory does not exist!")

    if not overwrite_permitted and destination_client.exists():
        raise FileException("overwriting directories is not accepted")
    if destination_client.exists() and source_client.exists():
        destination_client.delete_directory()
    if not destination_client.exists():
        source_client.rename_directory(
            new_name=f"{source_client.file_system_name}/{destination}"
        )


def copy_directory(file_system: FileSystemClient, source: str, destination: str, overwrite_permitted: bool) -> None:
    """Moving directories while implementing subsequent copies (recursion)"""

    source_client = file_system.get_directory_client(source)

    if not source_client.exists():
        raise FileException("source directory does not exist!")

    if not overwrite_permitted:
        raise FileException("overwriting directories is not accepted")
    source_path: str = source_client.get_directory_properties().name

    destination_client = file_system.get_directory_client(destination)

    if destination.lower().startswith(source_path.lower()):
        raise FileException("the destination is inside of the source")
    if not destination_client.exists():
        destination_client.create_directory()

    for child_path in file_system.get_paths(source_path, recursive=False):
        target_path = f"{destination}/" + os.path.basename(
            child_path.name.rstrip("/").rstrip("\\")
        )
        if not child_path.is_directory:
            source_file = file_system.get_file_client(child_path.name)

            destination_file = file_system.get_file_client(target_path)

            source_file_bytes = source_file.download_file().readall()

            if not destination_file.exists():
                destination_file.create_file()
            destination_file.upload_data(source_file_bytes, overwrite=True)
        else:
            copy_directory(file_system, child_path, target_path, overwrite_permitted)


def file_operation(operation: Callable, req: func.HttpRequest) -> func.HttpResponse:
    overwrite_permitted = (
        req.params["overwrite-permitted"]
        if "overwrite-permitted" in req.params
        else "true"
    )

    if overwrite_permitted not in ["true", "false"]:
        return func.HttpResponse("Overwrite-permitted must be true or false", status_code=500)

    overwrite: bool = overwrite_permitted.lower().strip() == "true"

    source: str = "AI-READI/metadata/test2/t1"
    destination: str = "AI-READI/metadata/test2/t2"
    container = "stage-1-container"

    try:
        file_system = FileSystemClient.from_connection_string(
            config.AZURE_STORAGE_CONNECTION_STRING,
            file_system_name=container,
        )
        operation(file_system, source, destination, overwrite)
        return func.HttpResponse("Success", status_code=200)
    except FileException as e:
        return func.HttpResponse(e.args[0], status_code=500)
    except Exception as e:
        print(f"Exception: {e}")
    return func.HttpResponse("Internal Server Error", status_code=500)


def get_file_tree():
    container = "stage-1-container"
    file_system = FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_CONNECTION_STRING,
        file_system_name=container,
    )
    source: str = "AI-READI/metadata/test2/t1"

    return recurse_file_tree(file_system, source)


def recurse_file_tree(file_system: FileSystemClient, source: str) -> FileStructure:
    source_client = file_system.get_directory_client(source)

    if not source_client.exists():
        raise FileException("source directory does not exist!")

    source_path: str = source_client.get_directory_properties().name
    return FolderStructure(os.path.basename(source_path),
                           [recurse_file_tree(file_system, child_path)
                            if child_path.is_directory
                            else FileStructure(os.path.basename(child_path.name))
                            for child_path
                            in file_system.get_paths(source_path, recursive=False)
                            ])


def pipeline():
    """
        Reads the file structure from azure
    """
    license_text = ""

    conn = psycopg2.connect(
        host=config.FAIRHUB_DATABASE_HOST,
        database=config.FAIRHUB_DATABASE_NAME,
        user=config.FAIRHUB_DATABASE_USER,
        password=config.FAIRHUB_DATABASE_PASSWORD,
        port=config.FAIRHUB_DATABASE_PORT,
    )

    cur = conn.cursor()

    study_id = "c588f59c-cacb-4e52-99dd-95b37dcbfd5c"
    dataset_id = "af4be921-e507-41a9-9328-4cbb4b7dca1c"

    cur.execute(
        "SELECT * FROM dataset WHERE id = %s AND study_id = %s",
        (dataset_id, study_id),
    )
    dataset = cur.fetchone()

    if dataset is None:
        return "Dataset not found"

    conn.close()
    # upload the file to the metadata folder

    # metadata_folder = "AI-READI/metadata"
    #
    # sas_token = azureblob.generate_account_sas(
    #     account_name="b2aistaging",
    #     account_key=config.AZURE_STORAGE_ACCESS_KEY,
    #     resource_types=azureblob.ResourceTypes(container=True, object=True),
    #     permission=azureblob.AccountSasPermissions(read=True, write=True, list=True),
    #     expiry=datetime.datetime.now(datetime.timezone.utc)
    #            + datetime.timedelta(hours=1),
    # )
    #
    # # Get the blob service client
    # blob_service_client = azureblob.BlobServiceClient(
    #     account_url="https://b2aistaging.blob.core.windows.net/",
    #     credential=sas_token,
    # )
    #
    # # upload the file to the metadata folder
    # blob_client = blob_service_client.get_blob_client(
    #     container="stage-1-container",
    #     blob=f"{metadata_folder}/",
    # )
    return
