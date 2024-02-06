import os
from typing import Callable
import config
from azure.storage.filedatalake import FileSystemClient
import azure.functions as func


class FileException(Exception):
    pass


def move_directory(container: str, source: str, destination: str, overwrite_permitted: bool):
    file_system = FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_CONNECTION_STRING,
        file_system_name=container,
    )

    source_client = file_system.get_directory_client(source)
    destination_client = file_system.get_directory_client(destination)
    if not source_client.exists():
        raise FileException("source directory does not exist!")

    if not overwrite_permitted and destination_client.exists():
        raise FileException("overwriting directories is not accepted")

    if destination_client.exists() and source_client.exists():
        destination_client.delete_directory()

    source_client.rename_directory(
        new_name=f"{source_client.file_system_name}/{destination}"
    )


def copy_directory(container: str, source: str, destination: str, overwrite_permitted: bool) -> None:
    """Moving directories while implementing subsequent copies (recursion)"""
    file_system = FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_CONNECTION_STRING,
        file_system_name=container,
    )
    source_client = file_system.get_directory_client(source)
    if not source_client.exists():
        raise FileException("source directory does not exist!")
    source_path: str = source_client.get_directory_properties().name

    destination_client = file_system.get_directory_client(destination)

    if destination.lower().startswith(source_path.lower()):
        raise FileException("the destination is inside of the source")
    if not destination_client.exists():
        destination_client.create_directory()
    if not overwrite_permitted:
        raise FileException("overwriting directories is not accepted")

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
            copy_directory(container, child_path, target_path, overwrite_permitted)


def file_operation(operation: Callable, req: func.HttpRequest) -> func.HttpResponse:
    overwrite_permitted = (
        req.params["overwrite-permitted"]
        if "overwrite-permitted" in req.params
        else "true"
    )

    if overwrite_permitted not in ["true", "false"]:
        return func.HttpResponse("Overwrite-permitted must be true or false", status_code=500)

    overwrite: bool = overwrite_permitted.lower().strip() == "true"

    source: str = "AI-READI/metadata/test2/sub4"
    destination: str = "AI-READI/metadata/test2/sub5"
    container = "stage-1-container"

    try:
        operation(container, source, destination, overwrite)
        return func.HttpResponse("Success", status_code=200)
    except FileException as e:
        return func.HttpResponse(e.args[0], status_code=500)
    except Exception as e:
        print(f"Exception: {e}")
    return func.HttpResponse("Internal Server Error", status_code=500)