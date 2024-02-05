import os
import config
from azure.storage.filedatalake import FileSystemClient
import azure.functions as func


class MoveException(Exception):
    pass


def move_directory(overwrite_permitted):
    file_system = FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_CONNECTION_STRING,
        file_system_name="stage-1-container",
    )
    dir_name: str = "AI-READI/metadata/test2/sub4"
    new_dir_name: str = "AI-READI/metadata/test2/sub5"
    source_path = file_system.get_directory_client(dir_name)
    destination_path = file_system.get_directory_client(new_dir_name)

    if overwrite_permitted != "true" and overwrite_permitted != "false":
        raise MoveException('overwrite-permitted must be "true" or "false"')

    if overwrite_permitted == "false" and destination_path.exists():
        raise MoveException("overwriting directories is not accepted")

    if destination_path.exists() and source_path.exists():
        destination_path.delete_directory()

    source_path.rename_directory(
        new_name=f"{source_path.file_system_name}/{new_dir_name}"
    )

