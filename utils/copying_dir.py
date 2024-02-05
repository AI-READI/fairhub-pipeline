import os
import config
from azure.storage.filedatalake import FileSystemClient


class CopyException(Exception):
    pass


def copy_directory(overwrite_permitted: bool) -> None:
    """Moving directories while implementing subsequent copies (recursion)"""
    file_system = FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_CONNECTION_STRING,
        file_system_name="stage-1-container",
    )
    dir_name: str = "AI-READI/metadata/test2/sub4"
    destination: str = "AI-READI/metadata/test2/sub5"
    directory_path = file_system.get_directory_client(dir_name)
    source: str = directory_path.get_directory_properties().name

    directory_client = file_system.get_directory_client(destination)

    if destination.lower().startswith(source.lower()):
        raise Exception("the destination is inside of the source")
    if not directory_client.exists():
        directory_client.create_directory()
    else:
        if not overwrite_permitted:
            raise Exception("overwriting directories is not accepted")
    for path in file_system.get_paths(source, recursive=False):
        target = (
                destination + "/" + os.path.basename(path.name.rstrip("/").rstrip("\\"))
        )
        if not path.is_directory:
            source_file = file_system.get_file_client(path.name)

            destination_file = file_system.get_file_client(target)

            source_file_bytes = source_file.download_file().readall()

            if not destination_file.exists():
                destination_file.create_file()
            destination_file.upload_data(source_file_bytes, overwrite=True)

        else:
            copy_directory(overwrite_permitted)


def copying_permissions(overwrite_permitted):
    if overwrite_permitted != "true" and overwrite_permitted != "false":
        raise CopyException("Only overwrite-permitted=true or overwrite-permitted=false accepted")

