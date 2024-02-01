import os
import config
from azure.storage.filedatalake import FileSystemClient
import azure.functions as func


def moving_dirs(req, source_path, destination_path):
    overwrite_permitted = (
        req.params["overwrite-permitted"]
        if "overwrite-permitted" in req.params
        else "true"
    )

    if overwrite_permitted != "true" and overwrite_permitted != "false":
        return func.HttpResponse(
            "Only overwrite-permitted=true or overwrite-permitted=false accepted",
            status_code=403,
        )
    if overwrite_permitted == "false" and destination_path.exists():
        return func.HttpResponse(
            "overwriting directories is not accepted", status_code=500
        )
    if destination_path.exists() and source_path.exists():
        destination_path.delete_directory()


def copy_directory(
        file_system: FileSystemClient,
        source: str,
        destination: str,
        overwrite_permitted: bool,
) -> None:
    """Moving directories while implementing subsequent copies (recursion)"""
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
            copy_directory(file_system, path.name, target, overwrite_permitted)
            # check if dir is empty:
            #     delete_directory


def copying_dirs_beginning(req):
    overwrite_permitted = (
        req.params["overwrite-permitted"]
        if "overwrite-permitted" in req.params
        else "true"
    )

    if overwrite_permitted != "true" and overwrite_permitted != "false":
        return func.HttpResponse(
            "Only overwrite-permitted=true or overwrite-permitted=false accepted",
            status_code=403,
        )
