"""Download a folder from the raw storage account and save it in the local folder"""

import os
import azure.storage.filedatalake as azurelake  # type: ignore
import config

completed_folders_file = "completed_folders_garmin.json"


def main():  # sourcery skip: low-code-quality
    source_directory = "AI-READI/heidelberg_octa_test"
    # source_directory = "AI-READI/dependency/Cirrus"
    local_download_folder = r"C:\Users\b2aiUsr\Desktop\heidelberg_octa_test"

    if not os.path.exists(local_download_folder):
        os.makedirs(local_download_folder)

    # create datalake clients
    source_service_client = azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_CONNECTION_STRING, file_system_name="stage-1-container"
    )

    print(f"Downloading folder {source_directory} to {local_download_folder}")

    source_folder_file_paths = source_service_client.get_paths(
        path=source_directory, recursive=True
    )

    for item in source_folder_file_paths:
        print(f"Found item {item.name}")
        remote_file_path = item.name.replace("/", "\\")
        # Download the file
        file_client = source_service_client.get_file_client(file_path=item.name)

        file_properties = file_client.get_file_properties().metadata

        ff = source_directory.replace("/", "\\")
        file_name = remote_file_path.replace(ff, "")[1:]
        print(f"ff is {ff} and file_name is {file_name}")

        # Check if item is a directory
        if file_properties.get("hdi_isfolder"):
            print("file path is a directory. Creating directory")
            print(f"Creating directory {remote_file_path} as {file_name}")

            # Create the directory
            file_path = os.path.join(local_download_folder, file_name)
            os.makedirs(file_path, exist_ok=True)

            continue

        file_path = os.path.join(local_download_folder, file_name)
        print(f"Downloading file {remote_file_path} to {file_path}")

        with open(file=file_path, mode="wb") as f:
            f.write(file_client.download_file().readall())


if __name__ == "__main__":
    main()
