"""Process cirrus data files and pool data into a .zip file"""

import subprocess
import sys
import os
import zipfile
import tempfile
import azure.storage.filedatalake as azurelake

import config

# & "C:\Users\b2aiUsr\.scripts\zeiss\bin\java.exe" -cp ".;C:\Program Files\MATLAB\MATLAB Runtime\v91\toolbox\javabuilder\jar\javabuilder.jar;C:\Users\b2aiUsr\.scripts\zeiss\cirrusdcmold\*" demoVis C:\Users\b2aiUsr\.scripts\zeiss\N_4063OD_SD512_20220407 C:\Users\b2aiUsr\.scripts\zeiss\N_4063OD_SD512_20220407_CONVERTED 0


def main():
    """script downloads cirrus files to local, runs executable, then bundles output and uploads to data lake stage-1 container"""
    project_name = "AI-READI"
    site_names = ["UW", "UAB", "UCSD"]
    device = "Cirrus"

    # create datalake clients
    source_service_client = azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_CONNECTION_STRING, file_system_name="raw-storage"
    )
    destination_service_client = azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_CONNECTION_STRING, file_system_name="stage-1-container"
    )

    for site_name in site_names:
        print(f"Processing cirrus data for {site_name}")

        source_directory = f"{project_name}/{site_name}/{site_name}_{device}"
        destination_directory = f"{project_name}/pooled-data/{device}"

        source_folder_paths = source_service_client.get_paths(
            path=source_directory, recursive=False
        )

        for folder in source_folder_paths:
            full_folder_path = folder.name
            folder_name = os.path.basename(full_folder_path)

            print(f"Processing folder {folder_name}")

            source_folder_file_paths = source_service_client.get_paths(
                path=full_folder_path, recursive=True
            )

            # Create a temporary folder on the local machine
            temp_source_folder_path = tempfile.mkdtemp()

            # Download all files in the folder
            for file in source_folder_file_paths:
                full_file_path = file.name
                file_name = os.path.basename(file.name)

                local_file_path = full_file_path.split(full_folder_path)[1]

                temp_file_path = os.path.join(
                    temp_source_folder_path,
                    local_file_path.lstrip("/").replace("/", "\\"),  # Windows path
                )

                file_client = source_service_client.get_file_client(file_path=file.name)

                file_properties = file_client.get_file_properties().metadata

                # Check if item is a directory
                if file_properties.get("hdi_isfolder"):
                    print("file path is a directory. Creating directory")
                    print(f"Creating directory {temp_file_path}")
                    # Create the directory
                    os.makedirs(temp_file_path, exist_ok=True)
                    continue

                print(f"Downloading file {file_name} to {temp_file_path}")

                with open(file=temp_file_path, mode="wb") as f:
                    f.write(file_client.download_file().readall())

            temp_output_folder_path = tempfile.mkdtemp()

            output_folder_path = os.path.join(
                temp_output_folder_path, f"{folder_name}_CONVERTED"
            )

            # Run the executable
            print(f"Running executable for folder {folder_name}")

            subprocess.call(
                [
                    "&",
                    "C:\\Users\\b2aiUsr\\.scripts\\zeiss\\bin\\java.exe",
                    "-cp",
                    ".;C:\\Program Files\\MATLAB\\MATLAB Runtime\\v91\\toolbox\\javabuilder\\jar\\javabuilder.jar;C:\\Users\\b2aiUsr\\.scripts\\zeiss\\cirrusdcmold\\*",
                    "demoVis",
                    temp_source_folder_path,
                    output_folder_path,
                    "0",
                ],
                stdout=sys.stdout,
            )

            # Create a zip file
            zip_file_base_name = f"{folder_name}.zip"

            print(f"Creating zip file {zip_file_base_name}")

            with zipfile.ZipFile(
                file=zip_file_base_name, mode="w", compression=zipfile.ZIP_DEFLATED
            ) as archive:
                for dir_path, dir_name, file_list in os.walk(output_folder_path):
                    for file in file_list:
                        file_path = os.path.join(dir_path, file)
                        archive.write(filename=file_path, arcname=file)

            # Upload the zip file to the destination container
            print(f"Uploading zip file {zip_file_base_name}")

            destination_container_client = destination_service_client.get_file_client(
                file_path=f"{destination_directory}/{zip_file_base_name}"
            )

            with open(file=zip_file_base_name, mode="rb") as f:
                destination_container_client.upload_data(f, overwrite=True)

            # Clean up
            print("Cleaning up")

            os.remove(zip_file_base_name)
            os.remove(temp_output_folder_path)
            os.remove(temp_source_folder_path)


if __name__ == "__main__":
    main()
