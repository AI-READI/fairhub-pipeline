"""Process cirrus data files and pool data into a .zip file"""

import subprocess
import os
import json
import zipfile
import tempfile
import azure.storage.filedatalake as azurelake  # type: ignore
import shutil

import config

# & "C:\Users\b2aiUsr\.scripts\zeiss\bin\java.exe" -cp ".;C:\Program Files\MATLAB\MATLAB Runtime\v91\toolbox\javabuilder\jar\javabuilder.jar;C:\Users\b2aiUsr\.scripts\zeiss\cirrusDCMvisualizationsDICOMWrapper20240719_141654\*" demoVis C:\Users\b2aiUsr\.scripts\zeiss\UW_Cirrus_20240604-20240607 C:\Users\b2aiUsr\.scripts\zeiss\UW_Cirrus_20240604-20240607_CONVERTED 0

java_path = r"C:\Users\b2aiUsr\.scripts\zeiss\bin\java.exe"
classpath = r".;C:\Program Files\MATLAB\MATLAB Runtime\v91\toolbox\javabuilder\jar\javabuilder.jar;C:\Users\b2aiUsr\.scripts\zeiss\cirrusDCMvisualizationsDICOMWrapper20240719_141654\*"
main_class = "demoVis"
# input_dir = r"C:\Users\b2aiUsr\.scripts\zeiss\N_4063OD_SD512_20220407"
# output_dir = r"C:\Users\b2aiUsr\.scripts\zeiss\N_4063OD_SD512_20220407_CONVERTED"
additional_arg = "0"


completed_folders_file = "completed_folders.json"


def main():  # sourcery skip: low-code-quality
    """script downloads cirrus files to local, runs executable, then bundles output and uploads to data lake stage-1 container"""
    project_name = "AI-READI"
    site_names = ["UW", "UAB", "UCSD"]
    # site_names = ["site-test"]
    device = "Cirrus"

    # create datalake clients
    source_service_client = azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_CONNECTION_STRING, file_system_name="raw-storage"
    )
    destination_service_client = azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_CONNECTION_STRING, file_system_name="stage-1-container"
    )

    completed_folders = []

    if os.path.exists(completed_folders_file):
        with open(completed_folders_file, "r") as f:
            completed_folders = json.load(f)

    for site_name in site_names:
        print(f"Processing cirrus data for {site_name}")

        source_directory = f"{project_name}/{site_name}/{site_name}_{device}"
        destination_directory = f"{project_name}/pooled-data/{device}"

        source_folder_paths = source_service_client.get_paths(
            path=source_directory, recursive=False
        )

        for folder in source_folder_paths:
            # Batch folder level here
            full_folder_path = folder.name

            # Check if folder has already been processed
            if any(
                [folder["folder"] == full_folder_path for folder in completed_folders]
            ):
                print(f"Folder {full_folder_path} has already been processed. Skipping")
                continue

            folder_name = os.path.basename(full_folder_path)
            date_range = folder_name.split("_")[2]

            print(f"Processing folder {folder_name}")

            source_folder_file_paths = source_service_client.get_paths(
                path=full_folder_path, recursive=True
            )

            # Create a temporary folder on the local machine
            temp_source_folder_path = tempfile.mkdtemp(prefix="cirrus_source_")

            # Download all files in the folder
            for file in source_folder_file_paths:
                full_file_path = file.name
                file_name = os.path.basename(file.name)

                local_file_path = full_file_path.split(full_folder_path)[1]

                temp_file_path = os.path.join(
                    temp_source_folder_path,
                    local_file_path.lstrip("/").replace("/", "\\"),  # Windows path
                )

                file_client = source_service_client.get_file_client(
                    file_path=full_file_path
                )

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

            temp_output_folder_path = tempfile.mkdtemp(prefix="cirrus_output_")

            output_folder_path = temp_output_folder_path

            # Run the executable
            print(f"Running executable for folder {folder_name}")

            input_dir = temp_source_folder_path
            output_dir = output_folder_path

            returncode = subprocess.call(
                [
                    java_path,
                    "-cp",
                    classpath,
                    main_class,
                    input_dir,
                    output_dir,
                    additional_arg,
                ]
            )

            if returncode == 0:
                print("Command executed successfully")
            else:
                print(f"Command failed with return code {returncode}")
                exit(returncode)

            # subprocess.call(
            #     [
            #         "&",
            #         "C:\\Users\\b2aiUsr\\.scripts\\zeiss\\bin\\java.exe",
            #         "-cp",
            #         ".;C:\\Program Files\\MATLAB\\MATLAB Runtime\\v91\\toolbox\\javabuilder\\jar\\javabuilder.jar;C:\\Users\\b2aiUsr\\.scripts\\zeiss\\cirrusdcmold\\*",
            #         "demoVis",
            #         temp_source_folder_path,
            #         output_folder_path,
            #         "0",
            #     ],
            #     stdout=sys.stdout,
            # )

            # Create the zip files

            # Read the contents of the output folder
            # Zip only the directories in the output folder and not the files
            # Each directory in the output folder is a separate file

            for dir_path, dir_name, file_list in os.walk(output_dir):
                for dir_name in dir_name:
                    output_dir_path = os.path.join(output_dir, dir_name)
                    zip_file_base_name = (
                        f"{site_name}_{device}_{date_range}_{dir_name}.fda.zip"
                    )

                    print(f"Creating zip file {zip_file_base_name}")

                    with zipfile.ZipFile(
                        file=zip_file_base_name,
                        mode="w",
                        compression=zipfile.ZIP_DEFLATED,
                    ) as archive:
                        for sub_folder_dir_path, dir_name, file_list in os.walk(
                            output_dir_path
                        ):
                            for file in file_list:
                                file_path = os.path.join(sub_folder_dir_path, file)
                                archive.write(filename=file_path, arcname=file)

                    # Upload the zip file to the destination container
                    print(f"Uploading zip file {zip_file_base_name}")

                    destination_container_client = (
                        destination_service_client.get_file_client(
                            file_path=f"{destination_directory}/{zip_file_base_name}"
                        )
                    )

                    with open(file=zip_file_base_name, mode="rb") as f:
                        destination_container_client.upload_data(f, overwrite=True)

                    # Clean up
                    print("Cleaning up")
                    os.remove(zip_file_base_name)
                    shutil.rmtree(dir_path)

            # Clean up
            print("Cleaning up")
            shutil.rmtree(temp_output_folder_path)
            shutil.rmtree(temp_source_folder_path)

            # Update the completed folders list
            completed_folders.append(
                {
                    "site": site_name,
                    "device": device,
                    "folder": full_folder_path,
                }
            )

            # Write at the end of each loop in case of failure
            with open(completed_folders_file, "w") as f:
                json.dump(completed_folders, f, indent=4)

            print(f"Folder {folder_name} processed successfully")


if __name__ == "__main__":
    main()
