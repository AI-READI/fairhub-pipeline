"""Pool environment sensor data into a .zip file"""

import os
import json
import zipfile
import tempfile
import shutil
import azure.storage.filedatalake as azurelake  # type: ignore
import config

completed_folders_file = "completed_folders_env_sensor.json"


def main():  # sourcery skip: low-code-quality
    project_name = "AI-READI"
    site_names = ["UW", "UAB", "UCSD"]
    # site_names = ["JS_pilot_envsensor"]
    device = "EnvSensor"
    # device = "JS_EnvSensor"

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

    destination_directory = f"{project_name}/pooled-data/{device}"

    count = 0

    for site_name in site_names:
        print(f"Processing {device} data for {site_name}")

        source_directory = f"{project_name}/{site_name}/{site_name}_{device}"
        # source_directory = f"{project_name}/{site_name}/{device}"
        source_folder_paths = source_service_client.get_paths(
            path=source_directory, recursive=False
        )

        for folder in source_folder_paths:
            # Batch folder level here
            full_folder_path = folder.name
            batch_folder_name = os.path.basename(full_folder_path)

            # Check if folder has already been processed
            if any(
                folder["folder"] == full_folder_path for folder in completed_folders
            ):
                print(f"Folder {full_folder_path} has already been processed. Skipping")
                continue

            print(f"Processing folder {batch_folder_name}")

            source_folder_file_paths = source_service_client.get_paths(
                path=full_folder_path, recursive=False
            )

            # Create temporary folders on the local machine
            temp_source_folder_path = tempfile.mkdtemp(prefix="env_sensor_source_")

            temp_folder_path = os.path.join(temp_source_folder_path, batch_folder_name)
            if not os.path.exists(temp_folder_path):
                os.makedirs(temp_folder_path)

            # Download all files in the folder
            for file in source_folder_file_paths:
                full_file_path = file.name
                file_name = os.path.basename(file.name)

                temp_file_path = os.path.join(
                    temp_folder_path,
                    file_name,
                )

                file_client = source_service_client.get_file_client(
                    file_path=full_file_path
                )

                print(f"Downloading file {file_name} to {temp_file_path}")

                with open(file=temp_file_path, mode="wb") as f:
                    f.write(file_client.download_file().readall())

            # Create a zip file
            zip_file_base_name = f"{batch_folder_name}.zip"

            print(f"Creating zip file {zip_file_base_name}")

            with zipfile.ZipFile(
                file=zip_file_base_name,
                mode="w",
                compression=zipfile.ZIP_DEFLATED,
            ) as archive:
                for dir_path, dir_name, file_list in os.walk(temp_source_folder_path):
                    for file in file_list:
                        file_path = os.path.join(dir_path, file)
                        archive.write(filename=file_path, arcname=file)

            # Upload the zip file to the destination container
            print(f"Uploading zip file {zip_file_base_name}")

            destination_file_path = f"{destination_directory}/{zip_file_base_name}"

            destination_container_client = destination_service_client.get_file_client(
                file_path=destination_file_path
            )

            with open(file=zip_file_base_name, mode="rb") as f:
                destination_container_client.upload_data(f, overwrite=True)

            # Clean up
            print("Cleaning up")
            os.remove(zip_file_base_name)
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

            print(f"Folder {batch_folder_name} processed successfully")

            count += 1
            print(f"Processed {count} folders")


if __name__ == "__main__":
    main()
