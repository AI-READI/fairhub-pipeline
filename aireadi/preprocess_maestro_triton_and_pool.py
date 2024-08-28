"""Process Maestro and Triton data files and pool data into a .zip file"""

import subprocess
import os
import json
import zipfile
import tempfile
import shutil
import azure.storage.filedatalake as azurelake  # type: ignore

import config

dicom_executable_location = os.path.abspath(
    "./DICOMOCTExport_2/DICOMOCTExport_2/DicomOctExport.exe"
)


def main():
    """script downloads files to local, runs executable, then bundles output and uploads to data lake stage-1 container"""
    project_name = "AI-READI"
    # site_names = ["UW", "UAB", "UCSD"]
    site_names = ["UAB"]
    devices = ["Maestro2", "Triton"]

    # create datalake clients
    source_service_client = azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_CONNECTION_STRING, file_system_name="raw-storage"
    )
    destination_service_client = azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_CONNECTION_STRING, file_system_name="stage-1-container"
    )

    completed_files = []

    # Create temporary folders on the local machine
    temp_source_folder_path = tempfile.mkdtemp(prefix="maestro_triton_source_")
    temp_output_folder_path = tempfile.mkdtemp(prefix="maestro_triton_output_")

    for device in devices:
        print(f"Processing data for device {device}")

        completed_files = []

        device_completed_files_path = f"completed_files_{device}.json"

        if os.path.exists(device_completed_files_path):
            with open(device_completed_files_path, "r") as f:
                completed_files = json.load(f)

        for site_name in site_names:
            print(f"Processing {device} data for {site_name}")

            source_directory = f"{project_name}/{site_name}/{site_name}_{device}"
            destination_directory = f"{project_name}/pooled-data/{device}"

            source_folder_paths = source_service_client.get_paths(
                path=source_directory, recursive=False
            )

            for batch_folder in source_folder_paths:
                full_folder_path = batch_folder.name
                folder_name = os.path.basename(full_folder_path)

                date_range = folder_name.split("_")[2]

                print(f"Processing folder {full_folder_path}")

                source_folder_file_paths = source_service_client.get_paths(
                    path=full_folder_path, recursive=True
                )

                # Download all files in the folder
                for file in source_folder_file_paths:
                    full_file_path = file.name

                    # Check if file has already been processed
                    if any(
                        [
                            file["input_file"] == full_file_path
                            and file["device"] == device
                            for file in completed_files
                        ]
                    ):
                        print(
                            f"File {full_file_path} has already been processed. Skipping"
                        )
                        continue

                    file_name = os.path.basename(full_file_path)

                    temp_file_path = os.path.join(
                        temp_source_folder_path,
                        file_name,
                    )

                    file_client = source_service_client.get_file_client(
                        file_path=file.name
                    )

                    file_properties = file_client.get_file_properties().metadata

                    # Check if item is a directory
                    if file_properties.get("hdi_isfolder"):
                        print("is a directory. Skipping")
                        continue

                    print(f"Downloading file {file_name} to {temp_file_path}")

                    with open(file=temp_file_path, mode="wb") as f:
                        f.write(file_client.download_file().readall())

                    file_id = file_name.split(".")[0]

                    output_folder_path = os.path.join(temp_output_folder_path, file_id)

                    print(f"Running executable for file {file_name}")

                    returncode = subprocess.call(
                        [
                            dicom_executable_location,
                            temp_file_path,
                            output_folder_path,
                            "-octa",
                            "-enfaceSlabs",
                            "-overlayDcm",
                            "-segDcm",
                            "-dcm",
                        ]
                    )

                    if returncode == 0:
                        print("Command completed successfully")
                    else:
                        print(f"Command failed with return code {returncode}")
                        exit(returncode)

                    # Create a zip file
                    zip_file_base_name = f"{file_name}.zip"

                    print(f"Creating zip file {zip_file_base_name}")

                    with zipfile.ZipFile(
                        file=zip_file_base_name,
                        mode="w",
                        compression=zipfile.ZIP_DEFLATED,
                    ) as archive:
                        for dir_path, dir_name, file_list in os.walk(
                            output_folder_path
                        ):
                            for file in file_list:
                                file_path = os.path.join(dir_path, file)
                                archive.write(filename=file_path, arcname=file)

                    # Upload the zip file to the destination container
                    print(f"Uploading zip file {zip_file_base_name}")

                    destination_file_path = f"{destination_directory}/{site_name}_{device}_{date_range}_{zip_file_base_name}"

                    destination_container_client = (
                        destination_service_client.get_file_client(
                            file_path=destination_file_path
                        )
                    )

                    with open(file=zip_file_base_name, mode="rb") as f:
                        destination_container_client.upload_data(f, overwrite=True)

                    # Clean up
                    print("Cleaning up")

                    os.remove(zip_file_base_name)
                    shutil.rmtree(output_folder_path)
                    os.remove(temp_file_path)

                    # Update the completed files list
                    completed_files.append(
                        {
                            "site": site_name,
                            "device": device,
                            "input_file": full_file_path,
                            "output_file": destination_file_path,
                        }
                    )

                    # Write at the end of each loop in case of failure
                    with open(device_completed_files_path, "w") as f:
                        json.dump(completed_files, f, indent=4)

                    print(f"Processed file {full_file_path}")

                print(f"Processed folder {full_folder_path}")

            print(f"Processed all folders for {device} at {site_name}")

        print(f"Processed all data for {device}")

    print("Processed all data")

    # Clean up
    shutil.rmtree(temp_source_folder_path)
    shutil.rmtree(temp_output_folder_path)


if __name__ == "__main__":
    main()
