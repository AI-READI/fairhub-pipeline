"""Pool environment sensor data into a .zip file"""

import os
import json
import zipfile
import tempfile
import shutil
import time
from datetime import datetime, timedelta
import azure.storage.filedatalake as azurelake  # type: ignore
from tqdm import tqdm
import config
import contextlib

completed_folders_file = "completed_folders_garmin.json"


def format_time_delta(seconds):
    """Format seconds into a human-readable time string"""
    return str(timedelta(seconds=int(seconds)))


def main():  # sourcery skip: low-code-quality
    start_time = time.time()
    project_name = "AI-READI"
    site_names = ["UW", "UAB", "UCSD"]
    # site_names = ["UAB"]
    device = "FitnessTracker"

    print(
        f"Starting {device} data processing at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    print(f"Processing {len(site_names)} sites: {', '.join(site_names)}")

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
        print(f"Found {len(completed_folders)} previously completed folders")

    destination_directory = f"{project_name}/pooled-data/{device}"

    with contextlib.suppress(Exception):
        destination_service_client.delete_directory(destination_directory)

    # Count total folders for progress tracking
    total_folders = 0
    for site_name in site_names:
        source_directory = f"{project_name}/{site_name}/{site_name}_{device}"
        source_folder_paths = source_service_client.get_paths(
            path=source_directory, recursive=False
        )
        for folder in source_folder_paths:
            full_folder_path = folder.name
            if not any(
                folder["folder"] == full_folder_path for folder in completed_folders
            ):
                total_folders += 1

    print(f"Total folders to process: {total_folders}")

    processed_folders = 0

    for site_name in tqdm(site_names, desc="Processing sites", unit="site"):
        site_folder_start_time = time.time()
        print(f"\nProcessing {device} data for {site_name}")

        source_directory = f"{project_name}/{site_name}/{site_name}_{device}"
        source_folder_paths = source_service_client.get_paths(
            path=source_directory, recursive=False
        )

        # Count folders for this site
        site_folders = []
        for folder in source_folder_paths:
            full_folder_path = folder.name
            if not any(
                folder["folder"] == full_folder_path for folder in completed_folders
            ):
                site_folders.append(folder)

        print(f"Found {len(site_folders)} folders to process for {site_name}")

        for folder in tqdm(
            site_folders, desc=f"Processing {site_name} folders", unit="folder"
        ):
            folder_start_time = time.time()

            # Batch folder level here
            full_folder_path = folder.name
            batch_folder_name = os.path.basename(full_folder_path)

            # Check if folder has already been processed
            if any(
                folder["folder"] == full_folder_path for folder in completed_folders
            ):
                print(f"Folder {full_folder_path} has already been processed. Skipping")
                continue

            print(f"\nProcessing folder {batch_folder_name}")

            source_folder_file_paths = source_service_client.get_paths(
                path=full_folder_path, recursive=True
            )

            # Create temporary folders on the local machine
            temp_source_folder_path = tempfile.mkdtemp(prefix="garmin_source_")

            # Download all files in the folder
            print(f"Downloading {len(list(source_folder_file_paths))} files...")
            for file in tqdm(
                source_folder_file_paths,
                desc="Downloading files",
                unit="file",
                leave=False,
            ):
                full_file_path = str(file.name)
                file_name = os.path.basename(file.name)

                local_file_path = full_file_path.split(full_folder_path)[1]

                temp_file_path = os.path.join(
                    temp_source_folder_path,
                    local_file_path.lstrip("/").replace("/", "\\"),
                )  # Windows path

                file_client = source_service_client.get_file_client(
                    file_path=full_file_path,
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

            temp_output_folder_path = tempfile.mkdtemp(prefix="garmin_output_")

            # Create a zip file of the folder
            zip_file_path = os.path.join(
                temp_output_folder_path, f"{batch_folder_name}.zip"
            )

            print(f"Creating zip file {zip_file_path}")

            with zipfile.ZipFile(file=zip_file_path, mode="w") as archive:
                for dir_path, dir_name, file_list in os.walk(temp_source_folder_path):
                    for file in file_list:
                        file_path = os.path.join(dir_path, file)
                        archive_path = os.path.relpath(
                            file_path, temp_source_folder_path
                        )
                        archive.write(filename=file_path, arcname=archive_path)

            # Upload the zip file to the destination container
            print(f"Uploading zip file {zip_file_path}")

            zip_file_base_name = os.path.basename(zip_file_path)

            destination_file_path = f"{destination_directory}/{zip_file_base_name}"

            destination_container_client = destination_service_client.get_file_client(
                file_path=destination_file_path
            )

            with open(file=zip_file_path, mode="rb") as f:
                destination_container_client.upload_data(f, overwrite=True)

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

            processed_folders += 1
            folder_time = time.time() - folder_start_time

            # Calculate ETA
            if processed_folders > 0:
                avg_time_per_folder = (time.time() - start_time) / processed_folders
                remaining_folders = total_folders - processed_folders
                eta_seconds = avg_time_per_folder * remaining_folders
                eta_str = format_time_delta(eta_seconds)

                print(
                    f"Folder {batch_folder_name} processed successfully in {format_time_delta(folder_time)}"
                )
                print(
                    f"Progress: {processed_folders}/{total_folders} folders ({processed_folders / total_folders * 100:.1f}%)"
                )
                print(f"ETA: {eta_str}")

        site_time = time.time() - site_folder_start_time
        print(f"\nCompleted {site_name} in {format_time_delta(site_time)}")

    total_time = time.time() - start_time
    print(f"\n{'='*50}")
    print(f"Processing completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total processing time: {format_time_delta(total_time)}")
    print(f"Processed {processed_folders} folders")
    print(
        f"Average time per folder: {format_time_delta(total_time / processed_folders) if processed_folders > 0 else 'N/A'}"
    )


if __name__ == "__main__":
    main()
