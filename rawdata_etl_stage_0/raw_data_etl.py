"""ETL/zipping for environmental sensor directories"""
import datetime as dt
import zipfile
import tempfile
import re
import os
import azure.storage.filedatalake as azurelake

import config

def get_filter_date():
        """produces a string representation of the date range used to filter device entries from raw data"""
        next_sunday_offset = dt.timedelta((12-dt.datetime.now().weekday()) % 7)
        end_date = dt.datetime.now() + next_sunday_offset
        start_date = end_date - dt.timedelta(days=7)
        filter_date = f"{start_date.strftime('%Y%m%d')}-{end_date.strftime('%Y%m%d')}"
        return filter_date

def envsensor_raw_processing():
    """runs extractions on envsensor devices"""
    filter_date = get_filter_date()
    project_name = "AI-READI"
    device_name = "EnvSensor"
    sites = ["site-test", "UW", "UAB", "UCSD"]

    # create datalake clients
    source_service_client = azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_CONNECTION_STRING,
        file_system_name="raw-storage")
    destination_service_client = azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_CONNECTION_STRING,
        file_system_name="stage-1-container")
    
    # fetch EnvSensor subdirectories for each site
    for site_name in sites:
        subdirectories = set()
        source_directory = f"{project_name}/{site_name}/{site_name}_{device_name}/{site_name}_{device_name}_{filter_date}"
        destination_directory = f"{project_name}/pooled-data/{device_name}"
        for blob_name in source_service_client.get_paths(path=source_directory):
            # Extract subdirectory name from the blob name
            subdirectory = re.search(r"(ENV-\d\d\d\d-\d\d\d)",blob_name.name)
            if subdirectory:
                subdirectories.add(subdirectory.group(0))
        # repeat the above after having fetched the blobs
        for directory in subdirectories:
            with tempfile.TemporaryDirectory() as tmp_dir:
                arch_directory_name = os.path.join(tmp_dir,directory)
                os.mkdir(arch_directory_name)
                for file in source_service_client.get_paths(path=f"{source_directory}/{directory}/"):
                    file_name = str(file.name)
                    local_file_name = f"{file_name.rsplit(sep='/', maxsplit=-1)[-1]}"
                    download_file_path = os.path.join(arch_directory_name, local_file_name)
                    file_client = source_service_client.get_file_client(file_path=file_name)
                    with open(download_file_path, mode="w+b") as download_file:
                        download_file.write(file_client.download_file().readall())
                zip_file_base_name = f"{site_name}_{device_name}_{site_name}_{device_name}_{filter_date}_{directory}.zip"
                with zipfile.ZipFile(file=zip_file_base_name,mode='w',compression=zipfile.ZIP_DEFLATED) as archive:
                    for (dir_path, dir_name, file_list) in os.walk(arch_directory_name):
                        archive.mkdir(zinfo_or_directory_name=directory)
                        for file in file_list:
                            file_path = os.path.join(dir_path, file)
                            archive.write(filename=file_path, arcname=f"{directory}/{file}")
                archive.close()
                destination_container_client = destination_service_client.get_file_client(file_path=f"{destination_directory}/{zip_file_base_name}")
                with open(file=zip_file_base_name, mode="rb") as f:
                    destination_container_client.upload_data(f, overwrite=True)
                os.remove(zip_file_base_name)