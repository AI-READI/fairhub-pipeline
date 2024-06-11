"""Process ecg data files"""

import contextlib
import datetime
import os
import tempfile
import shutil
import ecg.ecg_root as ecg
import azure.storage.blob as azureblob
import azure.storage.filedatalake as azurelake
import config


def pipeline():

    study_id = "AI-READI"

    if study_id is None or not study_id:
        raise ValueError("study_id is required")

    input_folder = f"{study_id}/pooled-data/ECG-pool"
    processed_data_output_folder = f"{study_id}/pooled-data/ECG-processed"
    data_plot_output_folder = f"{study_id}/pooled-data/ECG-dataplot"

    sas_token = azureblob.generate_account_sas(
        account_name="b2aistaging",
        account_key=config.AZURE_STORAGE_ACCESS_KEY,
        resource_types=azureblob.ResourceTypes(container=True, object=True),
        permission=azureblob.AccountSasPermissions(read=True, write=True, list=True),
        expiry=datetime.datetime.now(datetime.timezone.utc)
        + datetime.timedelta(hours=1),
    )

    # Get the blob service client
    blob_service_client = azureblob.BlobServiceClient(
        account_url="https://b2aistaging.blob.core.windows.net/",
        credential=sas_token,
    )

    # Get the list of blobs in the input folder
    file_system_client = azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_CONNECTION_STRING,
        file_system_name="stage-1-container",
    )

    # Delete the output folder if it exists
    with contextlib.suppress(Exception):
        file_system_client.delete_directory(processed_data_output_folder)

    with contextlib.suppress(Exception):
        file_system_client.delete_directory(data_plot_output_folder)

    paths = file_system_client.get_paths(path=input_folder)

    str_paths = []

    for path in paths:
        t = str(path.name)
        str_paths.append(t)

    # Create a temporary folder on the local machine
    temp_folder_path = tempfile.mkdtemp()

    # Create the output folder
    file_system_client.create_directory(processed_data_output_folder)

    for idx, path in enumerate(str_paths):
        log_idx = idx + 1

        file_client = file_system_client.get_file_client(path)
        is_directory = file_client.get_file_properties().metadata.get("hdi_isfolder")

        if is_directory:
            continue

        print(f"Processing {path} - ({log_idx}/{len(str_paths)})")

        # get the file name from the path
        file_name = path.split("/")[-1]

        if file_name == ".DS_Store":
            continue

        # skip if the path is a folder (check if extension is empty)
        if not path.split(".")[-1]:
            continue

        # download the file to the temp folder
        blob_client = blob_service_client.get_blob_client(
            container="stage-1-container", blob=path
        )

        download_path = os.path.join(temp_folder_path, file_name)

        with open(download_path, "wb") as data:
            blob_client.download_blob().readinto(data)

        print(
            f"Downloaded {file_name} to {download_path} - ({log_idx}/{len(str_paths)})"
        )

        ecg_path = download_path

        ecg_temp_folder_path = tempfile.mkdtemp()
        wfdb_temp_folder_path = tempfile.mkdtemp()

        xecg = ecg.ECG()

        conv_retval_dict = xecg.convert(
            ecg_path, ecg_temp_folder_path, wfdb_temp_folder_path
        )

        print("conv_retval_dict", conv_retval_dict)

        print(f"Converted {file_name} - ({log_idx}/{len(str_paths)})")

        output_files = conv_retval_dict["output_files"]

        print(
            f"Uploading {file_name} to {processed_data_output_folder} - ({log_idx}/{len(str_paths)}"
        )

        # file is in the format 1001_ecg_25aafb4b.dat

        for file in output_files:
            with open(f"{file}", "rb") as data:
                file_name = file.split("/")[-1]
                participant_id = file_name.split("_")[0]
                output_blob_client = blob_service_client.get_blob_client(
                    container="stage-1-container",
                    blob=f"{processed_data_output_folder}/{participant_id}/{file_name}",
                )
                output_blob_client.upload_blob(data)

        print(
            f"Uploaded {file_name} to {processed_data_output_folder} - ({log_idx}/{len(str_paths)})"
        )

        # Do the data plot
        print(f"Data plotting {file_name} - ({log_idx}/{len(str_paths)})")

        dataplot_retval_dict = xecg.dataplot(conv_retval_dict, ecg_temp_folder_path)

        print(f"Data plotted {file_name} - ({log_idx}/{len(str_paths)})")

        dataplot_pngs = dataplot_retval_dict["output_files"]

        print(
            f"Uploading {file_name} to {data_plot_output_folder} - ({log_idx}/{len(str_paths)}"
        )

        for file in dataplot_pngs:
            with open(f"{file}", "rb") as data:
                file_name = file.split("/")[-1]
                output_blob_client = blob_service_client.get_blob_client(
                    container="stage-1-container",
                    blob=f"{data_plot_output_folder}/{file_name}",
                )
                output_blob_client.upload_blob(data)

        print(
            f"Uploaded {file_name} to {data_plot_output_folder} - ({log_idx}/{len(str_paths)}"
        )

        # Create the file metadata

        print(f"Creating metadata for {file_name} - ({log_idx}/{len(str_paths)})")

        output_hea_file = conv_retval_dict["output_hea_file"]

        hea_metadata = xecg.metadata(output_hea_file)
        print(hea_metadata)

        print(f"Metadata created for {file_name} - ({log_idx}/{len(str_paths)})")

        shutil.rmtree(ecg_temp_folder_path)
        shutil.rmtree(wfdb_temp_folder_path)
        os.remove(download_path)


if __name__ == "__main__":
    pipeline()

    # delete the ecg.log file
    if os.path.exists("ecg.log"):
        os.remove("ecg.log")
