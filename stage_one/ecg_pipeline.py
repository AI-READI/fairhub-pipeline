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
import utils.dependency as deps


def pipeline(study_id: str):  # sourcery skip: low-code-quality
    """Process ecg data files for a study
    Args:
        study_id (str): the study id
    """

    if study_id is None or not study_id:
        raise ValueError("study_id is required")

    input_folder = f"{study_id}/pooled-data/ECG"
    processed_data_output_folder = f"{study_id}/pooled-data/ECG-processed"
    dependency_folder = f"{study_id}/dependency/ECG"
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

    file_paths = []

    for path in paths:
        t = str(path.name)

        file_name = t.split("/")[-1]

        print(f"Processing {file_name}")

        # Check if the item is an xml file
        if file_name.split(".")[-1] != "xml":
            continue

        # Get the parent folder of the file.
        # The name of this folder is in the format siteName_dataType_startDate-endDate
        batch_folder = t.split("/")[-2]

        print(f"Batch folder: {batch_folder}")

        # Check if the folder name is in the format siteName_dataType_startDate-endDate
        if len(batch_folder.split("_")) != 3:
            continue

        site_name, data_type, start_date_end_date = batch_folder.split("_")

        start_date = start_date_end_date.split("-")[0]
        end_date = start_date_end_date.split("-")[1]

        file_paths.append(
            {
                "file_path": t,
                "batch_folder": batch_folder,
                "site_name": site_name,
                "data_type": data_type,
                "start_date": start_date,
                "end_date": end_date,
            }
        )

    print(f"Found {len(file_paths)} files in {input_folder}")

    # Create a temporary folder on the local machine
    temp_folder_path = tempfile.mkdtemp()

    # Create the output folder
    file_system_client.create_directory(processed_data_output_folder)

    workflow_file_dependencies = deps.WorkflowFileDependencies()

    total_files = len(file_paths)

    for idx, file in enumerate(file_paths):
        log_idx = idx + 1

        path = file["file_path"]

        # file_client = file_system_client.get_file_client(path)
        # is_directory = file_client.get_file_properties().metadata.get("hdi_isfolder")

        # if is_directory:
        #     continue

        workflow_input_files = [path]

        print(f"Processing {path} - ({log_idx}/{total_files})")

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

        print(f"Downloaded {file_name} to {download_path} - ({log_idx}/{total_files})")

        ecg_path = download_path

        ecg_temp_folder_path = tempfile.mkdtemp()
        wfdb_temp_folder_path = tempfile.mkdtemp()

        xecg = ecg.ECG()

        conv_retval_dict = xecg.convert(
            ecg_path, ecg_temp_folder_path, wfdb_temp_folder_path
        )

        print(f"Converted {file_name} - ({log_idx}/{total_files})")

        output_files = conv_retval_dict["output_files"]
        participant_id = conv_retval_dict["participantID"]

        print(
            f"Uploading {file_name} to {processed_data_output_folder} - ({log_idx}/{total_files})"
        )

        # file is in the format 1001_ecg_25aafb4b.dat

        workflow_output_files = []

        for file in output_files:
            with open(f"{file}", "rb") as data:
                file_name = file.split("/")[-1]

                output_file_path = f"{processed_data_output_folder}/ecg_12lead/philips_tc30/{participant_id}/{file_name}"

                output_blob_client = blob_service_client.get_blob_client(
                    container="stage-1-container",
                    blob=output_file_path,
                )
                output_blob_client.upload_blob(data)

                workflow_output_files.append(output_file_path)

        print(
            f"Uploaded {file_name} to {processed_data_output_folder} - ({log_idx}/{total_files})"
        )

        workflow_file_dependencies.add_dependency(
            workflow_input_files, workflow_output_files
        )

        # Do the data plot
        # print(f"Data plotting {file_name} - ({log_idx}/{total_files})")

        # dataplot_retval_dict = xecg.dataplot(conv_retval_dict, ecg_temp_folder_path)

        # print(f"Data plotted {file_name} - ({log_idx}/{total_files})")

        # dataplot_pngs = dataplot_retval_dict["output_files"]

        # print(
        #     f"Uploading {file_name} to {data_plot_output_folder} - ({log_idx}/{total_files}"
        # )

        # for file in dataplot_pngs:
        #     with open(f"{file}", "rb") as data:
        #         file_name = file.split("/")[-1]
        #         output_blob_client = blob_service_client.get_blob_client(
        #             container="stage-1-container",
        #             blob=f"{data_plot_output_folder}/{file_name}",
        #         )
        #         output_blob_client.upload_blob(data)

        # print(
        #     f"Uploaded {file_name} to {data_plot_output_folder} - ({log_idx}/{total_files}"
        # )

        # Create the file metadata

        # print(f"Creating metadata for {file_name} - ({log_idx}/{total_files})")

        # output_hea_file = conv_retval_dict["output_hea_file"]

        # hea_metadata = xecg.metadata(output_hea_file)
        # print(hea_metadata)

        # print(f"Metadata created for {file_name} - ({log_idx}/{total_files})")

        shutil.rmtree(ecg_temp_folder_path)
        shutil.rmtree(wfdb_temp_folder_path)
        os.remove(download_path)

        # dev
        if log_idx == 10:
            break

    deps_output = workflow_file_dependencies.write_to_file(temp_folder_path)

    json_file_path = deps_output["file_path"]
    json_file_name = deps_output["file_name"]

    with open(json_file_path, "rb") as data:
        output_blob_client = blob_service_client.get_blob_client(
            container="stage-1-container",
            blob=f"{dependency_folder}/{json_file_name}",
        )
        output_blob_client.upload_blob(data)


if __name__ == "__main__":
    pipeline("AI-READI")

    # delete the ecg.log file
    if os.path.exists("ecg.log"):
        os.remove("ecg.log")
