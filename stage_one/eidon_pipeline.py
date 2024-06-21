"""Process ecg data files"""

import contextlib
import datetime
import os
import tempfile
import shutil
import imaging.imaging_eidon_retinal_photography_root as EIDON
import imaging.imaging_utils as imaging_utils
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

    input_folder = f"{study_id}/pooled-data/Eidon"
    dependency_folder = f"{study_id}/dependency/Eidon"
    processed_data_output_folder = f"{study_id}/pooled-data/retinal_photography"

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

    paths = file_system_client.get_paths(path=input_folder)

    file_paths = []

    for path in paths:
        t = str(path.name)

        file_name = t.split("/")[-1]

        # Check if the item is an dicom file
        if file_name.split(".")[-1] != "dcm":
            continue

        # Get the parent folder of the file.
        # The name of this folder is in the format siteName_dataType_startDate-endDate
        batch_folder = t.split("/")[-2]

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

        workflow_input_files = [path]

        print(f"Processing {path} - ({log_idx}/{total_files})")

        # get the file name from the path
        file_name = path.split("/")[-1]

        # download the file to the temp folder
        blob_client = blob_service_client.get_blob_client(
            container="stage-1-container", blob=path
        )

        download_path = os.path.join(temp_folder_path, file_name)

        with open(download_path, "wb") as data:
            blob_client.download_blob().readinto(data)

        print(f"Downloaded {file_name} to {download_path} - ({log_idx}/{total_files})")

        # process the file

        organize_temp_folder_path = tempfile.mkdtemp()

        eidon_instance = EIDON.Eidon()

        organize_result = eidon_instance.organize(
            download_path, organize_temp_folder_path
        )

        # pprint.pp(organize_result)

        step2_paths = []

        for root, dirs, files in os.walk(organize_temp_folder_path):
            for file in files:
                step2_paths.append(os.path.join(root, file))

        convert_temp_folder_path = tempfile.mkdtemp()

        # rule = organize_result["Rule"]
        file_name = organize_result["Filename"]
        # file_path = organize_result["Path"]

        for step2_path in step2_paths:
            eidon_instance.convert(step2_path, convert_temp_folder_path)

        filtered_file_names = imaging_utils.get_filtered_file_names(
            convert_temp_folder_path
        )

        for file_name in filtered_file_names:
            format_temp_folder_path = tempfile.mkdtemp()
            #

            # format_info = imaging_utils.format_file(file_name, format_temp_folder_path)
            imaging_utils.format_file(file_name, format_temp_folder_path)

            # patient_id = format_info["PatientID"]

            # modality = rule.split("_")[-1]

            workflow_output_files = []

            for root, dirs, files in os.walk(format_temp_folder_path):
                for file in files:
                    file_path = os.path.join(root, file)

                    with open(f"{file_path}", "rb") as data:
                        file_name2 = file_path.split("/")[-5:]

                        combined_file_name = "/".join(file_name2)

                        print(
                            f"Uploading {combined_file_name} - ({log_idx}/{total_files})"
                        )

                        output_file_path = (
                            f"{processed_data_output_folder}/{combined_file_name}"
                        )

                        output_blob_client = blob_service_client.get_blob_client(
                            container="stage-1-container",
                            # blob=f"{processed_data_output_folder}/imaging/{modality}/icare_eidon/{patient_id}/{file_name}",
                            blob=output_file_path,
                        )
                        output_blob_client.upload_blob(data)

                        workflow_output_files.append(output_file_path)

            workflow_file_dependencies.add_dependency(
                workflow_input_files, workflow_output_files
            )

            shutil.rmtree(format_temp_folder_path)

        shutil.rmtree(convert_temp_folder_path)
        shutil.rmtree(organize_temp_folder_path)
        os.remove(download_path)

        # dev
        # if log_idx == 5:
        #     break

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
