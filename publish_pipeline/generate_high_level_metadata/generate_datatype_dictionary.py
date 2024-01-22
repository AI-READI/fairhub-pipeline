"""Process environmental sensor data files"""

import datetime
import pathlib
import tempfile

import azure.storage.blob as azureblob
import pyfairdatatools

import config


def pipeline():
    """
    Reads the database for the dataset folders within pooled-data and
    generates a datatype_description.yaml file in the metadata folder.
    """

    # Get the folder names of the dataset
    container = "stage-1-container"
    pooled_data = "AI-READI/pooled-data"

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

    # Get the folders within pooled-data
    blob_content = blob_service_client.get_container_client(container)
    blobs_list = blob_content.list_blobs(name_starts_with=pooled_data)

    pooled_data_folders = []
    for blob in blobs_list:
        if blob.name.count("/") == 2:
            pooled_data_folders.append(blob.name.split("/")[2])

    # print(pooled_data_folders)
    pooled_data_folders = ["ekg", "redcap_data", "oct"]

    # Create a temporary folder on the local machine
    temp_folder_path = tempfile.mktemp()

    temp_file_path = pathlib.Path(temp_folder_path, "datatype_description.yaml")

    # Validate the data folders
    data_is_valid = pyfairdatatools.validate.validate_datatype_dictionary(
        data=pooled_data_folders,
    )

    if not data_is_valid:
        raise Exception("Data types are not valid")

    # Generate the datatype_description.yaml file
    pyfairdatatools.generate.generate_datatype_file(
        data=pooled_data_folders, file_path=temp_file_path, file_type="yaml"
    )

    # Upload the datatype_description.yaml file to the metadata folder
    metadata_folder = "AI-READI/metadata"

    blob_service_client = azureblob.BlobServiceClient(
        account_url="https://b2aistaging.blob.core.windows.net/",
        credential=sas_token,
    )

    blob_client = blob_service_client.get_blob_client(
        container="stage-1-container",
        blob=f"{metadata_folder}/datatype_description.yaml",
    )

    with open(temp_file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

    return
