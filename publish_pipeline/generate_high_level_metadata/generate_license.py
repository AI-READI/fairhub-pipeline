# pylint: disable=line-too-long
"""Process environmental sensor data files"""

import datetime
import pathlib
import tempfile
import uuid

import azure.storage.blob as azureblob
import psycopg2
import pyfairdatatools

import config


def pipeline():
    """Reads the database for the dataset and generates a license.txt file in the metadata folder."""

    license_metadata = {}

    conn = psycopg2.connect(
        host=config.FAIRHUB_DATABASE_HOST,
        database=config.FAIRHUB_DATABASE_NAME,
        user=config.FAIRHUB_DATABASE_USER,
        password=config.FAIRHUB_DATABASE_PASSWORD,
        port=config.FAIRHUB_DATABASE_PORT,
    )

    cur = conn.cursor()

    study_id = "c588f59c-cacb-4e52-99dd-95b37dcbfd5c"
    dataset_id = "af4be921-e507-41a9-9328-4cbb4b7dca1c"

    cur.execute(
        "SELECT * FROM dataset WHERE id = %s AND study_id = %s",
        (dataset_id, study_id),
    )

    dataset = cur.fetchone()

    if dataset is None:
        return "Dataset not found"

    license_text = ""

    cur.execute(
        "SELECT rights FROM dataset_rights WHERE dataset_id = %s",
        (dataset_id,),
    )

    dataset_other = cur.fetchone()
    # license_text = dataset_other.join(",")
    license_text = dataset_other[0]
    license_metadata = license_text

    conn.close()

    # Create a temporary folder on the local machine
    temp_folder_path = tempfile.mkdtemp()

    temp_file_path = pathlib.Path(temp_folder_path, "license.txt")

    data_is_valid = pyfairdatatools.validate.validate_license(identifier=license_metadata)
    print(data_is_valid)
    if not data_is_valid:
        raise Exception("Dataset description is not valid")

    pyfairdatatools.generate.generate_license_file(
        data=license_metadata, file_path=temp_file_path, file_type="txt"
    )

    # upload the file to the metadata folder

    metadata_folder = "AI-READI/metadata"

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

    # upload the file to the metadata folder
    blob_client = blob_service_client.get_blob_client(
        container="stage-1-container",
        blob=f"{metadata_folder}/license.txt",
    )

    with open(temp_file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

    return