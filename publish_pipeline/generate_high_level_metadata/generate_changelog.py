import datetime
import pathlib
import tempfile

import azure.storage.blob as azureblob
import psycopg2
import pyfairdatatools

import config


def pipeline():
    """Reads the database for the dataset and generates a changelog.md file in the metadata folder."""

    changelog_metadata = ""

    conn = psycopg2.connect(
        host=config.FAIRHUB_DATABASE_HOST,
        database=config.FAIRHUB_DATABASE_NAME,
        user=config.FAIRHUB_DATABASE_USER,
        password=config.FAIRHUB_DATABASE_PASSWORD,
        port=config.FAIRHUB_DATABASE_PORT,
    )

    cur = conn.cursor()

    # study_id = "c588f59c-cacb-4e52-99dd-95b37dcbfd5c"
    dataset_id = "af4be921-e507-41a9-9328-4cbb4b7dca1c"
    version_id = "3880f581-e142-44ea-bc4c-10242eca7d75"

    cur.execute(
        "SELECT * FROM version WHERE id = %s AND dataset_id = %s",
        (version_id, dataset_id),
    )

    version = cur.fetchone()

    if version is None:
        return "dataset not found"

    changelog = ""
    cur.execute(
        "SELECT changelog FROM version WHERE id = %s AND dataset_id = %s",
        (version_id, dataset_id),
    )

    version_changelog = cur.fetchone()
    changelog = version_changelog[0]

    changelog_metadata = changelog

    conn.close()

    # Create a temporary folder on the local machine
    temp_folder_path = tempfile.mkdtemp()

    temp_file_path = pathlib.Path(temp_folder_path, "changelog.md")
    pyfairdatatools.generate.generate_changelog_file(
        data=changelog_metadata, file_path=temp_file_path, file_type="md"
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
        blob=f"{metadata_folder}/changelog.md",
    )

    with open(temp_file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

    return
