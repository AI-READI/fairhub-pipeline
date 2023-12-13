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
    """Reads the database for the dataset and generates a README.md file in the metadata folder."""

    readme_metadata = {}

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

    title = ""

    cur.execute(
        "SELECT title FROM dataset_title WHERE dataset_id = %s AND type = 'MainTitle'",
        (dataset_id,),
    )

    dataset_title = cur.fetchone()

    title = dataset_title[0]

    readme_metadata["Title"] = title

    # todo: using a placeholder for now
    # todo: replace with the actual doi when we have it
    identifier = "10.5281/zenodo.7641684"

    readme_metadata["Identifier"] = identifier

    # todo: generating a random uuid for now
    version = str(uuid.uuid4())

    readme_metadata["Version"] = version

    publication_date = datetime.datetime.now().strftime("%Y-%m-%d")

    readme_metadata["PublicationDate"] = publication_date

    abstract = ""

    cur.execute(
        "SELECT description FROM dataset_description WHERE dataset_id = %s AND type = 'Abstract'",
        (dataset_id,),
    )

    dataset_abstract = cur.fetchone()

    abstract = dataset_abstract[0]

    readme_metadata["About"] = abstract

    detailed_description = ""

    cur.execute(
        "SELECT detailed_description FROM study_description WHERE study_id = %s",
        (study_id,),
    )

    study_description = cur.fetchone()

    detailed_description = study_description[0]

    readme_metadata["DatasetDescription"] = detailed_description

    access_details = ""

    cur.execute(
        "SELECT type, description FROM dataset_access WHERE dataset_id = %s",
        (dataset_id,),
    )

    dataset_access = cur.fetchone()

    access_details = f"${dataset_access[0]} - {dataset_access[1]}"

    readme_metadata["DatasetAccess"] = access_details

    standards_followed = ""

    cur.execute(
        "SELECT standards_followed FROM dataset_other WHERE dataset_id = %s",
        (dataset_id,),
    )

    dataset_standards_followed = cur.fetchone()

    standards_followed = dataset_standards_followed[0]

    readme_metadata["StandardsFollowed"] = standards_followed

    # todo: resources

    # todo: license

    # todo: create citation

    acknowledgement = ""

    cur.execute(
        "SELECT acknowledgement FROM dataset_other WHERE dataset_id = %s",
        (dataset_id,),
    )

    dataset_acknowledgement = cur.fetchone()

    acknowledgement = dataset_acknowledgement[0]

    if acknowledgement:
        readme_metadata["Acknowledgement"] = acknowledgement

    print(readme_metadata)

    conn.commit()
    conn.close()

    # Create a temporary folder on the local machine
    temp_folder_path = tempfile.mkdtemp()

    temp_file_path = pathlib.Path(temp_folder_path, "README.md")

    data_is_valid = pyfairdatatools.validate.validate_readme(data=readme_metadata)

    if not data_is_valid:
        raise Exception("Dataset description is not valid")

    pyfairdatatools.generate.generate_readme(
        data=readme_metadata, file_path=temp_file_path, file_type="md"
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
        blob=f"{metadata_folder}/README.md",
    )

    with open(temp_file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

    return
