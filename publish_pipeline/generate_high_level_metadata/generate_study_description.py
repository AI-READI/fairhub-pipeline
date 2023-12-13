"""Process environmental sensor data files"""
import datetime
import json
import os
import tempfile
import uuid

import psycopg2

import config

# import azure.storage.blob as azureblob


def pipeline():
    """Reads the database for the study and generates a study_description.json file in the metadata folder."""

    conn = psycopg2.connect(
        host=config.FAIRHUB_DATABASE_HOST,
        database=config.FAIRHUB_DATABASE_NAME,
        user=config.FAIRHUB_DATABASE_USER,
        password=config.FAIRHUB_DATABASE_PASSWORD,
        port=config.FAIRHUB_DATABASE_PORT,
    )

    cur = conn.cursor()

    study_id = "c588f59c-cacb-4e52-99dd-95b37dcbfd5c"

    cur.execute("SELECT * FROM studies WHERE id = %s", (study_id))

    study = cur.fetchone()

    conn.commit()
    conn.close()

    return study

    # # generate temp metadata file called study_description.json
    # temp_metadata_file, temp_metadata_file_path = tempfile.mkstemp(
    #     prefix="study_description", suffix=".json", text=True
    # )

    # metadata_folder = "AI-READI/metadata"

    # sas_token = azureblob.generate_account_sas(
    #     account_name="b2aistaging",
    #     account_key=config.AZURE_STORAGE_ACCESS_KEY,
    #     resource_types=azureblob.ResourceTypes(container=True, object=True),
    #     permission=azureblob.AccountSasPermissions(read=True, write=True, list=True),
    #     expiry=datetime.datetime.now(datetime.timezone.utc)
    #     + datetime.timedelta(hours=1),
    # )

    # return
