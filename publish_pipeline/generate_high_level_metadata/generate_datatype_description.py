"""Process environmental sensor data files"""

import datetime
import pathlib
import tempfile

import azure.storage.blob as azureblob
import psycopg2
import pyfairdatatools

import config


def pipeline():
    """
    Reads the database for the dataset folders and 
    generates a datatype_description.yaml file in the metadata folder.
    """

    datatype_description = {}

    conn = psycopg2.connect(
        host=config.FAIRHUB_DATABASE_HOST,
        database=config.FAIR_HUB_DATABASE_NAME,
        user=config.FAIRHUB_DATABASE_USER,
        password=config.FAIRHUB_DATABASE_PASSWORD,
        port=config.FAIRHUB_DATABASE_PORT,
    )

    cur = conn.cursor()

    study_id = "c588f59c-cacb-4e52-99dd-95b37dcbfd5c"
    dataset_id = "af4be921-e507-41a9-9328-4cbb4b7dca1c"

    cur.execute("SELECT * FROM dataset WHERE id = %s AND study_id = %s", (dataset_id, study_id))

    dataset = cur.fetchone()
