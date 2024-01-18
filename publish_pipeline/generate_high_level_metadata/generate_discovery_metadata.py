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
    """Reads the database for the dataset and generates a discovery.md file in the metadata folder."""

    discovery_metadata = {}

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

    discovery_metadata["Title"] = title

    identifier = "10.5281/zenodo.7641684"

    discovery_metadata["Identifier"] = identifier

    version = str(uuid.uuid4())

    discovery_metadata["Version"] = version

    publication_date = datetime.datetime.now().strftime("%Y-%m-%d")

    discovery_metadata["PublicationDate"] = publication_date

    detailed_description = ""

    cur.execute(
        "SELECT detailed_description FROM study_description WHERE study_id = %s",
        (study_id,),
    )
    study_description = cur.fetchone()
    detailed_description = study_description[0]
    discovery_metadata["About"] = detailed_description

    # license

    license_text = ""

    cur.execute(
        "SELECT rights FROM dataset_rights WHERE dataset_id = %s",
        (dataset_id,),
    )

    dataset_rights = cur.fetchone()
    # license_text = dataset_other.join(",")
    license_text = dataset_rights[0]
    discovery_metadata["License"] = license_text

    acknowledgement = ""

    cur.execute(
        "SELECT acknowledgement FROM dataset_other WHERE dataset_id = %s",
        (dataset_id,),
    )

    dataset_acknowledgement = cur.fetchone()
    acknowledgement = dataset_acknowledgement[0]
    if acknowledgement:
        discovery_metadata["Acknowledgement"] = acknowledgement

    # conn.commit()
    conn.close()
    return
