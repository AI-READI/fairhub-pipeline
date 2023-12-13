# pylint: disable=line-too-long
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

    study_metadata = {}

    conn = psycopg2.connect(
        host=config.FAIRHUB_DATABASE_HOST,
        database=config.FAIRHUB_DATABASE_NAME,
        user=config.FAIRHUB_DATABASE_USER,
        password=config.FAIRHUB_DATABASE_PASSWORD,
        port=config.FAIRHUB_DATABASE_PORT,
    )

    cur = conn.cursor()

    study_id = "c588f59c-cacb-4e52-99dd-95b37dcbfd5c"

    cur.execute("SELECT * FROM study WHERE id = %s", (study_id,))

    study = cur.fetchone()

    if study is None:
        return "Study not found"

    identification_module = {}

    cur.execute(
        "SELECT identifier, identifier_type, identifier_domain, identifier_link FROM study_identification WHERE study_id = %s AND secondary = false",
        (study_id,),
    )

    primary_study_identification = cur.fetchone()

    identification_module["OrgStudyIdInfo"] = {
        "OrgStudyId": primary_study_identification[0],
        "OrgStudyIdType": primary_study_identification[1],
        "OrgStudyIdDomain": primary_study_identification[2],
        "OrgStudyIdLink": primary_study_identification[3],
    }

    cur.execute(
        "SELECT identifier, identifier_type, identifier_domain, identifier_link FROM study_identification WHERE study_id = %s AND secondary = true",
        (study_id,),
    )

    secondary_study_identification = cur.fetchall()

    identification_module["SecondaryIdInfo"] = []

    for row in secondary_study_identification:
        item = {
            "SecondaryId": row[0],
            "SecondaryIdType": row[1],
            "SecondaryIdDomain": row[2],
            "SecondaryIdLink": row[3],
        }

        identification_module["SecondaryIdInfo"].append(item)

    study_metadata["IdentificationModule"] = identification_module

    status_module = {}

    cur.execute(
        "SELECT overall_status, why_stopped, start_date, start_date_type, completion_date, completion_date_type FROM study_status WHERE study_id = %s",
        (study_id,),
    )

    study_status = cur.fetchone()

    status_module["OverallStatus"] = study_status[0]
    status_module["WhyStopped"] = study_status[1]

    start_date = datetime.datetime.strptime(study_status[2], "%Y-%m-%d %H:%M:%S")

    status_module["StartDateStruct"] = {
        # date format: Month DD, YYYY
        "StartDate": start_date.strftime("%B %d, %Y"),
        "StartDateType": study_status[3],
    }

    completion_date = datetime.datetime.strptime(study_status[4], "%Y-%m-%d %H:%M:%S")

    status_module["CompletionDateStruct"] = {
        "CompletionDate": completion_date.strftime("%B %d, %Y"),
        "CompletionDateType": study_status[5],
    }

    study_metadata["StatusModule"] = status_module

    sponsor_collaborators_module = {}

    cur.execute(
        "SELECT responsible_party_type, responsible_party_investigator_name, responsible_party_investigator_title, responsible_party_investigator_affiliation, lead_sponsor_name, collaborator_name FROM study_sponsors_collaborators WHERE study_id = %s",
        (study_id,),
    )

    sponsor_collaborators = cur.fetchone()

    sponsor_collaborators_module["ResponsibleParty"] = {
        "ResponsiblePartyType": sponsor_collaborators[0],
        "ResponsiblePartyInvestigatorFullName": sponsor_collaborators[1],
        "ResponsiblePartyInvestigatorTitle": sponsor_collaborators[2],
        "ResponsiblePartyInvestigatorAffiliation": sponsor_collaborators[3],
    }

    sponsor_collaborators_module["LeadSponsor"] = {
        "LeadSponsorName": sponsor_collaborators[4]
    }

    sponsor_collaborators_module["CollaboratorList"] = []

    sponsor_collaborators = sponsor_collaborators[5]

    for row in sponsor_collaborators:
        item = {"CollaboratorName": row}

        sponsor_collaborators_module["CollaboratorList"].append(item)

    study_metadata["SponsorCollaboratorsModule"] = sponsor_collaborators_module

    conn.commit()
    conn.close()

    return json.dumps(study_metadata, indent=4, sort_keys=True, default=str)

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
