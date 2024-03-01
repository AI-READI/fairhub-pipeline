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
    """Reads the database for the dataset and generates a dataset_description.json file in the metadata folder."""

    dataset_metadata = {}

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

    identifier = {}

    # Get the dataset identifier
    cur.execute(
        "SELECT doi FROM version WHERE dataset_id = %s",
        (dataset_id,),
    )

    doi = cur.fetchone()
    identifier["identifierValue"] = doi[0]
    identifier["identifierType"] = "DOI"

    dataset_metadata["identifier"] = identifier

    titles = []

    # Get the dataset titles
    cur.execute(
        "SELECT title, type FROM dataset_title WHERE dataset_id = %s",
        (dataset_id,),
    )

    dataset_titles = cur.fetchall()

    if dataset_titles is not None:
        for title in dataset_titles:
            item = {}

            item["titleValue"] = title[0]

            if not title[1] == "MainTitle":
                item["titleType"] = title[1]

            titles.append(item)

    dataset_metadata["title"] = titles

    # todo: generating a random uuid for now
    # Get the dataset version
    version = str(uuid.uuid4())

    dataset_metadata["version"] = version

    alternate_identifiers = []

    # Get the dataset alternate identifiers
    cur.execute(
        "SELECT identifier, type FROM dataset_alternate_identifier WHERE dataset_id = %s",
        (dataset_id,),
    )

    dataset_alternate_identifiers = cur.fetchall()

    if dataset_alternate_identifiers is not None:
        for alternate_identifier in dataset_alternate_identifiers:
            item = {}

            item["alternateIdentifierValue"] = alternate_identifier[0]
            item["alternateIdentifierType"] = alternate_identifier[1]

            alternate_identifiers.append(item)

    dataset_metadata["alternateIdentifier"] = alternate_identifiers

    creators = []

    # Get the dataset creators
    cur.execute(
        "SELECT family_name, given_name, name_type, name_identifier, name_identifier_scheme, name_identifier_scheme_uri, affiliations FROM dataset_contributor WHERE dataset_id = %s AND creator = true",
        (dataset_id,),
    )

    dataset_creators = cur.fetchall()

    if dataset_creators is not None:
        for creator in dataset_creators:
            item = {}

            creator_name = ""

            if (creator[0] is not None and creator[0] != "") and (
                creator[1] is not None and creator[1] != ""
            ):
                creator_name = f"{creator[0]}, {creator[1]}"
            elif creator[0] is not None and creator[0] != "":
                creator_name = creator[0]
            elif creator[1] is not None and creator[1] != "":
                creator_name = creator[1]

            item["creatorName"] = creator_name
            item["nameType"] = creator[2]

            name_identifier = {}
            name_identifier["nameIdentifierValue"] = creator[3]
            name_identifier["nameIdentifierScheme"] = creator[4]
            if creator[5] is not None and creator[5] != "":
                name_identifier["schemeURI"] = creator[5]

            item["nameIdentifier"] = [name_identifier]

            affiliations = creator[6]

            item["affiliation"] = []

            for affiliation in affiliations:
                affiliation_item = {}

                affiliation_item["affiliationName"] = affiliation["name"]
                if (
                    affiliation["identifier"] is not None
                    and affiliation["identifier"] != ""
                ):
                    affiliation_item["affiliationIdentifier"] = affiliation[
                        "identifier"
                    ]
                if affiliation["scheme"] is not None and affiliation["scheme"] != "":
                    affiliation_item["affiliationIdentifierScheme"] = affiliation[
                        "scheme"
                    ]

                if (
                    affiliation["scheme_uri"] is not None
                    and affiliation["scheme_uri"] != ""
                ):
                    affiliation_item["schemeURI"] = affiliation["scheme_uri"]

                item["affiliation"].append(affiliation_item)

            creators.append(item)

    dataset_metadata["creator"] = creators

    contributors = []

    # Get the dataset contributors
    cur.execute(
        "SELECT family_name, given_name, name_type, name_identifier, name_identifier_scheme, name_identifier_scheme_uri, contributor_type, affiliations FROM dataset_contributor WHERE dataset_id = %s AND creator = false",
        (dataset_id,),
    )

    dataset_contributors = cur.fetchall()

    if dataset_contributors is not None:
        for contributor in dataset_contributors:
            item = {}

            contributor_name = ""

            if (contributor[0] is not None and contributor[0] != "") and (
                contributor[1] is not None and contributor[1] != ""
            ):
                contributor_name = f"{contributor[0]}, {contributor[1]}"
            elif contributor[0] is not None and contributor[0] != "":
                contributor_name = contributor[0]
            elif contributor[1] is not None and contributor[1] != "":
                contributor_name = contributor[1]

            item["contributorName"] = contributor_name

            item["nameType"] = contributor[1]

            name_identifier = {}
            name_identifier["nameIdentifierValue"] = contributor[2]
            name_identifier["nameIdentifierScheme"] = contributor[3]

            if contributor[4] is not None and contributor[4] != "":
                name_identifier["schemeURI"] = contributor[4]

            item["nameIdentifier"] = [name_identifier]

            item["contributorType"] = contributor[5]

            affiliations = contributor[6]

            item["affiliation"] = []

            for affiliation in affiliations:
                affiliation_item = {}

                affiliation_item["affiliationName"] = affiliation["name"]
                if (
                    affiliation["identifier"] is not None
                    and affiliation["identifier"] != ""
                ):
                    affiliation_item["affiliationIdentifier"] = affiliation[
                        "identifier"
                    ]
                if affiliation["scheme"] is not None and affiliation["scheme"] != "":
                    affiliation_item["affiliationIdentifierScheme"] = affiliation[
                        "scheme"
                    ]
                if (
                    affiliation["scheme_uri"] is not None
                    and affiliation["scheme_uri"] != ""
                ):
                    affiliation_item["schemeURI"] = affiliation["scheme_uri"]

                item["affiliation"].append(affiliation_item)

            contributors.append(item)

    dataset_metadata["contributor"] = contributors

    # Get the publication year
    publication_year = str(datetime.datetime.now().year)

    dataset_metadata["publicationYear"] = publication_year

    dates = []

    # Get the dataset dates
    cur.execute(
        "SELECT date, type, information FROM dataset_date WHERE dataset_id = %s",
        (dataset_id,),
    )

    dataset_dates = cur.fetchall()

    if dataset_dates is not None:
        for date in dataset_dates:
            item = {}

            input_timestamp = datetime.datetime.fromtimestamp(date[0] / 1000)

            item["dateValue"] = input_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
            item["dateType"] = date[1]
            if date[2] is not None and date[2] != "":
                item["dateInformation"] = date[2]

            dates.append(item)

    dataset_metadata["date"] = dates

    resource_type = {}

    # Get the dataset resource type
    cur.execute(
        "SELECT resource_type FROM dataset_other WHERE dataset_id = %s",
        (dataset_id,),
    )

    dataset_resource_type = cur.fetchone()

    resource_type["resourceTypeValue"] = dataset_resource_type[0]
    resource_type["resourceTypeGeneral"] = "Dataset"

    dataset_metadata["resourceType"] = resource_type

    dataset_de_ident_level = {}

    # Get the dataset de-identification levels
    cur.execute(
        "SELECT type, direct, hipaa, dates, nonarr, k_anon, details FROM dataset_de_ident_level WHERE dataset_id = %s",
        (dataset_id,),
    )

    de_ident_level = cur.fetchone()

    dataset_de_ident_level["deIdentType"] = de_ident_level[0]
    dataset_de_ident_level["deIdentDirect"] = de_ident_level[1]
    dataset_de_ident_level["deIdentHIPAA"] = de_ident_level[2]
    dataset_de_ident_level["deIdentDates"] = de_ident_level[3]
    dataset_de_ident_level["deIdentNonarr"] = de_ident_level[4]
    dataset_de_ident_level["deIdentKAnon"] = de_ident_level[5]
    if de_ident_level[6] is not None and de_ident_level[6] != "":
        dataset_de_ident_level["deIdentDetails"] = de_ident_level[6]

    dataset_metadata["datasetDeIdentLevel"] = dataset_de_ident_level

    dataset_consent = {}

    # Get the dataset consent
    cur.execute(
        "SELECT type, noncommercial, geog_restrict, research_type, genetic_only, no_methods, details FROM dataset_consent WHERE dataset_id = %s",
        (dataset_id,),
    )

    consent = cur.fetchone()

    dataset_consent["consentType"] = consent[0]
    dataset_consent["consentNoncommercial"] = consent[1]
    dataset_consent["consentGeogRestrict"] = consent[2]
    dataset_consent["consentResearchType"] = consent[3]
    dataset_consent["consentGeneticOnly"] = consent[4]
    dataset_consent["consentNoMethods"] = consent[5]
    if consent[6] is not None and consent[6] != "":
        dataset_consent["consentsDetails"] = consent[6]

    dataset_metadata["datasetConsent"] = dataset_consent

    descriptions = []

    # Get the dataset descriptions
    cur.execute(
        "SELECT description, type FROM dataset_description WHERE dataset_id = %s",
        (dataset_id,),
    )

    dataset_descriptions = cur.fetchall()

    if dataset_descriptions is not None:
        for description in dataset_descriptions:
            item = {}

            item["descriptionValue"] = description[0]
            item["descriptionType"] = description[1]

            descriptions.append(item)

    dataset_metadata["description"] = descriptions

    cur.execute(
        "SELECT language FROM dataset_other WHERE dataset_id = %s",
        (dataset_id,),
    )

    dataset_language = cur.fetchone()

    if dataset_language[0] is not None and dataset_language[0] != "":
        dataset_metadata["language"] = dataset_language[0]

    subjects = []

    # Get the dataset subjects
    cur.execute(
        "SELECT subject, scheme, scheme_uri, value_uri, classification_code FROM dataset_subject WHERE dataset_id = %s",
        (dataset_id,),
    )

    dataset_subjects = cur.fetchall()

    if dataset_subjects is not None:
        for subject in dataset_subjects:
            item = {}

            item["subjectValue"] = subject[0]
            if subject[1] is not None and subject[1] != "":
                item["subjectScheme"] = subject[1]
            if subject[2] is not None and subject[2] != "":
                item["schemeURI"] = subject[2]
            if subject[3] is not None and subject[3] != "":
                item["valueURI"] = subject[3]
            if subject[4] is not None and subject[4] != "":
                item["classificationCode"] = subject[4]

            subjects.append(item)

    dataset_metadata["subject"] = subjects

    managing_organization = {}

    # Get the dataset managing organization
    cur.execute(
        "SELECT managing_organization_name, managing_organization_ror_id FROM dataset_other WHERE dataset_id = %s",
        (dataset_id,),
    )

    dataset_managing_organization = cur.fetchone()

    managing_organization["name"] = dataset_managing_organization[0]
    if (
        dataset_managing_organization[1] is not None
        and dataset_managing_organization[1] != ""
    ):
        managing_organization["rorId"] = dataset_managing_organization[1]

    dataset_metadata["managingOrganization"] = managing_organization

    access_details = {}

    # Get the dataset access details
    cur.execute(
        "SELECT type, description, url, url_last_checked FROM dataset_access WHERE dataset_id = %s",
        (dataset_id,),
    )

    dataset_access = cur.fetchone()

    access_details["description"] = dataset_access[1]
    if dataset_access[2] is not None and dataset_access[2] != "":
        access_details["url"] = dataset_access[2]

    if dataset_access[3] is not None and dataset_access[3] != "":
        timestamp = datetime.datetime.fromtimestamp(dataset_access[3] / 1000)
        access_details["urlLastChecked"] = timestamp.strftime("%Y-%m-%d")

    # Get the dataset Access Type
    dataset_metadata["accessType"] = dataset_access[0]
    dataset_metadata["accessDetails"] = access_details

    rights = []

    # Get the dataset rights
    cur.execute(
        "SELECT rights, uri, identifier, identifier_scheme, identifier_scheme_uri FROM dataset_rights WHERE dataset_id = %s",
        (dataset_id,),
    )

    dataset_rights = cur.fetchall()

    if dataset_rights is not None:
        for right in dataset_rights:
            item = {}

            item["rightsName"] = right[0]

            if right[1] is not None and right[1] != "":
                item["rightsURI"] = right[1]
            if right[2] is not None and right[2] != "":
                item["rightsIdentifier"] = right[2]
            if right[3] is not None and right[3] != "":
                item["rightsIdentifierScheme"] = right[3]
            if right[4] is not None and right[4] != "":
                item["schemeURI"] = right[4]

            rights.append(item)

    dataset_metadata["rights"] = rights

    # Create the publisher object
    dataset_metadata["publisher"] = {
        "publisherName": "FAIRhub",
    }

    sizes = []

    # Get the dataset sizes
    cur.execute(
        "SELECT size FROM dataset_other WHERE dataset_id = %s",
        (dataset_id,),
    )

    dataset_sizes = cur.fetchone()

    if len(dataset_sizes[0]) > 0:
        for size in dataset_sizes[0]:
            sizes.append(size)

    dataset_metadata["size"] = sizes

    formats = []

    # Get the dataset formats
    cur.execute(
        "SELECT format FROM dataset_other WHERE dataset_id = %s",
        (dataset_id,),
    )

    dataset_formats = cur.fetchone()

    if len(dataset_formats[0]) > 0:
        for dataset_format in dataset_formats[0]:
            formats.append(dataset_format)

    dataset_metadata["format"] = formats

    funding_references = []

    # Get the dataset funding references
    cur.execute(
        "SELECT name, identifier, identifier_type, identifier_scheme_uri, award_number, award_uri, award_title FROM dataset_funder WHERE dataset_id = %s",
        (dataset_id,),
    )

    dataset_funding_references = cur.fetchall()

    if dataset_funding_references is not None:
        for funding_reference in dataset_funding_references:
            item = {}

            item["funderName"] = funding_reference[0]
            item["funderIdentifier"] = {}
            item["funderIdentifier"]["funderIdentifierValue"] = funding_reference[1]

            if funding_reference[2] is not None and funding_reference[2] != "":
                item["funderIdentifier"]["funderIdentifierType"] = funding_reference[2]
            if funding_reference[3] is not None and funding_reference[3] != "":
                item["funderIdentifier"]["schemeURI"] = funding_reference[3]

            item["awardNumber"] = {}
            item["awardNumber"]["awardNumberValue"] = funding_reference[4]

            if funding_reference[5] is not None and funding_reference[5] != "":
                item["awardNumber"]["awardURI"] = funding_reference[5]
            if funding_reference[6] is not None and funding_reference[6] != "":
                item["awardTitle"] = funding_reference[6]

            funding_references.append(item)

    dataset_metadata["fundingReference"] = funding_references

    related_identifiers = []

    # Get the dataset related identifiers
    cur.execute(
        "SELECT identifier, identifier_type, relation_type, related_metadata_scheme, scheme_uri, scheme_type, resource_type FROM dataset_related_identifier WHERE dataset_id = %s",
        (dataset_id,),
    )

    dataset_related_identifiers = cur.fetchall()

    if dataset_related_identifiers is not None:
        for related_identifier in dataset_related_identifiers:
            item = {}

            item["relatedIdentifierValue"] = related_identifier[0]
            item["relatedIdentifierType"] = related_identifier[1]
            item["relationType"] = related_identifier[2]

            if related_identifier[3] is not None and related_identifier[3] != "":
                item["relatedMetadataScheme"] = related_identifier[3]
            if related_identifier[4] is not None and related_identifier[4] != "":
                item["schemeURI"] = related_identifier[4]
            if related_identifier[5] is not None and related_identifier[5] != "":
                item["schemeType"] = related_identifier[5]
            if related_identifier[6] is not None and related_identifier[6] != "":
                item["resourceTypeGeneral"] = related_identifier[6]

            related_identifiers.append(item)

    dataset_metadata["relatedIdentifier"] = related_identifiers

    conn.commit()
    conn.close()

    # Create a temporary folder on the local machine
    temp_folder_path = tempfile.mkdtemp()

    temp_file_path = pathlib.Path(temp_folder_path, "dataset_description.json")

    data_is_valid = pyfairdatatools.validate.validate_dataset_description(
        data=dataset_metadata
    )

    # sourcery skip: raise-specific-error
    if not data_is_valid:
        raise Exception("Dataset description is not valid")

    pyfairdatatools.generate.generate_dataset_description(
        data=dataset_metadata, file_path=temp_file_path, file_type="json"
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
        blob=f"{metadata_folder}/dataset_description.json",
    )

    with open(temp_file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

    return
