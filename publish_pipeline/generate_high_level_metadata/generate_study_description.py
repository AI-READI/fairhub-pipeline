# pylint: disable=line-too-long
"""Process environmental sensor data files"""

import datetime
import pathlib
import tempfile

import azure.storage.blob as azureblob
import psycopg2
import pyfairdatatools

import config


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

    cur.execute("SELECT title, acronym FROM study WHERE id = %s", (study_id,))

    study = cur.fetchone()

    if study[0] is None:
        return "Study not found"

    identification_module = {}

    # Get the study identification metadata
    cur.execute(
        "SELECT identifier, identifier_type, identifier_domain, identifier_link FROM study_identification WHERE study_id = %s AND secondary = false",
        (study_id,),
    )

    primary_study_identification = cur.fetchone()

    identification_module["officialTitle"] = study[0]

    if study[1] is not None and study[1] != "":
        identification_module["acronym"] = {}

    identification_module["orgStudyIdInfo"] = {}

    # Study Identifier
    identification_module["orgStudyIdInfo"]["orgStudyId"] = (
        primary_study_identification[0]
    )
    # Study Identifier Type
    identification_module["orgStudyIdInfo"]["orgStudyIdType"] = (
        primary_study_identification[1]
    )

    if primary_study_identification[2] and primary_study_identification[2] != "":
        # Study Identifier Domain
        identification_module["orgStudyIdInfo"]["orgStudyIdDomain"] = (
            primary_study_identification[2]
        )

    if primary_study_identification[3] and primary_study_identification[3] != "":
        # Study Identifier Link
        identification_module["orgStudyIdInfo"]["orgStudyIdLink"] = (
            primary_study_identification[3]
        )

    # Get the secondary study identification metadata
    cur.execute(
        "SELECT identifier, identifier_type, identifier_domain, identifier_link FROM study_identification WHERE study_id = %s AND secondary = true",
        (study_id,),
    )

    secondary_study_identification = cur.fetchall()

    identification_module["secondaryIdInfoList"] = []

    for row in secondary_study_identification:
        item = {}
        # Study Identifier and Study Identifier Type
        item["secondaryId"] = row[0]
        item["secondaryIdType"] = row[1]

        if row[2]:
            # Study Identifer Domain
            item["secondaryIdDomain"] = row[2]

        if row[3]:
            # Study Identifier Link
            item["secondaryIdLink"] = row[3]

        identification_module["secondaryIdInfoList"].append(item)

    study_metadata["identificationModule"] = identification_module

    status_module = {}

    # Get the study status metadata
    cur.execute(
        "SELECT overall_status, why_stopped, start_date, start_date_type, completion_date, completion_date_type FROM study_status WHERE study_id = %s",
        (study_id,),
    )

    study_status = cur.fetchone()

    status_module["overallStatus"] = study_status[0]
    status_module["whyStopped"] = study_status[1]

    start_date = datetime.datetime.strptime(study_status[2], "%Y-%m-%d %H:%M:%S")

    status_module["startDateStruct"] = {
        # date format: Month DD, YYYY
        "startDate": start_date.strftime("%B %d, %Y"),
        "startDateType": study_status[3],
    }

    completion_date = datetime.datetime.strptime(study_status[4], "%Y-%m-%d %H:%M:%S")

    status_module["completionDateStruct"] = {
        "completionDate": completion_date.strftime("%B %d, %Y"),
        "completionDateType": study_status[5],
    }

    study_metadata["statusModule"] = status_module

    sponsor_collaborators_module = {}

    # Get the study sponsor metadata
    cur.execute(
        "SELECT responsible_party_type, responsible_party_investigator_first_name, responsible_party_investigator_last_name, responsible_party_investigator_title, responsible_party_investigator_identifier_value, responsible_party_investigator_identifier_scheme, responsible_party_investigator_identifier_scheme_uri, responsible_party_investigator_affiliation_name, responsible_party_investigator_affiliation_identifier_value, responsible_party_investigator_affiliation_identifier_scheme, responsible_party_investigator_affiliation_identifier_scheme_uri, lead_sponsor_name, lead_sponsor_identifier, lead_sponsor_scheme, lead_sponsor_scheme_uri FROM study_sponsors WHERE study_id = %s",
        (study_id,),
    )

    study_sponsors = cur.fetchone()

    responsible_party = {}

    responsible_party["responsiblePartyType"] = study_sponsors[0]

    if study_sponsors[1] is not None and study_sponsors[1] != "":
        responsible_party["responsiblePartyInvestigatorFirstName"] = study_sponsors[1]
    if study_sponsors[2] is not None and study_sponsors[2] != "":
        responsible_party["responsiblePartyInvestigatorLastName"] = study_sponsors[2]
    if study_sponsors[3] is not None and study_sponsors[3] != "":
        responsible_party["responsiblePartyInvestigatorTitle"] = study_sponsors[3]
    if study_sponsors[4] is not None and study_sponsors[4] != "":
        responsible_party["responsiblePartyInvestigatorIdentifier"] = {}

        responsible_party["responsiblePartyInvestigatorIdentifier"][
            "responsiblePartyInvestigatorIdentifierValue"
        ] = study_sponsors[4]

        if study_sponsors[5] is not None and study_sponsors[5] != "":
            responsible_party["responsiblePartyInvestigatorIdentifier"][
                "responsiblePartyInvestigatorIdentifierScheme"
            ] = study_sponsors[5]

        if study_sponsors[6] is not None and study_sponsors[6] != "":
            responsible_party["responsiblePartyInvestigatorIdentifier"]["schemeURI"] = (
                study_sponsors[6]
            )

    if study_sponsors[7] is not None and study_sponsors[7] != "":
        responsible_party["responsiblePartyInvestigatorAffiliation"] = {}

        responsible_party["responsiblePartyInvestigatorAffiliation"][
            "responsiblePartyInvestigatorAffiliationName"
        ] = study_sponsors[7]

        if study_sponsors[8] is not None and study_sponsors[8] != "":
            responsible_party["responsiblePartyInvestigatorAffiliation"][
                "responsiblePartyInvestigatorAffiliationIdentifier"
            ] = {}

            responsible_party["responsiblePartyInvestigatorAffiliation"][
                "responsiblePartyInvestigatorAffiliationIdentifier"
            ][
                "responsiblePartyInvestigatorAffiliationIdentifierValue"
            ] = study_sponsors[
                8
            ]

            if study_sponsors[9] is not None and study_sponsors[9] != "":
                responsible_party["responsiblePartyInvestigatorAffiliation"][
                    "responsiblePartyInvestigatorAffiliationIdentifier"
                ][
                    "responsiblePartyInvestigatorAffiliationIdentifierScheme"
                ] = study_sponsors[
                    9
                ]

            if study_sponsors[10] is not None and study_sponsors[10] != "":
                responsible_party["responsiblePartyInvestigatorAffiliation"][
                    "responsiblePartyInvestigatorAffiliationIdentifier"
                ]["schemeURI"] = study_sponsors[10]

    sponsor_collaborators_module["responsibleParty"] = responsible_party

    lead_sponsor = {"leadSponsorName": study_sponsors[11]}

    if study_sponsors[12] is not None and study_sponsors[12] != "":
        lead_sponsor["leadSponsor"]["leadSponsorIdentifier"] = {
            "leadSponsorIdentifierValue": study_sponsors[12]
        }
        if study_sponsors[13] is not None and study_sponsors[13] != "":
            lead_sponsor["leadSponsor"]["leadSponsorIdentifier"][
                "leadSponsorIdentifierScheme"
            ] = study_sponsors[13]

    sponsor_collaborators_module["leadSponsor"] = lead_sponsor

    # Get the study collaborators metadata
    cur.execute(
        "SELECT name, identifier, scheme, scheme_uri FROM study_collaborators WHERE study_id = %s",
        (study_id,),
    )

    study_collaborators = cur.fetchall()

    collaborators = []

    for row in study_collaborators:
        item = {}

        item["collaboratorName"] = row[0]

        if row[1] is not None and row[1] != "":
            item["collaboratorNameIdentifier"] = {
                "collaboratorNameIdentifierValue": row[1]
            }

            if row[2] is not None and row[2] != "":
                item["collaboratorNameIdentifier"][
                    "collaboratorNameIdentifierScheme"
                ] = row[2]
            if row[3] is not None and row[3] != "":
                item["collaboratorNameIdentifier"]["schemeURI"] = row[3]

        collaborators.append(item)

    sponsor_collaborators_module["collaboratorList"] = collaborators

    study_metadata["sponsorCollaboratorsModule"] = sponsor_collaborators_module

    oversight_module = {}

    # Get the study oversight metadata
    cur.execute(
        "SELECT fda_regulated_drug, fda_regulated_device, human_subject_review_status, has_dmc FROM study_oversight WHERE study_id = %s",
        (study_id,),
    )

    study_oversight = cur.fetchone()

    if study_oversight[0] is not None and study_oversight[0] != "":
        oversight_module["isFDARegulatedDrug"] = study_oversight[0]
    if study_oversight[1] is not None and study_oversight[1] != "":
        oversight_module["isFDARegulatedDevice"] = study_oversight[1]

    oversight_module["humanSubjectReviewStatus"] = study_oversight[2]

    if study_oversight[3] is not None and study_oversight[3] != "":
        oversight_module["oversightHasDMC"] = study_oversight[3]

    study_metadata["oversightModule"] = oversight_module

    description_module = {}

    # Get the study description metadata
    cur.execute(
        "SELECT brief_summary, detailed_description FROM study_description WHERE study_id = %s",
        (study_id,),
    )

    study_description = cur.fetchone()

    description_module["briefSummary"] = study_description[0]
    if study_description[1] and study_description[1] != "":
        description_module["detailedDescription"] = study_description[1]

    study_metadata["descriptionModule"] = description_module

    conditions_module = {}

    # Get the study conditions metadata
    cur.execute(
        "SELECT name, classification_code, scheme, scheme_uri, condition_uri FROM study_conditions WHERE study_id = %s",
        (study_id,),
    )

    study_conditions = cur.fetchall()

    conditions_list = []

    for row in study_conditions:
        item = {}

        item["conditionName"] = row[0]

        if row[1] is not None and row[1] != "":
            item["conditionIdentifier"] = {"conditionClassificationCode": row[1]}

            if row[2] is not None and row[2] != "":
                item["conditionIdentifier"]["conditionScheme"] = row[2]

            if row[3] is not None and row[3] != "":
                item["conditionIdentifier"]["schemeURI"] = row[3]

            if row[4] is not None and row[4] != "":
                item["conditionIdentifier"]["conditionURI"] = row[4]

        conditions_list.append(item)

    conditions_module["conditionList"] = conditions_list

    # Get the study keywords metadata
    cur.execute(
        "SELECT name, classification_code, scheme, scheme_uri, keyword_uri FROM study_keywords WHERE study_id = %s",
        (study_id,),
    )

    study_keywords = cur.fetchall()

    keywords_list = []

    for row in study_keywords:
        item = {}

        item["keywordName"] = row[0]

        if row[1] is not None and row[1] != "":
            item["keywordIdentifier"] = {"keywordClassificationCode": row[1]}

            if row[2] is not None and row[2] != "":
                item["keywordIdentifier"]["keywordScheme"] = row[2]

            if row[3] is not None and row[3] != "":
                item["keywordIdentifier"]["schemeURI"] = row[3]

            if row[4] is not None and row[4] != "":
                item["keywordIdentifier"]["keywordURI"] = row[4]

        keywords_list.append(item)

    conditions_module["keywordList"] = keywords_list

    study_metadata["conditionsModule"] = conditions_module

    design_module = {}

    # Get the study design metadata
    cur.execute(
        "SELECT study_type, design_allocation, design_intervention_model, design_intervention_model_description, design_primary_purpose, design_masking, design_masking_description, design_who_masked_list, phase_list, enrollment_count, enrollment_type, number_arms,design_observational_model_list, design_time_perspective_list, bio_spec_retention, bio_spec_description, target_duration, number_groups_cohorts, isPatientRegistry FROM study_design WHERE study_id = %s",
        (study_id,),
    )

    study_design = cur.fetchone()

    study_type = study_design[0]
    design_module["studyType"] = study_type

    if study_type == "Interventional":
        design_module["designInfo"] = {}
        design_module["designInfo"]["designAllocation"] = study_design[1]
        design_module["designInfo"]["designInterventionModel"] = study_design[2]
        if study_design[3] and study_design[3] != "":
            design_module["designInfo"]["designInterventionModelDescription"] = (
                study_design[3]
            )
        design_module["designInfo"]["designPrimaryPurpose"] = study_design[4]

        design_module["designInfo"]["designMaskingInfo"] = {}
        design_module["designInfo"]["designMaskingInfo"]["designMasking"] = (
            study_design[5]
        )
        design_module["designInfo"]["designMaskingInfo"]["designMaskingDescription"] = (
            study_design[6]
        )

        design_module["designInfo"]["designMaskingInfo"]["designWhoMaskedList"] = []

        if study_design[7] is not None:
            for row in study_design[7]:
                design_module["designInfo"]["designMaskingInfo"][
                    "designWhoMaskedList"
                ].append(row)

        design_module["phaseList"] = []

        if study_design[8] is not None:
            for row in study_design[8]:
                design_module["phaseList"].append(row)

    design_module["enrollmentInfo"] = {}
    design_module["enrollmentInfo"]["enrollmentCount"] = str(study_design[9])
    design_module["enrollmentInfo"]["enrollmentType"] = study_design[10]

    if study_type == "interventional":
        design_module["numberArms"] = str(study_design[11])

    if study_type == "observational":
        design_module["designInfo"] = {}
        design_module["designInfo"]["designObservationalModelList"] = []

        if study_design[12] is not None:
            for row in study_design[12]:
                design_module["designInfo"]["designObservationalModelList"].append(row)

        design_module["designInfo"]["designTimePerspectiveList"] = []

        if study_design[13] is not None:
            for row in study_design[13]:
                design_module["designInfo"]["designTimePerspectiveList"].append(row)

        design_module["bioSpec"] = {}
        design_module["bioSpec"]["bioSpecRetention"] = study_design[14]

        if study_design[15] is not None and study_design[15] != "":
            design_module["bioSpec"]["bioSpecDescription"] = study_design[15]

        design_module["targetDuration"] = study_design[16]
        design_module["numberGroupsCohorts"] = str(study_design[17])

        if study_design[18] is not None and study_design[18] != "":
            design_module["isPatientRegistry"] = study_design[18]

    study_metadata["designModule"] = design_module

    arms_interventions_module = {}

    # Get the study arms and interventions metadata
    cur.execute(
        "SELECT label, type, description, intervention_list FROM study_arm WHERE study_id = %s",
        (study_id,),
    )

    study_arms = cur.fetchall()

    arms_interventions_module["armGroupList"] = []

    for row in study_arms:
        item = {}

        item["armGroupLabel"] = row[0]
        if study_type == "Interventional":
            item["armGroupType"] = row[1]

        item["armGroupDescription"] = row[2]

        if study_type == "Interventional" and row[3] is not None and len(row[3]) > 0:
            item["armGroupInterventionList"] = []

            for intervention in row[3]:
                item["armGroupInterventionList"].append(intervention)

        arms_interventions_module["armGroupList"].append(item)

    # Get the study interventions metadata
    cur.execute(
        "SELECT type, name, description, other_name_list FROM study_intervention WHERE study_id = %s",
        (study_id,),
    )

    study_interventions = cur.fetchall()

    arms_interventions_module["interventionList"] = []

    for row in study_interventions:
        item = {}

        item["interventionType"] = row[0]
        item["interventionName"] = row[1]
        item["interventionDescription"] = row[2]

        if row[3] is not None and len(row[3]) > 0:
            item["interventionOtherNameList"] = []

            for other_name in row[3]:
                item["interventionOtherNameList"].append(other_name)

        arms_interventions_module["interventionList"].append(item)

    study_metadata["armsInterventionsModule"] = arms_interventions_module

    eligibility_module = {}

    # Get the study eligibility metadata
    cur.execute(
        "SELECT sex, gender_based, gender_description, minimum_age_value, minimum_age_unit, maximum_age_value, maximum_age_unit, healthy_volunteers, inclusion_criteria, exclusion_criteria, study_population, sampling_method FROM study_eligibility WHERE study_id = %s",
        (study_id,),
    )

    study_eligibility = cur.fetchone()

    eligibility_module["sex"] = study_eligibility[0]
    eligibility_module["genderBased"] = study_eligibility[1]
    eligibility_module["genderDescription"] = study_eligibility[2]
    eligibility_module["minimumAge"] = f"{study_eligibility[3]} {study_eligibility[4]}"
    eligibility_module["maximumAge"] = f"{study_eligibility[5]} {study_eligibility[6]}"
    eligibility_module["healthyVolunteers"] = study_eligibility[7]

    if study_type == "Observational":
        eligibility_module["studyPopulation"] = study_eligibility[10]
        eligibility_module["samplingMethod"] = study_eligibility[11]

    eligibility_criteria = {
        "eligibilityCriteriaInclusion": [],
        "eligibilityCriteriaExclusion": [],
    }

    if study_eligibility[8] is not None and len(study_eligibility[8]) > 0:
        eligibility_criteria["eligibilityCriteriaInclusion"] = study_eligibility[8]

    if study_eligibility[9] is not None and len(study_eligibility[9]) > 0:
        eligibility_criteria["eligibilityCriteriaExclusion"] = study_eligibility[9]

    eligibility_module["eligibilityCriteria"] = eligibility_criteria

    study_metadata["eligibilityModule"] = eligibility_module

    contacts_locations_module = {}

    # Get the study contacts and locations metadata
    cur.execute(
        "SELECT first_name, last_name, degree, identifier, identifier_scheme, identifier_scheme_uri, affiliation, affiliation_identifier, affiliation_identifier_scheme, affiliation_identifier_scheme_uri, phone, phone_ext, email_address FROM study_central_contact WHERE study_id = %s",
        (study_id,),
    )

    study_central_contacts = cur.fetchall()

    central_contacts = []

    if study_central_contacts is not None:
        for row in study_central_contacts:
            item = {}

            item["centralContactFirstName"] = row[0]
            item["centralContactLastName"] = row[1]

            if row[2] is not None and row[2] != "":
                item["centralContactDegree"] = row[2]

            if row[3] is not None and row[3] != "":
                item["centralContactIdentifier"] = {}

                item["centralContactIdentifier"]["centralContactIdentifierValue"] = row[
                    3
                ]
                item["centralContactIdentifierScheme"] = row[4]

                if row[5] is not None and row[5] != "":
                    item["schemeURI"] = row[5]

            item["centralContactAffiliation"] = {
                "centralContactAffiliationName": row[6]
            }

            if row[7] is not None and row[7] != "":
                item["centralContactAffiliation"][
                    "centralContactAffiliationIdentifier"
                ] = {}

                item["centralContactAffiliation"][
                    "centralContactAffiliationIdentifier"
                ]["centralContactAffiliationIdentifierValue"] = row[7]
                item["centralContactAffiliation"][
                    "centralContactAffiliationIdentifier"
                ]["centralContactAffiliationIdentifierScheme"] = row[8]

                if row[9] is not None and row[9] != "":
                    item["centralContactAffiliation"][
                        "centralContactAffiliationIdentifier"
                    ]["schemeURI"] = row[9]

            if row[10] is not None and row[10] != "":
                item["centralContactPhone"] = row[10]

            if row[11] is not None and row[11] != "":
                item["centralContactPhoneExt"] = row[11]

            item["centralContactEMail"] = row[12]

            central_contacts.append(item)

    contacts_locations_module["centralContactList"] = central_contacts

    # Get the study contacts metadata
    cur.execute(
        "SELECT first_name, last_name, degree, identifier, identifier_scheme, identifier_scheme_uri, affiliation, affiliation_identifier, affiliation_identifier_scheme, affiliation_identifier_scheme_uri, role FROM study_overall_official WHERE study_id = %s",
        (study_id,),
    )

    study_overall_officials = cur.fetchall()

    overall_officals = []

    if study_overall_officials is not None:
        for row in study_overall_officials:
            item = {}

            item["overallOfficialFirstName"] = row[0]
            item["overallOfficialLastName"] = row[1]
            item["overallOfficialDegree"] = row[2]

            if row[3] is not None and row[3] != "":
                item["overallOfficialIdentifier"] = {}

                item["overallOfficialIdentifier"]["overallOfficialIdentifierValue"] = (
                    row[3]
                )
                item["overallOfficialIdentifierScheme"] = row[4]

                if row[5] is not None and row[5] != "":
                    item["overallOfficialIdentifier"]["schemeURI"] = row[5]

            item["overallOfficialAffiliation"] = {
                "overallOfficialAffiliationName": row[6]
            }

            if row[7] is not None and row[7] != "":
                item["overallOfficialAffiliation"][
                    "overallOfficialAffiliationIdentifier"
                ] = {}

                item["overallOfficialAffiliation"][
                    "overallOfficialAffiliationIdentifier"
                ]["overallOfficialAffiliationIdentifierValue"] = row[7]
                item["overallOfficialAffiliation"][
                    "overallOfficialAffiliationIdentifier"
                ]["overallOfficialAffiliationIdentifierScheme"] = row[8]

                if row[9] is not None and row[9] != "":
                    item["overallOfficialAffiliation"][
                        "overallOfficialAffiliationIdentifier"
                    ]["schemeURI"] = row[9]

            if row[10] is not None and row[10] != "":
                item["overallOfficialRole"] = row[10]

            overall_officals.append(item)

    contacts_locations_module["overallOfficialList"] = overall_officals

    # Get the study locations metadata
    cur.execute(
        "SELECT facility, status, city, state, zip, country, identifier, identifier_scheme, identifier_scheme_uri FROM study_location WHERE study_id = %s",
        (study_id,),
    )

    study_locations = cur.fetchall()

    location_list = []

    if study_locations is not None:
        for row in study_locations:
            item = {}

            item["locationFacility"] = row[0]
            item["locationStatus"] = row[1]
            item["locationCity"] = row[2]

            if row[3] is not None and row[3] != "":
                item["locationState"] = row[3]

            if row[4] is not None and row[4] != "":
                item["locationZip"] = row[4]

            item["locationCountry"] = row[5]

            if row[6] is not None and row[6] != "":
                item["locationIdentifier"] = {}

                item["locationIdentifier"]["locationIdentifierValue"] = row[6]
                item["locationIdentifierScheme"] = row[7]

                if row[8] is not None and row[8] != "":
                    item["locationIdentifier"]["schemeURI"] = row[8]

            location_list.append(item)

    contacts_locations_module["locationList"] = location_list

    study_metadata["contactsLocationsModule"] = contacts_locations_module

    conn.commit()
    conn.close()

    # Create a temporary folder on the local machine
    temp_folder_path = tempfile.mkdtemp()

    temp_file_path = pathlib.Path(temp_folder_path, "study_description.json")

    data_is_valid = pyfairdatatools.validate.validate_study_description(
        data=study_metadata
    )

    if not data_is_valid:
        raise Exception("Study description is not valid")

    pyfairdatatools.generate.generate_study_description(
        data=study_metadata, file_path=temp_file_path, file_type="json"
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
        blob=f"{metadata_folder}/study_description.json",
    )

    with open(temp_file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

    return
