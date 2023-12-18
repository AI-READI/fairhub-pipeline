# pylint: disable=line-too-long
"""Process environmental sensor data files"""

import datetime
import pathlib
import tempfile

import azure.storage.blob as azureblob
import psycopg2
import pyfairdatatools

import config
import json


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

    # Get the study identification metadata
    cur.execute(
        "SELECT identifier, identifier_type, identifier_domain, identifier_link FROM study_identification WHERE study_id = %s AND secondary = false",
        (study_id,),
    )

    primary_study_identification = cur.fetchone()

    identification_module["OrgStudyIdInfo"] = {}

    # Study Identifier
    identification_module["OrgStudyIdInfo"][
        "OrgStudyId"
    ] = primary_study_identification[0]
    # Study Identifier Type
    identification_module["OrgStudyIdInfo"][
        "OrgStudyIdType"
    ] = primary_study_identification[1]

    if primary_study_identification[2] and primary_study_identification[2] != "":
        # Study Identifier Domain
        identification_module["OrgStudyIdInfo"][
            "OrgStudyIdDomain"
        ] = primary_study_identification[2]

    if primary_study_identification[3] and primary_study_identification[3] != "":
        # Study Identifier Link
        identification_module["OrgStudyIdInfo"][
            "OrgStudyIdLink"
        ] = primary_study_identification[3]

    # Get the secondary study identification metadata
    cur.execute(
        "SELECT identifier, identifier_type, identifier_domain, identifier_link FROM study_identification WHERE study_id = %s AND secondary = true",
        (study_id,),
    )

    secondary_study_identification = cur.fetchall()

    identification_module["SecondaryIdInfoList"] = []

    for row in secondary_study_identification:
        item = {}
        # Study Identifier and Study Identifier Type
        item["SecondaryId"] = row[0]
        item["SecondaryIdType"] = row[1]

        if row[2]:
            # Study Identifer Domain
            item["SecondaryIdDomain"] = row[2]

        if row[3]:
            # Study Identifier Link
            item["SecondaryIdLink"] = row[3]

        identification_module["SecondaryIdInfoList"].append(item)

    study_metadata["IdentificationModule"] = identification_module

    status_module = {}

    # Get the study status metadata
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

    # Get the study sponsor and collaborators metadata
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
        # Add the collabarator(s) to the list
        item = {"CollaboratorName": row}

        sponsor_collaborators_module["CollaboratorList"].append(item)

    study_metadata["SponsorCollaboratorsModule"] = sponsor_collaborators_module

    oversight_module = {}

    # Get the study oversight metadata
    cur.execute(
        "SELECT oversight_has_dmc FROM study_other WHERE study_id = %s",
        (study_id,),
    )

    study_oversight = cur.fetchone()

    if study_oversight[0]:
        oversight_module["OversightHasDMC"] = "Yes"
    else:
        oversight_module["OversightHasDMC"] = "No"

    study_metadata["OversightModule"] = oversight_module

    description_module = {}

    # Get the study description metadata
    cur.execute(
        "SELECT brief_summary, detailed_description FROM study_description WHERE study_id = %s",
        (study_id,),
    )

    study_description = cur.fetchone()

    description_module["BriefSummary"] = study_description[0]
    if study_description[1] and study_description[1] != "":
        description_module["DetailedDescription"] = study_description[1]

    study_metadata["DescriptionModule"] = description_module

    conditions_module = {}

    # Get the study conditions metadata
    cur.execute(
        "SELECT conditions, keywords FROM study_other WHERE study_id = %s",
        (study_id,),
    )

    study_conditions = cur.fetchone()

    conditions_module["ConditionList"] = []
    conditions = study_conditions[0]

    for row in conditions:
        conditions_module["ConditionList"].append(row)

    # todo: add keywords from the UI and API
    conditions_module["KeywordList"] = ["Dataset"]
    keywords = study_conditions[1]
    for row in keywords:
        conditions_module["KeywordList"].append(row)

    study_metadata["ConditionsModule"] = conditions_module

    design_module = {}

    # Get the study design metadata
    cur.execute(
        "SELECT study_type, design_allocation, design_intervention_model, design_intervention_model_description, design_primary_purpose, design_masking, design_masking_description, design_who_masked_list, phase_list, enrollment_count, enrollment_type, number_arms,design_observational_model_list, design_time_perspective_list, bio_spec_retention, bio_spec_description, target_duration, number_groups_cohorts FROM study_design WHERE study_id = %s",
        (study_id,),
    )

    study_design = cur.fetchone()

    study_type = study_design[0]
    design_module["StudyType"] = study_type

    if study_type == "Interventional":
        design_module["DesignInfo"] = {}
        design_module["DesignInfo"]["DesignAllocation"] = study_design[1]
        design_module["DesignInfo"]["DesignInterventionModel"] = study_design[2]
        if study_design[3] and study_design[3] != "":
            design_module["DesignInfo"][
                "DesignInterventionModelDescription"
            ] = study_design[3]
        design_module["DesignInfo"]["DesignPrimaryPurpose"] = study_design[4]

        design_module["DesignInfo"]["DesignMaskingInfo"] = {}
        design_module["DesignInfo"]["DesignMaskingInfo"][
            "DesignMasking"
        ] = study_design[5]
        design_module["DesignInfo"]["DesignMaskingInfo"][
            "DesignMaskingDescription"
        ] = study_design[6]

        design_module["DesignInfo"]["DesignMaskingInfo"]["DesignWhoMaskedList"] = []

        if study_design[7] is not None:
            for row in study_design[7]:
                design_module["DesignInfo"]["DesignMaskingInfo"][
                    "DesignWhoMaskedList"
                ].append(row)

        design_module["PhaseList"] = []

        if study_design[8] is not None:
            for row in study_design[8]:
                design_module["PhaseList"].append(row)

    design_module["EnrollmentInfo"] = {}
    design_module["EnrollmentInfo"]["EnrollmentCount"] = str(study_design[9])
    design_module["EnrollmentInfo"]["EnrollmentType"] = study_design[10]

    if study_type == "Interventional":
        design_module["NumberArms"] = str(study_design[11])

    if study_type == "Observational":
        design_module["DesignInfo"] = {}
        design_module["DesignInfo"]["DesignObservationalModelList"] = []
        if study_design[12] is not None:
            for row in study_design[12]:
                design_module["DesignInfo"]["DesignObservationalModelList"].append(row)

        design_module["DesignInfo"]["DesignTimePerspectiveList"] = []

        if study_design[13] is not None:
            for row in study_design[13]:
                design_module["DesignInfo"]["DesignTimePerspectiveList"].append(row)

        design_module["BioSpec"] = {}
        design_module["BioSpec"]["BioSpecRetention"] = study_design[14]
        if study_design[15] is not None and study_design[15] != "":
            design_module["BioSpec"]["BioSpecDescription"] = study_design[15]

        design_module["TargetDuration"] = study_design[16]
        design_module["NumberGroupsCohorts"] = str(study_design[17])

    study_metadata["DesignModule"] = design_module

    arms_interventions_module = {}

    # Get the study arms and interventions metadata
    cur.execute(
        "SELECT label, type, description, intervention_list FROM study_arm WHERE study_id = %s",
        (study_id,),
    )

    study_arms = cur.fetchall()

    arms_interventions_module["ArmGroupList"] = []

    for row in study_arms:
        item = {}

        item["ArmGroupLabel"] = row[0]
        if study_type == "Interventional":
            item["ArmGroupType"] = row[1]

        if row[2] is not None and row[2] != "":
            item["ArmGroupDescription"] = row[2]

        if study_type == "Interventional" and row[3] is not None and len(row[3]) > 0:
            item["ArmGroupInterventionList"] = []

            for intervention in row[3]:
                item["ArmGroupInterventionList"].append(intervention)

        arms_interventions_module["ArmGroupList"].append(item)

    # Get the study interventions metadata
    cur.execute(
        "SELECT type, name, description, arm_group_label_list, other_name_list FROM study_intervention WHERE study_id = %s",
        (study_id,),
    )

    study_interventions = cur.fetchall()

    arms_interventions_module["InterventionList"] = []

    for row in study_interventions:
        item = {}

        item["InterventionType"] = row[0]
        item["InterventionName"] = row[1]
        if row[2] is not None and row[2] != "":
            item["InterventionDescription"] = row[2]

        item["InterventionArmGroupLabelList"] = []

        if row[3] is not None:
            for arm_group_label in row[3]:
                item["InterventionArmGroupLabelList"].append(arm_group_label)

        item["InterventionOtherNameList"] = []

        if row[4] is not None:
            for other_name in row[4]:
                item["InterventionOtherNameList"].append(other_name)

        arms_interventions_module["InterventionList"].append(item)

    study_metadata["ArmsInterventionsModule"] = arms_interventions_module

    eligibility_module = {}

    # Get the study eligibility metadata
    cur.execute(
        "SELECT gender, gender_based, gender_description, minimum_age_value, minimum_age_unit, maximum_age_value, maximum_age_unit, healthy_volunteers, inclusion_criteria, exclusion_criteria, study_population, sampling_method FROM study_eligibility WHERE study_id = %s",
        (study_id,),
    )

    study_eligibility = cur.fetchone()

    eligibility_module["Gender"] = study_eligibility[0]
    eligibility_module["GenderBased"] = study_eligibility[1]
    eligibility_module["GenderDescription"] = study_eligibility[2]
    eligibility_module["MinimumAge"] = f"{study_eligibility[3]} {study_eligibility[4]}"
    eligibility_module["MaximumAge"] = f"{study_eligibility[5]} {study_eligibility[6]}"
    if study_eligibility[7] is not None and study_eligibility[7] != "":
        eligibility_module["HealthyVolunteers"] = study_eligibility[7]
    if study_type == "Observational":
        eligibility_module["StudyPopulation"] = study_eligibility[10]
        eligibility_module["SamplingMethod"] = study_eligibility[11]

    eligibility_criteria = ""

    if study_eligibility[8] is not None:
        eligibility_criteria = "Inclusion Criteria\n"

        for criteria in study_eligibility[8]:
            eligibility_criteria += f"* {criteria}\n"

    if study_eligibility[9] is not None:
        eligibility_criteria += "\nExclusion Criteria\n"

        for criteria in study_eligibility[9]:
            eligibility_criteria += f"* {criteria}\n"

    eligibility_module["EligibilityCriteria"] = eligibility_criteria

    study_metadata["EligibilityModule"] = eligibility_module

    contacts_locations_module = {}

    # Get the study contacts and locations metadata
    cur.execute(
        "SELECT name, affiliation, phone, phone_ext, email_address FROM study_contact WHERE study_id = %s AND central_contact = true",
        (study_id,),
    )

    study_central_contacts = cur.fetchall()

    contacts_locations_module["CentralContactList"] = []

    if study_central_contacts is not None:
        for row in study_central_contacts:
            item = {}

            item["CentralContactName"] = row[0]
            item["CentralContactAffiliation"] = row[1]
            item["CentralContactPhone"] = row[2]
            if row[3] is not None and row[3] != "":
                item["CentralContactPhoneExt"] = row[3]
            item["CentralContactEMail"] = row[4]

            contacts_locations_module["CentralContactList"].append(item)

    # Get the study contacts metadata
    cur.execute(
        "SELECT name, affiliation, role FROM study_overall_official WHERE study_id = %s",
        (study_id,),
    )

    contacts_locations_module["OverallOfficialList"] = []

    study_overall_officials = cur.fetchall()

    if study_overall_officials is not None:
        for row in study_overall_officials:
            item = {}

            item["OverallOfficialName"] = row[0]
            item["OverallOfficialAffiliation"] = row[1]
            item["OverallOfficialRole"] = row[2]

            contacts_locations_module["OverallOfficialList"].append(item)

    # Get the study locations metadata
    cur.execute(
        "SELECT facility, status, city, state, zip, country FROM study_location WHERE study_id = %s",
        (study_id,),
    )

    study_locations = cur.fetchall()

    contacts_locations_module["LocationList"] = []

    if study_locations is not None:
        for row in study_locations:
            item = {}

            item["LocationFacility"] = row[0]
            item["LocationStatus"] = row[1]
            item["LocationCity"] = row[2]
            if row[3] is not None and row[3] != "":
                item["LocationState"] = row[3]
            if row[4] is not None and row[4] != "":
                item["LocationZip"] = row[4]
            item["LocationCountry"] = row[5]

            contacts_locations_module["LocationList"].append(item)

    study_metadata["ContactsLocationsModule"] = contacts_locations_module

    ipd_sharing_statement_module = {}

    # Get the study IPD sharing metadata
    cur.execute(
        "SELECT ipd_sharing, ipd_sharing_description, ipd_sharing_info_type_list, ipd_sharing_time_frame, ipd_sharing_access_criteria, ipd_sharing_url FROM study_ipdsharing WHERE study_id = %s",
        (study_id,),
    )

    ipd_sharing = cur.fetchone()

    bool_ipd_share = ipd_sharing[0]
    ipd_sharing_statement_module["IPDSharing"] = ipd_sharing[0]
    if bool_ipd_share == "No" and ipd_sharing[1] is not None and ipd_sharing[1] != "":
        ipd_sharing_statement_module["IPDSharingDescription"] = ipd_sharing[1]
    if bool_ipd_share == "Yes":
        ipd_sharing_statement_module["IPDSharingDescription"] = ipd_sharing[1]

    ipd_sharing_statement_module["IPDSharingInfoTypeList"] = []
    if ipd_sharing[2] is not None:
        for row in ipd_sharing[2]:
            ipd_sharing_statement_module["IPDSharingInfoTypeList"].append(row)

    if bool_ipd_share == "No" and ipd_sharing_statement_module["IPDSharingInfoTypeList"] == []:
        # Delete key if empty
        del ipd_sharing_statement_module["IPDSharingInfoTypeList"]

    if bool_ipd_share == "No" and ipd_sharing[3] is not None and ipd_sharing[3] != "":
        ipd_sharing_statement_module["IPDSharingTimeFrame"] = ipd_sharing[3]
    if bool_ipd_share == "Yes":
        ipd_sharing_statement_module["IPDSharingTimeFrame"] = ipd_sharing[3]

    if bool_ipd_share == "No" and ipd_sharing[4] is not None and ipd_sharing[4] != "":
        ipd_sharing_statement_module["IPDSharingAccessCriteria"] = ipd_sharing[4]
    if bool_ipd_share == "Yes":
        ipd_sharing_statement_module["IPDSharingAccessCriteria"] = ipd_sharing[4]

    if bool_ipd_share == "No" and ipd_sharing[5] is not None and ipd_sharing[5] != "":
        ipd_sharing_statement_module["IPDSharingURL"] = ipd_sharing[5]
    if bool_ipd_share == "Yes":
        ipd_sharing_statement_module["IPDSharingURL"] = ipd_sharing[5]

    study_metadata["IPDSharingStatementModule"] = ipd_sharing_statement_module

    references_module = {}

    # Get the study references metadata (publications)
    cur.execute(
        "SELECT identifier, type, citation FROM study_reference WHERE study_id = %s",
        (study_id,),
    )

    study_references = cur.fetchall()

    references_module["ReferenceList"] = []

    if study_references is not None:
        for row in study_references:
            item = {}

            if row[0] is not None and row[0] != "":
                item["ReferenceID"] = row[0]
            if row[1] is not None and row[1] != "":
                item["ReferenceType"] = row[1]
            if row[2] is not None and row[2] != "":
                item["ReferenceCitation"] = row[2]

            references_module["ReferenceList"].append(item)

    # Get the study links metadata
    cur.execute(
        "SELECT url, title FROM study_link WHERE study_id = %s",
        (study_id,),
    )

    study_links = cur.fetchall()

    references_module["SeeAlsoLinkList"] = []

    if study_links is not None:
        for row in study_links:
            item = {}

            item["SeeAlsoLinkURL"] = row[0]
            if row[1] is not None and row[1] != "":
                item["SeeAlsoLinkLabel"] = row[1]

            references_module["SeeAlsoLinkList"].append(item)

    # Get the study available IPD
    cur.execute(
        "SELECT identifier, type, url, comment FROM study_available_ipd WHERE study_id = %s",
        (study_id,),
    )

    study_available_ipd = cur.fetchall()

    references_module["AvailIPDList"] = []

    if study_available_ipd is not None:
        for row in study_available_ipd:
            item = {}

            item["AvailIPDId"] = row[0]
            item["AvailIPDType"] = row[1]
            item["AvailIPDURL"] = row[2]
            if row[3]:
                item["AvailIPDComment"] = row[3]

            references_module["AvailIPDList"].append(item)

    study_metadata["ReferencesModule"] = references_module

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
