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

    oversight_module = {}

    cur.execute(
        "SELECT oversight_has_dmc FROM study_other WHERE study_id = %s",
        (study_id,),
    )

    study_oversight = cur.fetchone()

    oversight_module["OversightHasDMC"] = study_oversight[0]

    study_metadata["OversightModule"] = oversight_module

    description_module = {}

    cur.execute(
        "SELECT brief_summary, detailed_description FROM study_description WHERE study_id = %s",
        (study_id,),
    )

    study_description = cur.fetchone()

    description_module["BriefSummary"] = study_description[0]
    description_module["DetailedDescription"] = study_description[1]

    study_metadata["DescriptionModule"] = description_module

    conditions_module = {}

    cur.execute(
        "SELECT conditions, keywords FROM study_other WHERE study_id = %s",
        (study_id,),
    )

    study_conditions = cur.fetchone()

    conditions_module["ConditionList"] = []
    conditions = study_conditions[0]

    for row in conditions:
        conditions_module["ConditionList"].append(row)

    conditions_module["KeywordList"] = []
    keywords = study_conditions[1]

    for row in keywords:
        conditions_module["KeywordList"].append(row)

    study_metadata["ConditionsModule"] = conditions_module

    design_module = {}

    cur.execute(
        "SELECT study_type, design_allocation, design_intervention_model, design_intervention_model_description, design_primary_purpose, design_masking, design_masking_description, design_who_masked_list, phase_list, enrollment_count, enrollment_type, number_arms,design_observational_model_list, design_time_perspective_list, bio_spec_retention, bio_spec_description, target_duration, number_groups_cohorts FROM study_design WHERE study_id = %s",
        (study_id,),
    )

    study_design = cur.fetchone()

    design_module["StudyType"] = study_design[0]

    design_module["DesignInfo"] = {}
    design_module["DesignInfo"]["DesignAllocation"] = study_design[1]
    design_module["DesignInfo"]["DesignInterventionModel"] = study_design[2]
    design_module["DesignInfo"]["DesignInterventionModelDescription"] = study_design[3]
    design_module["DesignInfo"]["DesignPrimaryPurpose"] = study_design[4]

    design_module["DesignInfo"]["DesignMaskingInfo"] = {}
    design_module["DesignInfo"]["DesignMaskingInfo"]["DesignMasking"] = study_design[5]
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
    design_module["EnrollmentInfo"]["EnrollmentCount"] = study_design[9]
    design_module["EnrollmentInfo"]["EnrollmentType"] = study_design[10]

    design_module["NumberArms"] = study_design[11]

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
    design_module["BioSpec"]["BioSpecDescription"] = study_design[15]

    design_module["TargetDuration"] = study_design[16]
    design_module["NumberGroupsCohorts"] = study_design[17]

    study_metadata["DesignModule"] = design_module

    arms_interventions_module = {}

    cur.execute(
        "SELECT label, type, description, intervention_list FROM study_arm WHERE study_id = %s",
        (study_id,),
    )

    study_arms = cur.fetchall()

    arms_interventions_module["ArmGroupList"] = []

    for row in study_arms:
        item = {}

        item["ArmGroupLabel"] = row[0]
        item["ArmGroupType"] = row[1]
        item["ArmGroupDescription"] = row[2]

        item["ArmGroupInterventionList"] = []

        if row[3] is not None:
            for intervention in row[3]:
                item["ArmGroupInterventionList"].append(intervention)

        arms_interventions_module["ArmGroupList"].append(item)

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
    eligibility_module["HealthyVolunteers"] = study_eligibility[7]

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
            item["CentralContactPhoneExt"] = row[3]
            item["CentralContactEMail"] = row[4]

            contacts_locations_module["CentralContactList"].append(item)

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
            item["LocationState"] = row[3]
            item["LocationZip"] = row[4]
            item["LocationCountry"] = row[5]
            item["LocationContactList"] = []

            contacts_locations_module["LocationList"].append(item)

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
