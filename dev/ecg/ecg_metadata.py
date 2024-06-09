import logging

meta_logger = logging.getLogger("ecg.metadata")


def extract_metadata(hea_file, extended_meta=False):
    """Extracts the metadata from the *.hea sidecar file and returns a dictionary.
    Args:
        hea_file (string): path to *.hea sidecar file
        extended_meta (boolean): if True, return all metadata; if False, return key metadata
    Returns:
        dictionary of meta data
    """

    # items for the manifest
    meta_manifest_list = [
        # static and device info
        "modality",
        "manufacturer",
        "device_model",  # static hardcoded
        "machine_text",
        "machine_detail_description",  # Philips Medical Products:860306:A.07.07.07
        #  more recently modified to be Philips Medical Products 860306 A.07.07.07
        "interpretation_criteriaversion",  # 0B or 0C
        "patient_criteriaversion",  # 0B or 0C
        "internalmeasurements_version",  # 10 or 11
        "report_description",
        "device_documentation_type_and_version",
        # participant
        "participant_id",
        # 'participant_year_of_birth',  # remove to avoid conflict w/ OMOP
        "participant_position",
        "Rate",
        "PR",
        "QRSD",
        "QT",
        "QTc",
        "P",
        "QRS",
        "T",
    ]

    hea_dict = dict()  # fetch everything for dev and debug
    meta_dict = dict()  # fetch limited set for the manifest

    with open(hea_file, "r") as f:
        for myline in f:
            if myline[0] == "#":
                myline_parts = myline[1:].strip().split(":")
                if len(myline_parts) != 2:  # expected for machine_detail_description
                    keyword = myline_parts[0]
                    val = (" ").join(myline_parts[1:]).strip()
                else:
                    keyword, val = myline[1:].strip().split(":")

                hea_dict[keyword] = val.strip()  # everything goes into hea_dict
                meta_logger.debug(f"keyword: {keyword} and val: {val}")

                if keyword in meta_manifest_list:
                    meta_dict[keyword] = val.strip()  # selected items go into meta_dict
            else:
                meta_logger.debug(f"Not starting with #: {myline}")

    if extended_meta is False:
        return meta_dict
    else:
        return hea_dict
