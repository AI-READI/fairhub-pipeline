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


class ECGManifest:
    def __init__(self):
        self.manifest = []

    def add_metadata(self, entry, wfdb_hea_filepath, wfdb_dat_filepath):

        entry["wfdb_hea_filepath"] = wfdb_hea_filepath
        entry["wfdb_dat_filepath"] = wfdb_dat_filepath

        self.manifest.append(entry)

    def write_tsv(
        self,
        file_path: str,
    ):
        # Sort the manifest by participant_id
        self.manifest = sorted(self.manifest, key=lambda x: x["participant_id"])

        # Write the data to a TSV file
        with open(file_path, "w") as f:
            f.write(
                "participant_id\tmodality\twfdb_hea_filepath\twfdb_dat_filepath\tmachine_text\tmachine_detail_description\tdevice_documentation_type_and_version\tinterpretation_criteriaversion\tpatient_criteriaversion\tinternalmeasurements_version\tparticipant_position\tRate\tPR\tQRSD\tQT\tQTc\tP\tQRS\tT\treport_description\tmanufacturer\tmanufacturers_model_name\n"
            )

            for entry in self.manifest:
                f.write(
                    f"{entry['participant_id']}\t{entry['modality']}\t{entry['wfdb_hea_filepath']}\t{entry['wfdb_dat_filepath']}\t{entry['machine_text']}\t{entry['machine_detail_description']}\t{entry['device_documentation_type_and_version']}\t{entry['interpretation_criteriaversion']}\t{entry['patient_criteriaversion']}\t{entry['internalmeasurements_version']}\t{entry['participant_position']}\t{entry['Rate']}\t{entry['PR']}\t{entry['QRSD']}\t{entry['QT']}\t{entry['QTc']}\t{entry['P']}\t{entry['QRS']}\t{entry['T']}\t{entry['report_description']}\t{entry['manufacturer']}\t{entry['device_model']}\n"
                )
