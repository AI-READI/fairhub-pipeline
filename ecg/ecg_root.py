import logging
from pathlib import Path

from ecg.ecg_standards import DataDomain
import ecg.ecg_converter as ecg_conv
import ecg.ecg_metadata as ecg_meta
import ecg.ecg_dataplot as ecg_plot
import ecg.ecg_utils as ecg_utils


# Create logger with 'ecg'
logger = logging.getLogger("ecg")
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.NullHandler())


class ECG(DataDomain):
    """
    Expects a *.xml file from Philips Pagewriter TC30
        The filename is not parsed, but the contents must comply with AI-READI standards, including
        - a user defined field called 'Position' or 'POSITION'
        - the test subject ID in the name fields
    """

    def __init__(self):
        super().__init__()
        self.ver = "1.0"
        # Legacy compatibility from ecg_260718/ecg.py.
        self.position_name = ["POSITION", "Position"]
        logger.info(f"ECG tool init ver {self.ver}")

    def convert(self, input_path, temp_csv_folder, output_wfdb_folder):
        """Reads the .xml ECG file and converts to *.wfdb with *.hea annotation sidecar file.
        Args:
            input_path (string): full path to ECG *.xml file
            temp_csv_folder (string): path to a temporary working folder
            output_wfdb_folder (string): path to the final location for the *.wfdb and *.hea files
        Returns:
            conv_dict (dict): information on issues and output files, e.g.
                {'participant_id': '9999',
                 'conversion_success': True,  # boolean
                 'conversion_issues': [],  # list or dict of issues TBD
                 'output_files': [destination_hea, destination_dat],
                 'output_hea_file': '/folder_name/9999_ecg_32bcfd77.hea',
                 'output_dat_file': '/folder_name/9999_ecg_32bcfd77.dat'
                }
                Note that the *.dat file will have the same base name as the *.hea file
        """
        logger.info(f"ECG conversion starting for {input_path}")
        # pID, destination_hea = ecg_conv.convert_ecg(input_path, temp_csv_folder,
        conv_dict = ecg_conv.convert_ecg(
            input_path, temp_csv_folder, output_wfdb_folder
        )
        pID = conv_dict["participant_id"]  # ToDo: sort out participantID and particpant_id
        destination_hea = conv_dict["output_hea_file"]
        logger.info(f"ECG {pID} is associated with {input_path}")
        logger.info(
            f"ECG {pID} conversion is complete; *.hea exported to {destination_hea}"
        )

        return conv_dict

    def metadata(
        self,
        input_file,
        output_file=None,
        extended_meta=False,
        all_data=False,
        verbose=False,
    ):
        """Extract metadata from either *.hea (current flow) or *.xml (legacy flow).

        Args:
            input_file (string): path to *.hea or *.xml input
            output_file (string|None): optional output file used in xml legacy mode
            extended_meta (boolean): used for *.hea extraction
            all_data (boolean): used for *.xml extraction; False returns a short subset
            verbose (boolean): emit additional logs
        Returns:
            dictionary
        """
        suffix = Path(input_file).suffix.lower()
        if suffix == ".hea":
            logger.info(f"ECG metadata extraction started for {input_file}")
            meta_dict = ecg_meta.extract_metadata(input_file, extended_meta=extended_meta)
            logger.info(f"ECG metadata extraction completed for {input_file}")
            return meta_dict

        if suffix == ".xml":
            if verbose:
                logger.info(f"ECG xml metadata extraction started for {input_file}")

            mdict = ecg_utils.extract_flattened_xml_metadata(
                input_file,
                remove_waveforms=True,
                verbosity=1 if verbose else 0,
            )

            short_metadata_list = [
                "documentinfo_documentname",
                "userdefines_userdefine_1_value",
                "dataacquisition_machine_@detaildescription",
                "patient_generalpatientdata_patientid",
                "patient_generalpatientdata_name_lastname",
                "patient_generalpatientdata_name_firstname",
                "patient_generalpatientdata_age_dateofbirth",
            ]

            if all_data:
                out_dict = mdict
            else:
                out_dict = {k: mdict[k] for k in short_metadata_list if k in mdict}

            if output_file is not None:
                logger.info(f"Writing xml metadata output to {output_file}")
                with open(output_file, "w") as fout:
                    for key, val in mdict.items():
                        # The standard format is <sex>Male</sex>, but the code contains additional precautions
                        key_lower = str(key).lower()
                        is_gender_val = isinstance(val, str) and val.lower() in ["male", "female"]
                        if is_gender_val or "sex" in key_lower or "gender" in key_lower:
                            fout.write(f"{key}: redacted\n")
                        else:
                            fout.write(f"{key}: {val}\n")

            if verbose:
                logger.info(f"ECG xml metadata extraction completed for {input_file}")

            return out_dict

        raise ValueError(
            f"Unsupported metadata input extension for {input_file}. Expected .hea or .xml"
        )

    def dataplot(self, conv_dict, output_folder):
        """Reads the converted data and outputs a waveform plot for visual quality checks.
        Args:
            conv_dict (dict): must contain at least 3 valid elements:
                participant_id
                output_hea_file
                output_dat_file
            output_folder (string): full path to a folder for the saved plot
        Returns:
            fig_path (string): full path to the saved plot
        """
        logger.info(f"ECG dataplot working on {conv_dict['participant_id']}")
        dataplot_dict = ecg_plot.make_dataplot(conv_dict, output_folder)

        return dataplot_dict
