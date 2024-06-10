import logging

from ecg.ecg_standards import DataDomain
import ecg.ecg_converter as ecg_conv
import ecg.ecg_metadata as ecg_meta
import ecg.ecg_dataplot as ecg_plot


# Create logger with 'ecg'
logger = logging.getLogger("ecg")
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
# reminder: DEBUG, INFO, WARN, ERROR, CRITICAl
fh = logging.FileHandler("ecg.log", mode="w")
fh.setLevel(logging.DEBUG)
# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
# create formatter and add it to the handlers
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
# reminder: time format will be YYYY-mm-dd hh:mm:ss,uuu where uuu are microseconds
fh.setFormatter(formatter)
ch.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)  # writes to file
logger.addHandler(ch)  # writes to console (screen)


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
        logger.info(f"ECG tool init ver {self.ver}")

    def convert(self, input_path, temp_csv_folder, output_wfdb_folder):
        """Reads the .xml ECG file and converts to *.wfdb with *.hea annotation sidecar file.
        Args:
            input_path (string): full path to ECG *.xml file
            temp_csv_folder (string): path to a temporary working folder
            output_wfdb_folder (string): path to the final location for the *.wfdb and *.hea files
        Returns:
            conv_dict (dict): information on issues and output files, e.g.
                {'participantID': '9999',
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
        pID = conv_dict["participantID"]
        destination_hea = conv_dict["output_hea_file"]
        logger.info(f"ECG {pID} is associated with {input_path}")
        logger.info(
            f"ECG {pID} conversion is complete; *.hea exported to {destination_hea}"
        )

        return conv_dict

    def metadata(self, hea_file, extended_meta=False):
        """Reads the .hea annotation sidecar file and returns the meta data as a dict.
        Args:
            hea_file (string): full path to the *.hea file
        Returns:
            dictionary
        """
        logger.info(f"ECG metadata extraction started for {hea_file}")
        meta_dict = ecg_meta.extract_metadata(hea_file, extended_meta=extended_meta)
        logger.info(f"ECG metadata extraction completed for {hea_file}")
        return meta_dict

    def dataplot(self, conv_dict, output_folder):
        """Reads the converted data and outputs a waveform plot for visual quality checks.
        Args:
            conv_dict (dict): must contain at least 3 valid elements:
                participantID
                output_hea_file
                output_dat_file
            output_folder (string): full path to a folder for the saved plot
        Returns:
            fig_path (string): full path to the saved plot
        """
        logger.info(f'ECG dataplot working on {conv_dict["participantID"]}')
        dataplot_dict = ecg_plot.make_dataplot(conv_dict, output_folder)

        return dataplot_dict
