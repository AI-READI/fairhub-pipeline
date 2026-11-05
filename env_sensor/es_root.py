"""High-level environmental sensor API for conversion, metadata extraction, and plotting."""

import logging
import sys
from pathlib import Path

try:
    from env_sensor.es_standards import DataDomain
    from env_sensor import es_converter as es_conv
    from env_sensor import es_metadata as es_meta
    from env_sensor import es_utils_plot as es_plot
except ImportError:
    try:
        from .es_standards import DataDomain
        from . import es_converter as es_conv
        from . import es_metadata as es_meta
        from . import es_utils_plot as es_plot
    except ImportError:
        project_root = Path(__file__).resolve().parent.parent
        if str(project_root) not in sys.path:
            sys.path.insert(0, str(project_root))
        from env_sensor.es_standards import DataDomain
        from env_sensor import es_converter as es_conv
        from env_sensor import es_metadata as es_meta
        from env_sensor import es_utils_plot as es_plot

logger = logging.getLogger("env_sensor")
logger.addHandler(logging.NullHandler())


class EnvironmentalSensor(DataDomain):
    """
    Works with *.csv files output from the environmental sensor

    ENV-pppp-nnn is the file naming convention where
        pppp = the participant ID
        nnn = the environmental sensor assembly ID

    convert() will
        expect the full path to a folder or folder.zip file of the form
            ENV-pppp-nnn or ENV-pppp-nnn.zip containing *.csv raw data files
        combine the raw files with an ESDS style header to create a self-documenting CSV-like file
        return a dictionary of information

    metadata() will
        expect a self-documenting CSV-like file and return a dictionary of metadata.

    dataplot() will
        expect a dictionary containing at least participantID and output_file,
        along with an output folder for the generated plot.
    """

    def __init__(self):
        super().__init__()
        logger.info("EnvironmentalSensor init v1.0")

        # Env sensor file name formats are ENV-pppp-nnn
        self.splitchar_envppppnnn = "-"
        # Self-documenting header import from file
        self.header_content = ""

    def convert(
        self,
        input_path,
        output_folder,
        visit_file,
        filter_level=2,
        build_file=None,
        doc_header_file=None,
        extended_conv_dict=False,
        simple_csv=False,
        es_log=None,
        verbose=False,
    ):
        """
        Read one folder with a sequence of environmental sensor *.csv files and combine them,
        then add a self-documenting header section.

        If a *.zip is passed, it will be extracted to access the folder contents before processing.
        Timestamp issues are identified and corrected when possible.

        Args:
            input_path (string): Full path to a folder of EnvironmentalSensor *.csv files
            output_folder (string): Full path to output folder
            visit_file (string): Full path to a csv file with visit data
            filter_level (int): (Optional) Controls data filters.
                0 = remove only corrupted data rows
                1 = + remove short files with less than 2 minutes of data
                2 = + remove files outside of the observation window visit_date to return_date
            build_file (string): (Optional) Full path to a csv file with esID to SEN55 mapping
            doc_header_file (string): (Optional) Full path to a self-documenting header template
            extended_conv_dict (boolean): (Optional) Return an extensive conv_dict suitable for debug
            simple_csv (boolean): (Optional) Accepted for compatibility with newer batch scripts
            es_log (string): (Optional) Accepted for compatibility with newer batch scripts
            verbose (boolean): (Optional) Accepted for compatibility with newer batch scripts

        Returns: dict including the following keys
            participantID (4 digit string): participant ID e.g. 9998
            output_file (string) : path to the file with the selfdocumenting header
            conversion_success (boolean): whether or not the final conversion was completed
            conversion_issues (list of strings): issues that interfered with conversion
        """
        logger.info(f"ES conversion starting for {input_path}")
        conv_dict = es_conv.convert_env_sensor(
            input_path,
            output_folder,
            visit_file=visit_file,
            filter_level=filter_level,
            build_file=build_file,
            doc_header_file=doc_header_file,
            extended_conv_dict=extended_conv_dict,
        )

        logger.info(f'ES conversion is complete; output is {conv_dict["output_file"]}')
        return conv_dict

    def metadata(self, input_csv, outfile=None, es_logfile=None, verbose=False):
        """Read final environmental sensor *.csv file and return a meta_dict.

        Args:
            input_csv (string): full path to EnvironmentalSensor self-documenting *.csv

        Returns:
            meta_dict (dict): metadata including key, values for
                modality, manufacturer, device, particpant_id, sesnor_id,
                sensor_location, number_of_observations, sensor_sampling_extent_in_days
        """
        # decision impacts whether we can check that the file is formatted correctly
        logger.info(f"ES metadata extraction started for {input_csv}")
        meta_dict = es_meta.metadata_env_sensor(
            input_csv,
            outfile=outfile,
            es_logfile=es_logfile,
            verbose=verbose,
        )
        logger.info(f"ES metadata extraction completed for {input_csv}")
        return meta_dict

    def dataplot(self, conv_dict, output_folder):
        """Reads the converted data and outputs a waveform plot for visual quality checks.
        Args:
            conv_dict (dict): must contain at least 2 valid elements:
                participantID
                output_file
            output_folder (string): full path to a folder for the saved plot
        Returns: dict with one key, value
            output_file (string): full path to the saved *.png plot
        """
        # logger.info(f'ES dataplot working on {conv_dict["participantID"]}')
        logger.info(f'ES dataplot working on {conv_dict["participantID"]}')
        dataplot_dict = es_plot.dataplot(conv_dict, output_folder)

        return dataplot_dict
