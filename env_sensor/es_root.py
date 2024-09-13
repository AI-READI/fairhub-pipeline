import logging

from .standards import DataDomain

from . import es_converter as es_conv
from . import es_metadata as es_meta
from . import es_utils_plot as es_plot

# Notes on amount of data:
#  example: 2023-06-17 05:19:00 to 2023-06-18 19:32:24 = 38 hours --> 3.8MB
#       1.5 hours of data is 145 kB
#  Estimated size for 10 days is (10*24/38)*3.8MB = 24MB
#  Estimated # of lines of data for 10 days is 172,800


# Create logger with 'es'
logger = logging.getLogger("es")
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
# reminder: DEBUG, INFO, WARN, ERROR, CRITICAl
fh = logging.FileHandler("es.log", mode="w")
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
        expect a self-documenting CSV-like file
        return a dictionary of metadata
        # TODO: update description of metadata() after code is final #30

    dataplot() will
        expect a dictionary of metadate including at least participantID and output_file
        expect an output_folder for the created plot
        return # ToDo: update return value
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
        extended_conv_dict=False,
    ):
        """
        Read one folder with a sequence of environmental sensor *.csv files and combine them, then
        add a self-documenting header section.
        If a *.zip is passed, it will be unzipped to access the folder. # ToDo: may not be implemented
        Timestamp issues will be noted and repaired if possible.

        Args:
            input_path (string): Full path to a folder of EnvironmentalSensor *.csv files
            output_folder (string): Full path to output folder
            visit_file (string): Full path to a csv file with visit data
            filter_level (int): (Optional) Controls data filters.
                0 = remove only corrupted data rows
                1 = + remove short files with less than 2 minutes of data
                2 = + remove files outside of the observation window visit_date to return_date
            build_file (string): (Optional) Full path to a csv file with esID to SEN55 mapping
            extended_conv_dict (boolean): (Optional) Return an extensive conv_dict suitable for debug

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
            extended_conv_dict=extended_conv_dict,
        )

        logger.info(f'ES conversion is complete; output is {conv_dict["output_file"]}')
        return conv_dict

    def metadata(self, input_csv):
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
        meta_dict = es_meta.metadata_env_sensor(input_csv)
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
