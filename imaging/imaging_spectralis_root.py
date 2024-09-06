from imaging.imaging_standards import DataDomain
import imaging.imaging_spectralis_converter as spectralis_conv
import imaging.imaging_spectralis_organize as spectralis_organize
import imaging.imaging_spectralis_metadata as spectralis_meta


class Spectralis(DataDomain):
    """
    A class to handle DICOM files from Spectralis devices, supporting organization, conversion, and metadata extraction.

    This class provides methods to organize Spectralis DICOM files by protocol, convert them to NEMA compliant formats,
    and extract metadata, saving it as a JSON file.
    """

    def __init__(self):
        super().__init__()
        self.ver = "1.0"

    def organize(self, dicom_file, output_folder):
        """
        Reads a DICOM file, organizes it by protocol in the output folder, and returns metadata as a dictionary.

        Args:
            dicom_file (str): Full path to the *.dicom file.
            output_folder (str): Full path to the output folder where organized files will be saved.

        Returns:
            dict: A dictionary containing metadata and information about the organized file.
        """
        organize_dict = spectralis_organize.filter_spectralis_files(
            dicom_file, output_folder
        )
        return organize_dict

    def convert(self, input_dicom_file, output):
        """
        Converts a DICOM file to a NEMA compliant *.dcm file.

        Args:
            input_dicom_file (str): Full path to the input DICOM *.dcm file.
            output (str): Path to the final location for the NEMA compliant *.dcm file.

        Returns:
            dict: A dictionary containing information on conversion issues and output files.
        """
        conv_dict = spectralis_conv.convert_dicom(input_dicom_file, output)

        return conv_dict

    def metadata(self, input_file, output_folder):
        """
        Extracts metadata from the input file and saves it as a JSON file in the output folder.

        Args:
            input_file (str): Full path to the DICOM *.dcm file.
            output_folder (str): Full path to the folder where the output metadata JSON file will be saved.

        Returns:
            dict: A dictionary containing extracted metadata.
        """
        meta_dict = spectralis_meta.meta_data_save(input_file, output_folder)

        return meta_dict
