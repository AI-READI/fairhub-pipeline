from imaging.imaging_standards import DataDomain
import imaging.imaging_cirrus_converter as cirrus_conv
import imaging.imaging_cirrus_organize as cirrus_organize
import imaging.imaging_cirrus_metadata as cirrus_meta


# import imaging.maestro2_triton_metadata as maestro2_triton_meta


class Cirrus(DataDomain):
    """
    A class to handle the organization and conversion of Maestro2 Triton DICOM files.

    This class inherits from the DataDomain base class and provides methods to
    organize DICOM files based on their protocol and convert them to NEMA compliant
    *.dcm files.

    Attributes:
        ver (str): Version of the Maestro2_Triton class.
    """

    def __init__(self):
        """
        Initializes the Maestro2_Triton class with a version attribute.
        """
        super().__init__()
        self.ver = "1.0"

    def organize(self, dicom_file, output_folder):
        """
        Organizes a DICOM file by its protocol in the specified output folder.

        Args:
            dicom_file (str): Full path to the *.dicom file.
            output_folder (str): Full path to the output folder.

        Returns:
            dict: A dictionary containing metadata and organization details.
        """
        organize_dict = cirrus_organize.filter_cirrus_files(dicom_file, output_folder)
        return organize_dict

    def convert(self, input_folder, output_folder):
        """
        Converts DICOM files to NEMA compliant *.dcm files.

        Args:
            input_folder (str): Full path to the folder containing the input DICOM *.dcm files.
            output_folder (str): Full path to the folder for the output NEMA compliant *.dcm files.

        Returns:
            dict: A dictionary containing information on issues and output files.
        """
        conv_dict = cirrus_conv.convert_dicom(input_folder, output_folder)

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
        meta_dict = cirrus_meta.meta_data_save(input_file, output_folder)

        return meta_dict
