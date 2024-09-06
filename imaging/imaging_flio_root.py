from imaging.imaging_standards import DataDomain
import imaging.imaging_flio_converter as flio_conv
import imaging.imaging_flio_organize as flio_organize
import imaging.imaging_flio_metadata as flio_meta
import imaging.imaging_utils as imaging_utils

# import imaging.flio_retinal_photography_metadata as flio_meta


class Flio(DataDomain):
    """
    Handles FLIO (Fluorescence Lifetime Imaging Ophthalmoscopy) data processing,
    including organizing DICOM files by protocol and converting them to NEMA-compliant formats.
    """

    def __init__(self):
        super().__init__()
        self.ver = "1.0"

    def organize(self, folder, output_folder):
        """
        Organizes DICOM files by their protocol into the specified output folder and returns metadata as a dictionary.

        Args:
            folder (str): Full path to the folder containing DICOM files.
            output_folder (str): Full path to the output folder where organized files will be saved.

        Returns:
            dict: Metadata information extracted during the organization process.
        """
        organize_dict = flio_organize.filter_flio_files_process(folder, output_folder)
        return organize_dict

    def convert1(self, input_folder, output, jsonpath):
        """
        Converts FLIO .dcm files to NEMA-compliant DICOM files.

        Args:
            input_folder (str): Full path to the folder containing DICOM .dcm files.
            output (str): Path to the final location for the converted DICOM files.
            jsonpath (str): Path to the JSON file containing conversion configurations.

        Returns:
            dict: Information on conversion issues and output files.
        """
        sdt, html = imaging_utils.find_html_sdt_files(input_folder)
        conv_dict = flio_conv.make_flio_dicom(sdt, html, output, jsonpath)

        return conv_dict

    def convert2(self, input_dicom, output):
        """
        Converts a single DICOM file to a NEMA-compliant format.

        Args:
            input_dicom (str): Full path to the input DICOM file.
            output (str): Path to the output location for the converted file.

        Returns:
            dict: Information on conversion issues and output files.
        """
        conv_dict = flio_conv.convert_dicom(input_dicom, output)

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

        meta_dict = flio_meta.meta_data_save(input_file, output_folder)

        return meta_dict
