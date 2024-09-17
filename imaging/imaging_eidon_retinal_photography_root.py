from imaging.imaging_standards import DataDomain
import imaging.imaging_eidon_retinal_photography_converter as eidon_conv
import imaging.imaging_eidon_retinal_photography_organize as eidon_organize
import imaging.imaging_eidon_retinal_photography_metadata as eidon_meta


class Eidon(DataDomain):
    """
    Expects a *.dicom file from Eidon
    """

    def __init__(self):
        super().__init__()
        self.ver = "1.0"

    def organize(self, dicom_file, output_folder):
        """Reads a dicom file, organize the file by their protocol in the output folder, and returns the meta data as a dict.
        Args:
            dicom_file (string): full path to the *.dicom file
            output_folder (string): full path to the output folder

        Returns:
            dictionary
        """
        organize_dict = eidon_organize.filter_eidon_files(dicom_file, output_folder)
        return organize_dict

    def convert(self, input_dicom_file, output_dicom_file):
        """Reads the .dcm file and converts to a NEMA compliant *.dcm file.
        Args:
            input_path (string): full path to DICOM *.dcm file
            output_path (string): path to the final location for the *.dcm file
        Returns:
            conv_dict (dict): information on issues and output files
        """

        conv_dict = eidon_conv.convert_dicom(input_dicom_file, output_dicom_file)

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
        meta_dict = eidon_meta.meta_data_save(input_file, output_folder)

        return meta_dict
