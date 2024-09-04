import os
import spectralis_onh_oct_converter
import spectralis_onh_retinal_photography_converter
import spectralis_ppol_oct_converter
import spectralis_ppol_retinal_photography_converter


def convert_dicom(input, outputfolder):
    """
    Converts a DICOM file to a specific format based on its type and saves the converted file to an output folder.

    This function determines the type of DICOM file from its filename and uses the appropriate converter module to process the file.
    The converted file is then saved to the specified output folder. If the output folder does not exist, it is created.

    Args:
        input (str): The full path to the input DICOM file that needs to be converted.
        outputfolder (str): The full path to the folder where the converted DICOM file will be saved.

    Returns:
        dict: A dictionary containing information about the conversion process, including details about the output file and any errors encountered.

    """

    if not os.path.exists(outputfolder):
        os.makedirs(outputfolder)

    if "spectralis_onh_rc_hr_retinal_photography_" in input:
        dic = spectralis_onh_retinal_photography_converter.convert_dicom(
            input, outputfolder
        )

    elif "spectralis_onh_rc_hr_oct_" in input:
        dic = spectralis_onh_oct_converter.convert_dicom(input, outputfolder)

    elif "spectralis_ppol_mac_hr_oct_" in input:
        dic = spectralis_ppol_oct_converter.convert_dicom(input, outputfolder)

    elif "spectralis_ppol_mac_hr_retinal_photography_" in input:
        dic = spectralis_ppol_retinal_photography_converter.convert_dicom(
            input, outputfolder
        )

    return dic