import os
import maestro2_triton.maestro2_triton_enface_converter as maestro2_triton_enface_converter
import maestro2_triton.maestro2_triton_oct_converter as maestro2_triton_oct_converter
import maestro2_triton.maestro2_triton_heightmap_converter as maestro2_triton_heightmap_converter
import maestro2_triton.maestro2_triton_retinal_photography_converter as maestro2_triton_retinal_photography_converter
import maestro2_triton.maestro2_triton_volume_converter as maestro2_triton_volume_converter
import imaging.imaging_utils as imaging_utils


def convert_dicom(folder, output):
    """
    Convert DICOM files from a specified folder and save the converted files to an output directory.

    This function processes and converts various types of DICOM files found in the specified input folder.
    It uses different converters based on the file extensions and saves the converted files to the output directory.
    The function also ensures that the output directory exists before conversion.

    Parameters:
    folder (str): The path to the folder containing the DICOM files to be converted.
    output (str): The path to the output directory where the converted files will be saved.

    Returns:
    dict: A dictionary containing:
        - "Foldername" (str): The name of the input folder.
        - "Number of files" (int): The total number of DICOM files found in the input folder.
        - "Correct number of conversion" (bool): A boolean indicating whether the number of conversions matches the number of files.
    """
    if not os.path.exists(output):
        os.makedirs(output)

    x = imaging_utils.get_filtered_file_names(folder)
    uids_sorted = sorted(x, key=imaging_utils.extract_numeric_part)

    conversion_num = 0
    for i in uids_sorted:
        if i.endswith("1.1.dcm"):
            maestro2_triton_oct_converter.convert_dicom(i, output)
            conversion_num += 1

        if i.endswith("2.1.dcm"):
            maestro2_triton_retinal_photography_converter.convert_dicom(i, output)
            conversion_num += 1

        if i.endswith("3.1.dcm") or i.endswith("5.1.dcm"):
            maestro2_triton_volume_converter.convert_dicom(i, output)
            conversion_num += 1

        if i.endswith("4.1.dcm"):
            maestro2_triton_heightmap_converter.convert_dicom(
                i, uids_sorted[0], uids_sorted[1], output
            )
            conversion_num += 1

        if (
            i.endswith("6.3.dcm")
            or i.endswith("6.4.dcm")
            or i.endswith("6.5.dcm")
            or i.endswith("6.80.dcm")
        ):
            maestro2_triton_enface_converter.convert_dicom(
                i,
                uids_sorted[3],  # seg
                uids_sorted[4],  # vol
                uids_sorted[1],  # op
                uids_sorted[0],  # opt
                output,
            )
            conversion_num += 1

    file_num = len(imaging_utils.get_filtered_file_names(folder))
    boolean = file_num == conversion_num

    dic = {
        "Input": folder,
        "Number of files": file_num,
        "Correct number of conversion ": boolean,
    }

    return dic
