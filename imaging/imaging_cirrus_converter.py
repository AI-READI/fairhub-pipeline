import os
import cirrus.cirrus_retinal_photography_converter as cirrus_retinal_photography_converter
import cirrus.cirrus_enface_converter as cirrus_enface_converter
import cirrus.cirrus_oct_converter as cirrus_oct_converter
import cirrus.cirrus_heightmap_converter as cirrus_heightmap_converter
import cirrus.cirrus_volume_converter as cirrus_volume_converter
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

    if len(x) == 11:

        struc = imaging_utils.find_string_in_files(x, "Struc.")
        ir = imaging_utils.find_string_in_files(x, "LSO")
        flow = imaging_utils.find_string_in_files(x, "Flow")
        seg = imaging_utils.find_string_in_files(x, "Seg")

        conversion_num = 0
        for file in x:
            if "Struc." in file:
                try:
                    cirrus_oct_converter.convert_dicom(file, output)

                except Exception as e:
                    print(f"An error occurred: {e}")

                conversion_num += 1

            elif "LSO" in file:
                try:
                    cirrus_retinal_photography_converter.convert_dicom(file, output)
                except Exception as e:
                    print(f"An error occurred: {e}")
                conversion_num += 1

            elif "Flow" in file:
                try:
                    cirrus_volume_converter.convert_dicom(file, output)
                except Exception as e:
                    print(f"An error occurred: {e}")
                conversion_num += 1

            elif "Seg" in file:
                try:
                    cirrus_heightmap_converter.convert_dicom(file, struc, ir, output)

                except Exception as e:
                    print(f"An error occurred: {e}")
                conversion_num += 1

            elif "AngioEnface" in file:
                try:
                    cirrus_enface_converter.convert_dicom(
                        file,
                        seg,
                        flow,
                        struc,
                        ir,
                        output,
                    )
                except Exception as e:
                    print(f"An error occurred: {e}")
                conversion_num += 1

            else:
                print("No converter found for the file: ", file)

        file_num = len(imaging_utils.get_filtered_file_names(folder))
        boolean = file_num == conversion_num

        dic = {
            "Input": folder,
            "Number of files": file_num,
            "Correct number of conversion ": boolean,
        }

    if len(x) == 2:

        struc = imaging_utils.find_string_in_files(x, "Struc.")
        ir = imaging_utils.find_string_in_files(x, "LSO")

        conversion_num = 0
        for file in x:
            if "Struc." in file:
                try:
                    cirrus_oct_converter.convert_dicom(file, output)
                except Exception as e:
                    print(f"An error occurred: {e}")
                conversion_num += 1

            elif "LSO" in file:
                try:
                    cirrus_retinal_photography_converter.convert_dicom(file, output)
                except Exception as e:
                    print(f"An error occurred: {e}")
                conversion_num += 1

            else:
                print("No converter found for the file: ", file)

        file_num = len(imaging_utils.get_filtered_file_names(folder))
        boolean = file_num == conversion_num

        dic = {
            "Input": folder,
            "Number of files": file_num,
            "Correct number of conversion ": boolean,
        }

    return dic
