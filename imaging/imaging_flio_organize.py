import shutil
import imaging.imaging_utils as imaging_utils
import os


def filter_flio_files_process(input, output):
    """
    Processes FLIO files by filtering folders that contain the required files and copies them to the specified output location.

    Args:
        input (str): Full path to the input folder containing patient subfolders with FLIO data.
        output (str): Full path to the output folder where filtered files will be copied.

    Returns:
        dict: A dictionary containing information about the processing, including the input folder, output folder, and any errors encountered.
    """
    pts = imaging_utils.list_subfolders(input)
    for pt in pts:
        laterality = imaging_utils.list_subfolders(pt)
        for one in laterality:
            folder_path = one
            if imaging_utils.check_files_in_folder(
                folder_path, ["Measurement.sdt", "measurement_info.html"]
            ):
                patient = pt.split("/")[-1]
                side = one.split("/")[-1]

                outputpath = f"{output}/flio_{patient}_{side}"
                os.makedirs(os.path.dirname(outputpath), exist_ok=True)
                shutil.copytree(folder_path, outputpath, dirs_exist_ok=True)

                dic = {
                    "Input batch folder": input,
                    "Output folder": outputpath.split("/")[-1],
                    "Error": "None",
                }

                print(dic)

            else:
                dic = {
                    "Input batch folder": input,
                    "Output folder": outputpath.split("/")[-1],
                    "Error": "Missing file",
                }
                print(dic)

    return dic
