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
    results = []
    for pt in pts:
        laterality = imaging_utils.list_subfolders(pt)
        for one in laterality:
            folder_path = one

            if imaging_utils.check_files_in_folder(
                folder_path, ["Measurement.sdt", "measurement_info.html"]
            ):

                html_file = imaging_utils.get_html_in_folder(folder_path)
                html_pt_id = imaging_utils.get_patient_id_from_html(
                    f"{folder_path}/{html_file}"
                )

                patient = pt.split("/")[-1]
                side = one.split("/")[-1]

                outputpath = f"{output}/flio_{patient}_{side}"

                os.makedirs(os.path.dirname(outputpath), exist_ok=True)
                shutil.copytree(folder_path, outputpath, dirs_exist_ok=True)

                #  # Copy individual files to overwrite if they already exist
                # for filename in os.listdir(folder_path):
                #     source_file = os.path.join(folder_path, filename)
                #     destination_file = os.path.join(outputpath, filename)
                #     if os.path.isfile(source_file):
                #         shutil.copy2(source_file, destination_file)  # Overwrite existing files

                dic = {
                    "Input batch folder": one,
                    "Output folder": outputpath.split("/")[-1],
                    "Patient ID HTML": html_pt_id,
                    "Error": "None",
                }

            else:
                print("else")

                dic = {
                    "Input batch folder": input,
                    "Output folder": outputpath.split("/")[-1],
                    "Error": "Missing file",
                }

            results.append(dic)

    return results
