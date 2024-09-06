import os
import imaging.imaging_utils as imaging_utils
import imaging.imaging_classifying_rules as imaging_classifying_rules


def filter_maestro2_triton_files(folder, output):
    """
    Filter and process Maestro2 or Triton files based on classification rules.

    This function performs the following steps:
    1. Checks critical information from files in the given folder.
    2. Determines the expected status of the files (e.g., "Unknown" or "Expected").
    3. Applies classification rules to the DICOM files.
    4. Processes and copies the files to the appropriate output directory based on the classification rule.

    Args:
        folder (str): The path to the folder containing DICOM files to be processed.
        output (str): The directory where the processed files will be stored.

    Returns:
        dict: A dictionary containing information about the processed files, including the protocol used
        and the folder name. The dictionary has the following keys:
            - "Protocol" (str): The protocol applied to the files.
            - "Foldername" (str): The name of the folder processed.
    """
    filtered_list = imaging_utils.get_filtered_file_names(folder)
    all_dicom = all(
        imaging_classifying_rules.is_dicom_file(file) for file in filtered_list
    )

    if all_dicom:

        check = imaging_utils.check_critical_info_from_files_in_folder(folder)

        if check == "pass":

            expected_status = imaging_utils.topcon_check_files_expected(folder)

            if expected_status == "Unknown":
                protocol = "unknown_protocol"
                imaging_utils.topcon_process_folder(folder, output, protocol)

            elif expected_status == "Expected":

                for root, dirs, files in os.walk(folder):
                    for file in files:
                        if file.endswith("1.1.dcm") and file.startswith("2"):
                            file_path = os.path.join(root, file)
                            protocol = imaging_classifying_rules.find_rule(file_path)
                            imaging_utils.topcon_process_folder(
                                folder, output, protocol
                            )

        else:
            protocol = f"{check}"

            imaging_utils.topcon_process_folder(folder, output, protocol)

        dic = {"Protocol": protocol, "Foldername": folder}
        print(dic)

    else:
        protocol = "invalid_dicom"
        imaging_utils.topcon_process_folder(folder, output, protocol)
        dic = {"Protocol": protocol, "Foldername": folder}
        print(dic)

    return dic
