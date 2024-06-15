import os
import imaging.imaging_classifying_rules as imaging_classifying_rules
import pandas as pd
import shutil
import pydicom


def get_filtered_file_names(folder_path):
    """
    Get filtered file names from a folder.

    This function walks through a folder and filters out files that don't start with a valid
    character (alphabet or digit) and aren't hidden files (starting with "._").

    Args:
        folder_path (str): The path to the folder to search for files.

    Returns:
        list: A list of filtered file paths.
    """
    filtered_files = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            full_path = os.path.join(root, file)
            file_name = full_path.split("/")[-1]
            if (
                file_name
                and not file_name.startswith("._")
                and (file_name[0].isalpha() or file_name[0].isdigit())
            ):
                filtered_files.append(full_path)
    return filtered_files


def spectralis_get_filtered_file_names(folder_path):
    """
    Get filtered file names from a folder.

    This function walks through a folder and filters out files that don't start with a valid
    character (alphabet or digit) and aren't hidden files (starting with "._").

    Args:
        folder_path (str): The path to the folder to search for files.

    Returns:
        list: A list of filtered file paths.
    """
    filtered_files = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            full_path = os.path.join(root, file)
            if full_path.split("/")[-1] and full_path.split("/")[-1].startswith("0"):
                filtered_files.append(full_path)
    return filtered_files


# def create_record(dataframe, folder_path, sheetname):
#     if not os.path.exists(folder_path):
#         os.makedirs(folder_path)

#     rule_sorted = dataframe.sort_values(by=["Rule", "PatientID"])
#     rule_sorted.to_excel(os.path.join(folder_path, f"{sheetname}_rulebased.xlsx"), index=False)

#     patientid_sorted = dataframe.sort_values(by=["PatientID", "Rule"])
#     patientid_sorted.to_excel(os.path.join(folder_path, f"{sheetname}_ptidbased.xlsx"), index=False)


def list_subfolders(folder_path):
    """
    List all subfolders in a given folder.

    Args:
        folder_path (str): The path to the folder.

    Returns:
        list: A list of paths to subfolders.
    """

    if not os.path.isdir(folder_path):
        print("Invalid folder path.")
        return []
    subfolders = [
        os.path.join(folder_path, item)
        for item in os.listdir(folder_path)
        if os.path.isdir(os.path.join(folder_path, item))
    ]
    return subfolders


def check_files_in_folder(folder_path, file_names):
    """
    Check if specific files exist in a folder.

    Args:
        folder_path (str): The path to the folder.
        file_names (list): A list of file names to check for.

    Returns:
        bool: True if all files are present, False otherwise.
    """
    files = os.listdir(folder_path)

    for file_name in file_names:
        if file_name not in files:
            return False

    return True


def topcon_check_files_expected(folder_path):
    """
    Check if a folder contains the expected number of specific files for Topcon devices.

    Args:
        folder_path (str): The path to the folder.

    Returns:
        str: "Expected" if the files match the expected patterns, otherwise "Unknown".
    """

    files_count_9 = 0

    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith(
                (
                    "1.1.dcm",
                    "2.1.dcm",
                    "3.1.dcm",
                    "4.1.dcm",
                    "5.1.dcm",
                    "6.3.dcm",
                    "6.4.dcm",
                    "6.5.dcm",
                    "6.80.dcm",
                )
            ) and file.startswith("2"):
                files_count_9 += 1

    # Check if files match expected patterns
    if (files_count_9 == 2) or (files_count_9 == 9):
        return "Expected"
    else:
        return "Unknown"


def topcon_process_folder(folder_path, outputpath, rule, empty_df):
    """
    Process a folder of Topcon device files and copy them to an output directory based on rules.

    Args:
        folder_path (str): The path to the folder to process.
        outputpath (str): The path to the output directory.
        rule (str): The classification rule to apply.
        empty_df (pd.DataFrame): A DataFrame to store information about processed files.

    Returns:
        pd.DataFrame: Updated DataFrame with information about processed files.
    """
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith("1.1.dcm") and file.startswith("2"):
                file_path = os.path.join(root, file)
                directory_one_level_up = os.path.basename(os.path.dirname(file_path))
                info = image_classifying_rules.extract_dicom_entry(file_path)
                laterality = info.laterality
                patientid = info.patientid
                error = info.error
                if rule.endswith("_oct"):
                    output = f"{outputpath}/{rule[:-4]}/{rule[:-4]}_{patientid}_{laterality}_{directory_one_level_up}"
                else:
                    output = f"{outputpath}/{rule}/{rule}_{patientid}_{laterality}_{directory_one_level_up}"
                os.makedirs(os.path.dirname(output), exist_ok=True)
                shutil.copytree(os.path.dirname(file_path), output)

                if rule in [
                    "maestro2_mac_6x6_octa_oct",
                    "maestro2_macula_oct_oct",
                    "maestro2_3d_wide_oct_oct",
                    "triton_3d_radial_oct_oct",
                    "triton_macula_6x6_octa_oct",
                    "triton_macula_12x12_octa_oct",
                ]:
                    row_to_append = {
                        "Rule": rule[:-4],
                        "PatientID": patientid,
                        "Folder": directory_one_level_up,
                        "Laterality": laterality,
                        "Error": error,
                    }

                    empty_df = pd.concat(
                        [empty_df, pd.DataFrame([row_to_append])], ignore_index=True
                    )
    return empty_df


def filter_eidon_files(file, outputfolder):
    """
    Filter and process EIDON files based on classification rules.

    This function applies classification rules to a DICOM file, extracts relevant information,
    and copies the file to an appropriate output directory based on the classification rule.

    Args:
        file (str): The path to the DICOM file to be processed.
        outputfolder (str): The directory where the processed files will be stored.

    Returns:
        dict: A dictionary containing information about the processed file, including rule, patient ID,
        patient name, laterality, rows, columns, SOP instance UID, series instance UID, filename,
        original file path, and any errors encountered.
    """

    filename = file.split("/")[-1]
    rule = imaging_classifying_rules.find_rule(file)
    b = imaging_classifying_rules.extract_dicom_entry(file)
    laterality = b.laterality
    uid = b.sopinstanceuid
    patientid = b.patientid
    rows = b.rows
    columns = b.columns
    seriesuid = b.seriesuid
    error = b.error
    name = b.name

    original_path = file
    output_path = f"{outputfolder}/{rule}/{rule}_{filename}"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    shutil.copyfile(original_path, output_path)

    dic = {
        "Rule": rule,
        "PatientID": patientid,
        "PatientName": name,
        "Laterality": laterality,
        "Rows": rows,
        "Columns": columns,
        "SOPInstanceuid": uid,
        "SeriesInstanceuid": seriesuid,
        "Filename": filename,
        "Path": file,
        "Error": error,
    }
    print(dic)
    return dic


protocol_mapping = {
    "optomed_mac_or_disk_centered_cfp": "optomed mac or disk centered color retinal photography",
    "eidon_mosaic_cfp": "eidon mosaic color retinal photography",
    "eidon_uwf_central_faf": "eidon central autofluorescence retinal photography",
    "eidon_uwf_central_ir": "eidon central infrared retinal photography",
    "eidon_uwf_nasal_cfp": "eidon nasal color retinal photography",
    "eidon_uwf_temporal_cfp": "eidon temporal color retinal photography",
    "eidon_uwf_central_cfp": "eidon central color retinal photography",
    "maestro2_3d_wide_oct": "maestro2 3d wide oct",
    "maestro2_mac_6x6_octa": "maestro2 macula 6x6 octa",
    "maestro2_3d_macula_oct": "maestro2 3d macula oct",
    "triton_3d_radial_oct": "triton 3d radial oct",
    "triton_macula_6x6_octa": "triton macula 6x6 octa",
    "triton_macula_12x12_octa": "triton macula 12x12 octa",
    "spectralis_onh_rc_hr_oct": "spectralis onh rc hr oct",
    "spectralis_onh_rc_hr_retinal_photography": "spectralis onh rc hr oct",
    "spectralis_ppol_mac_hr_oct": "spectralis ppol mac hr oct",
    "spectralis_ppol_mac_hr_oct_small": "spectralis ppol mac hr oct",
    "spectralis_ppol_mac_hr_retinal_photography": "spectralis ppol mac hr oct",
    "spectralis_ppol_mac_hr_retinal_photography_small": "spectralis ppol mac hr oct",
    "flio": "fluorescence lifetime imaging ophthalmoscopy",
}


name_mapping = {
    "optomed_mac_or_disk_centered_cfp": "optomed_mac_or_disk_centered_cfp",
    "eidon_mosaic_cfp": "eidon_mosaic_cfp",
    "eidon_uwf_central_faf": "eidon_uwf_central_faf",
    "eidon_uwf_central_ir": "eidon_uwf_central_ir",
    "eidon_uwf_nasal_cfp": "eidon_uwf_nasal_cfp",
    "eidon_uwf_temporal_cfp": "eidon_uwf_temporal_cfp",
    "eidon_uwf_central_cfp": "eidon_uwf_central_cfp",
    "maestro2_3d_wide_oct": "maestro2_3d_wide",
    "maestro2_mac_6x6_octa": "maestro2_macula_6x6",
    "maestro2_3d_macula_oct": "maestro2_3d_macula",
    "triton_3d_radial_oct": "triton_3d_radial",
    "triton_macula_6x6_octa": "triton_macula_6x6",
    "triton_macula_12x12_octa": "triton_macula_12x12",
    "spectralis_onh_rc_hr_oct": "spectralis_onh_rc_hr_oct",
    "spectralis_onh_rc_hr_retinal_photography": "spectralis_onh_rc_hr_ir",
    "spectralis_ppol_mac_hr_oct": "spectralis_ppol_mac_hr_oct",
    "spectralis_ppol_mac_hr_oct_small": "spectralis_ppol_mac_hr_oct",
    "spectralis_ppol_mac_hr_retinal_photography": "spectralis_ppol_mac_hr_ir",
    "spectralis_ppol_mac_hr_retinal_photography_small": "spectralis_ppol_mac_hr_ir",
    "flio": "flio",
}


maestro_octa_submodality_mapping = {
    "1.1.dcm": "oct",
    "2.1.dcm": "ir",
    "3.1.dcm": "flow_cube_raw_data",
    "4.1.dcm": "segmentation",
    "5.1.dcm": "flow_cube",
    "6.3.dcm": "enface",
    "6.4.dcm": "enface",
    "6.5.dcm": "enface",
    "6.80.dcm": "enface",
}

topcon_except_maestro_octa_submodality_mapping = {
    "1.1.dcm": "oct",
    "2.1.dcm": "cfp",
    "3.1.dcm": "flow_cube_raw_data",
    "4.1.dcm": "segmentation",
    "5.1.dcm": "flow_cube",
    "6.3.dcm": "enface",
    "6.4.dcm": "enface",
    "6.5.dcm": "enface",
    "6.80.dcm": "enface",
}


modality_folder_mapping = {
    "ir": "retinal_photography",
    "cfp": "retinal_photography",
    "faf": "retinal_photography",
    "flow_cube": "retinal_octa",
    "segmentation": "retinal_octa",
    "enface": "retinal_octa",
    "_oct_": "retinal_oct",
    "flio": "retinal_flio",
}

submodality_folder_mapping = {
    "ir": "ir",
    "cfp": "cfp",
    "faf": "faf",
    "oct": "oct_structural_scan",
    "flow_cube": "flow_cube",
    "segmentation": "segmentation",
    "enface": "enface",
    "flio": "flio",
}

device_folder_mapping = {
    "eidon": "icare_eidon",
    "optomed": "optomed_aurora",
    "maestro2": "topcon_maestro2",
    "triton": "topcon_triton",
    "spectralis": "heidelberg_spectralis",
    "flio": "heidelberg_flio",
}


def get_description(filename, mapping):
    """
    Get a description based on a filename and a mapping dictionary.

    Args:
        filename (str): The name of the file.
        mapping (dict): A dictionary mapping keys to descriptions.

    Returns:
        str: The description corresponding to the filename, or "Description not found" if no match is found.
    """
    for key, value in mapping.items():
        if key in filename:
            return value
    return "Description not found"


def check_format(string):
    """
    Check if a string follows a specific format.

    Args:
        string (str): The string to check.

    Returns:
        bool: True if the string follows the format "AIREADI-XXXX" where XXXX are digits, otherwise False.
    """
    if string.startswith("AIREADI-"):
        digits = string[len("AIREADI-") : len("AIREADI-") + 4]
        if len(digits) == 4 and digits[0] in ["1", "4", "7"] and digits.isdigit():
            return True
    return False


def find_number(val):
    """
    Find the first four-digit number in a string.

    Args:
        val (str): The string to search.

    Returns:
        str: The first four-digit number found, or "noid" if none is found.
    """
    is_number = False
    buffer = []
    for i in range(len(val)):
        if val[i].isnumeric():
            buffer.append(val[i])
            is_number = True
        else:
            if is_number:
                return "".join(buffer[:4])
    if is_number:
        return "".join(buffer[:4])
    return "noid"


def find_id(patientid, patientname):
    """
    Find a patient ID based on the given patient ID and name.

    This function checks the patient ID and name to extract a valid ID. It first checks if
    the ID follows a specific format, and if not, it searches for a number within the ID
    or name strings.

    Args:
        patientid (str): The patient ID.
        patientname (str): The patient name.

    Returns:
        str: The extracted patient ID or "noid" if no valid ID is found.
    """
    patientid = (
        ""
        if patientid == "None" or patientid is None or not isinstance(patientid, str)
        else patientid
    )
    patientname = (
        ""
        if patientname == "None"
        or patientname is None
        or not isinstance(patientname, str)
        else patientname
    )
    if check_format(patientid) == True:
        return patientid[len("AIREADI-") : len("AIREADI-") + 4]

    else:
        if find_number(patientid) != "noid":
            return find_number(patientid)
        else:
            if find_number(patientname) != "noid":

                return find_number(patientname)
            else:
                return find_number(patientname)


def format_file(file, output):
    """
    Format a DICOM file and save it to an output directory.

    This function reads a DICOM file, extracts relevant information, and formats it according to
    specified rules. It then saves the formatted file to the appropriate output directory.

    Args:
        file (str): The path to the DICOM file to be formatted.
        output (str): The base path to the output directory.

    Returns:
        dict: A dictionary containing protocol, patient ID, and laterality information.
    """
    dataset = pydicom.dcmread(file)
    try:
        uid = dataset.SOPInstanceUID
    except AttributeError:
        full_dir_path = output + "/missing_critical_info/no_uid/"
        os.makedirs(full_dir_path, exist_ok=True)
        filename = os.path.basename(file)
        full_file_path = os.path.join(full_dir_path, filename)
        dataset.save_as(full_file_path)
    else:
        id = find_id(dataset.PatientID, dataset.PatientName)

        if id == "noid":
            full_dir_path = output + "/missing_critical_info/no_id/"
            os.makedirs(full_dir_path, exist_ok=True)
            filename = os.path.basename(file)
            full_file_path = os.path.join(full_dir_path, filename)
            dataset.save_as(full_file_path)

        else:
            uid = dataset.SOPInstanceUID
            protocol = get_description(file, protocol_mapping)
            dataset.PatientID = id
            dataset.PatientName = ""
            dataset.PatientSex = "M"
            dataset.PatientBirthDate = ""
            dataset.ProtocolName = protocol

            laterality = dataset.ImageLaterality.lower()
            patientid = id
            protocol = protocol

            modality = get_description(file, name_mapping)

            submodality = ""

            if "maestro2_mac_6x6_octa" in file:
                submodality = get_description(file, maestro_octa_submodality_mapping)
                submodality = f"{submodality}"
                filename = f"{id}_{modality}_{submodality}_{laterality}_{uid}.dcm"

            elif "maestro2" in file or "triton" in file:
                submodality = get_description(
                    file, topcon_except_maestro_octa_submodality_mapping
                )
                submodality = f"{submodality}"
                filename = f"{id}_{modality}_{submodality}_{laterality}_{uid}.dcm"

            elif "flio" in file:
                submodality = next(
                    (
                        submodality
                        for submodality in ["short_wavelength", "long_wavelength"]
                        if submodality in file
                    ),
                    "unknown_submodality",
                )
                filename = f"{id}_{modality}_{submodality}_{laterality}_{uid}.dcm"

            else:
                filename = f"{id}_{modality}_{laterality}_{uid}.dcm"

            modality_folder = get_description(filename, modality_folder_mapping)
            submodality_folder = get_description(filename, submodality_folder_mapping)
            device_folder = get_description(filename, device_folder_mapping)

            folderpath = (
                f"/{modality_folder}/{submodality_folder}/{device_folder}/{id}/"
            )

            full_dir_path = output + folderpath

            os.makedirs(full_dir_path, exist_ok=True)

            full_file_path = os.path.join(full_dir_path, filename)

            dataset.save_as(full_file_path)

            dic = {
                "Protocol": protocol,
                "PatientID": dataset.PatientID,
                "Laterality": dataset.ImageLaterality,
            }

            print(dic)

            return dic
