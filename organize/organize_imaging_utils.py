import os
import shutil
from datetime import datetime
import imaging.imaging_utils as imaging_utils
import imaging.imaging_classifying_rules as imaging_classifying_rules
import pydicom
import pandas as pd
from tqdm import tqdm
import numpy as np

# basic comparison and base file names


def get_base_file_names(file_paths):
    """
    Extracts base filenames from a list of file paths, excluding hidden files (those starting with a dot).

    Args:
    file_paths (list): A list of file paths.

    Returns:
    list: A list of base file names without hidden files.
    """
    base_names = []
    for path in file_paths:
        base_name = os.path.basename(path)
        if not base_name.startswith("."):
            base_names.append(base_name)
    return base_names


def compare_outputs(folder1, folder2):
    """
    Compares the contents of two folders and identifies the differences in filenames.

    Args:
    folder1 (str): Path to the first folder.
    folder2 (str): Path to the second folder.

    Returns:
    tuple: A tuple containing:
        - diff_files: Files in folder1 but not in folder2.
        - diff_files1: Files in folder2 but not in folder1.
        - num1: Number of files in folder1.
        - num2: Number of files in folder2.
    """
    files1 = get_base_file_names(imaging_utils.get_filtered_all_file_names(folder1))
    files2 = get_base_file_names(imaging_utils.get_filtered_all_file_names(folder2))
    diff_files = list(set(files1) - set(files2))
    diff_files1 = list(set(files2) - set(files1))
    num1 = len(files1)
    num2 = len(files2)
    return diff_files, diff_files1, num1, num2


def compare_if_num_metadata_equal_to_data(folder1, folder2):
    """
    Compares the number of files in two folders.

    Args:
    folder1 (str): Path to the first folder.
    folder2 (str): Path to the second folder.

    Returns:
    tuple: A tuple containing the number of files in folder1 and folder2.
    """
    files1 = get_base_file_names(imaging_utils.get_filtered_all_file_names(folder1))
    files2 = get_base_file_names(imaging_utils.get_filtered_all_file_names(folder2))
    num1 = len(files1)
    num2 = len(files2)
    return num1, num2


def save_base_filenames(folder_path, txtname, output_path):
    """
    Saves the base filenames of files in a folder to a text file.

    Args:
    folder_path (str): Path to the folder.
    txtname (str): Name of the output text file.
    output_path (str): Path where the text file will be saved.
    """
    files = imaging_utils.get_filtered_file_names(folder_path)
    with open(f"{output_path}/{txtname}.txt", "w") as file:
        for item in files:
            file.write(f"{item}\n")


def read_base_filenames(txtfile):
    """
    Reads filenames from a text file and cleans them by removing extensions and underscores.

    Args:
    txtfile (str): Path to the text file containing filenames.

    Returns:
    set: A set of cleaned filenames.
    """

    with open(f"{txtfile}", "r") as file:
        list_from_file = [line.strip() for line in file.readlines()]

        if "json" in list_from_file:
            cleaned = [
                os.path.basename(file)
                .replace(".json", "")
                .replace("_", "")
                .replace(".", "")
                for file in list_from_file
            ]

        else:
            cleaned = [
                os.path.basename(file).replace("_", "").replace(".", "")
                for file in list_from_file
            ]

        return set(cleaned)


# file organize related


def clean_filename(filename):
    """
    Removes all underscores and periods from a filename.

    Args:
    - filename (str): The filename to clean.

    Returns:
    - str: The cleaned filename with underscores and periods removed.
    """
    return filename.replace("_", "").replace(".", "")


def merge_folders_filter_id_files(patientid_csv, sources, destination, remove_txt):
    """
    Merge files from the source directories into the destination directory
    based on a list of unique study IDs and an optional list of files to exclude by base name.

    Args:
    - patientid_csv (str): Path to the CSV file containing patient IDs.
    - sources (list): List of source directories.
    - destination (str): Path to the destination directory.
    - remove_txt (str): Path to the text file that contains the list of base filenames (without extension)
                        that need to be excluded.
    """

    # Read the files to be removed from the remove_txt file
    with open(remove_txt, "r", encoding="utf-8") as file:
        # Read all lines and strip newline characters
        files_to_remove = [line.strip() for line in file]

    # Read the CSV and get the list of unique study IDs
    df = pd.read_csv(patientid_csv, skiprows=2, header=None)
    unique_study_ids = df[0].astype(str).unique()  # Convert to string for consistency
    unique_study_ids_list = unique_study_ids.tolist()

    # Convert the files_to_remove list to a set for faster lookup
    files_to_remove_set = set(files_to_remove)

    for source in tqdm(sources):
        print(source)
        for root, dirs, files in os.walk(source):
            relative_path = os.path.relpath(root, source)
            dest_path = os.path.join(destination, relative_path)

            # Collect valid files (those matching the IDs and not in the removal list)
            valid_files = []

            for file in files:
                if not file.startswith("._"):
                    file_id = file.split("_")[0]

                    # Get the base name of the file (without the extension)
                    base_file_name = os.path.splitext(file)[0]
                    cleaned_base_file_name = clean_filename(base_file_name)

                    # Check if any cleaned element in files_to_remove_set is part of the cleaned base file name
                    should_remove = any(
                        clean_filename(item) in cleaned_base_file_name
                        for item in files_to_remove_set
                    )

                    if file_id in unique_study_ids_list and not should_remove:
                        valid_files.append(file)
                    else:
                        if should_remove:
                            print(
                                f"File {file} is in the removal list after cleaning. Skipping..."
                            )
                        else:
                            print(
                                f"File {file} not in the list of study IDs. Skipping..."
                            )

            # Only create the destination folder if there are valid files to copy
            if valid_files:
                os.makedirs(
                    dest_path, exist_ok=True
                )  # Create the folder if it doesn't exist

                # Copy over the valid files
                for file in valid_files:
                    src_file = os.path.join(root, file)
                    dest_file = os.path.join(dest_path, file)

                    if not os.path.exists(dest_file):
                        shutil.copy2(src_file, dest_file)
                        # print(f"Copied {src_file} to {dest_file}")
                    else:
                        print(f"File {dest_file} already exists. Skipping...")


# file inventory related


def get_protocol(file):
    """
    Extracts protocol information from a DICOM file using classification rules.

    If the file is valid and matches a known rule, information about the protocol, patient ID,
    and laterality is returned. If the file is not a valid DICOM, an error message is returned.

    Args:
    file (str): The path to the DICOM file.

    Returns:
    dict: A dictionary containing protocol information:
        - "Rule": The classification rule for the file.
        - "PatientID": Last 4 digits of the patient ID.
        - "Laterality": The laterality of the image (left/right eye).
        - "Input": Part of the input file path.
    """

    if imaging_classifying_rules.is_dicom_file(file):

        rule = imaging_classifying_rules.find_rule(file)
        if rule == "spectralis_ppol_mac_hr_oct_small":
            rule == "spectralis_ppol_mac_hr_oct"

        b = imaging_classifying_rules.extract_dicom_entry(file)

        laterality = b.laterality

        patientid = b.patientid

        original_path = file

        dic = {
            "Rule": rule,
            "PatientID": patientid[-4:],
            "Laterality": laterality,
            "Input": "/".join(file.split("/")[4:5]),
        }

    else:

        error = "Invalid_dicom"

        original_path = file

        dic = {
            "Rule": "invalid_dicom",
            "PatientID": "N/A",
            "Laterality": "N/A",
            "Input": "/".join(file.split("/")[4:5]),
        }

    return dic


def get_file_info_cirrus(folder_unzip):
    """
    Gathers file information from Cirrus folders, extracting patient ID and protocol details.

    Args:
    folder_unzip (str): The path to the unzipped folder containing patient data.

    Returns:
    list: A list of patient info, where each element is a list containing:
        - Patient ID (last 4 digits)
        - Protocol laterality (protocol name and laterality)
        - Batch folder name
    """

    patient_info = []

    folders = imaging_utils.list_subfolders(folder_unzip)

    for i in range(len(folders)):

        files = imaging_utils.get_filtered_file_names(folders[i])

        # Check if there are any files in the folder
        if len(files) > 0:
            file = next((file for file in files if file.endswith(".dcm")), None)
            a = pydicom.dcmread(file)

            patient_id = a.PatientID[-4:]
            protocol_laterality = (
                a.ProtocolName.replace(" ", "") + "-" + a.ImageLaterality
            )
            if "fda" in folders[i]:
                batch_foldername = "_".join(
                    os.path.basename(folders[i]).split("_")[0:-2]
                )
            else:
                batch_foldername = "_".join(
                    os.path.basename(folders[i]).split("_")[0:-1]
                )
            patient_info.append([patient_id, protocol_laterality, batch_foldername])

        else:
            print(f"No files found in {folders[i]}")

    return patient_info


def get_file_info_spectralis(folder_unzip):
    """
    Gathers file information from Spectralis folders, extracting patient ID and protocol details.

    Args:
    folder_unzip (str): The path to the unzipped folder containing patient data.

    Returns:
    list: A list of patient info, where each element is a list containing:
        - Patient ID (last 4 digits)
        - Protocol laterality (protocol name and laterality)
        - Batch folder name
    """

    patient_info = []

    folders = imaging_utils.list_subfolders(folder_unzip)

    for i in tqdm(range(len(folders)), desc="Processing folders"):

        files = imaging_utils.spectralis_get_filtered_file_names(folders[i])

        for a in range(len(files)):
            x = get_protocol(files[a])
            patient_id = x["PatientID"]
            protocol_laterality = x["Rule"] + "-" + x["Laterality"]
            batch_foldername = x["Input"]

            patient_info.append([patient_id, protocol_laterality, batch_foldername])

    return patient_info


def get_file_info_topcon(folder_unzip):
    """
    Gathers file information from Topcon folders, extracting patient ID and protocol details.

    Args:
    folder_unzip (str): The path to the unzipped folder containing patient data.

    Returns:
    list: A list of patient info, where each element is a list containing:
        - Patient ID (last 4 digits)
        - Protocol laterality (protocol name and laterality)
        - Batch folder name
    """

    patient_info = []

    folders = imaging_utils.list_subfolders(folder_unzip)

    for i in tqdm(range(len(folders)), desc="Processing folders"):

        files = imaging_utils.get_filtered_file_names(folders[i])

        if len(files) == 2:
            file = next((file for file in files if file.endswith("1.1.dcm")), None)
            a = pydicom.dcmread(file)
            patient_id = a.PatientID[-4:]

            if not hasattr(a, "ProtocolName") or not a.ProtocolName:
                print(f"Missing ProtocolName in file: {file}")
                protocol_name = ""
            else:
                protocol_name = a.ProtocolName.replace(" ", "").replace("/", "_")

            protocol_laterality = protocol_name + "-" + a.ImageLaterality
            batch_foldername = "_".join(os.path.basename(folders[i]).split("_")[0:-2])

            # Store them in a list and append to patient_info
            patient_info.append([patient_id, protocol_laterality, batch_foldername])

        elif len(files) == 9:

            required_suffixes = ["6.3.dcm", "6.4.dcm", "6.5.dcm", "6.80.dcm"]

            all_found = all(
                any(file.endswith(suffix) for file in files)
                for suffix in required_suffixes
            )
            if all_found:
                file = next((file for file in files if file.endswith("1.1.dcm")), None)
                a = pydicom.dcmread(file)
                patient_id = a.PatientID[-4:]

                if not hasattr(a, "ProtocolName") or not a.ProtocolName:
                    print(f"Missing ProtocolName in file: {file}")
                    protocol_name = ""
                else:
                    protocol_name = a.ProtocolName.replace(" ", "").replace("/", "_")

                protocol_laterality = protocol_name + "-" + a.ImageLaterality

                if "fda" in folders[i]:
                    batch_foldername = "_".join(
                        os.path.basename(folders[i]).split("_")[0:-2]
                    )
                else:
                    batch_foldername = "_".join(
                        os.path.basename(folders[i]).split("_")[0:-1]
                    )

                # Store them in a list and append to patient_info
                patient_info.append([patient_id, protocol_laterality, batch_foldername])
            else:
                print(f"wrong protocols in {folders[i]}")

        else:
            print(f"No files found in {folders[i]}")

    return patient_info


def output_make_template_dic_given_ids(file, device):
    """
    Generates a dictionary template for a given device and list of unique study IDs extracted from a file.

    Args:
        file (str): Path to the CSV file containing patient data.
        device (str): The device type (e.g., "cirrus", "maestro2", "triton", etc.) which determines the categories.

    Returns:
        dict: A dictionary where the keys are patient IDs and the values are dictionaries with empty strings for each category based on the device type.
    """

    df = pd.read_csv(file, skiprows=2, header=None)

    unique_study_ids = df[0].unique()

    print(len(unique_study_ids))

    if device == "cirrus":
        # Categories
        categories = [
            "MAC-Macular_Cube_512x128-R",
            "MAC-Macular_Cube_512x128-L",
            "ONH-Optic_Disc_Cube_200x200-R",
            "ONH-Optic_Disc_Cube_200x200-L",
            "MAC-Angiography_6x6_mm-R",
            "MAC-Angiography_6x6_mm-L",
            "ONH-Angiography_6x6_mm-R",
            "ONH-Angiography_6x6_mm-L",
        ]

    elif device == "maestro2":
        # Categories
        categories = [
            "maestro2_3d_macula_oct-R",
            "maestro2_3d_macula_oct-L",
            "maestro2_3d_wide_oct-R",
            "maestro2_3d_wide_oct-L",
            "maestro2_macula_6x6_octa-R",
            "maestro2_macula_6x6_octa-L",
        ]

    elif device == "triton":
        # Categories
        categories = [
            "triton_3d_radial_oct-R",
            "triton_3d_radial_oct-L",
            "triton_macula_6x6_octa-R",
            "triton_macula_6x6_octa-L",
            "triton_macula_12x12_octa-R",
            "triton_macula_12x12_octa-L",
        ]

    elif device == "spectralis":
        # Categories
        categories = [
            "spectralis_onh_rc_hr_oct-R",
            "spectralis_onh_rc_hr_oct-L",
            "spectralis_ppol_mac_hr_oct-R",
            "spectralis_ppol_mac_hr_oct-L",
        ]

    elif device == "optomed":
        # Categories
        categories = [
            "optomed_mac_or_disk_centered_color_retinal_photography-L",
            "optomed_mac_or_disk_centered_color_retinal_photography-R",
        ]

    elif device == "eidon":
        # Categories
        categories = [
            "eidon_mosaic_color_retinal_photography-R",
            "eidon_mosaic_color_retinal_photography-L",
            "eidon_nasal_color_retinal_photography-R",
            "eidon_nasal_color_retinal_photography-L",
            "eidon_temporal_color_retinal_photography-R",
            "eidon_temporal_color_retinal_photography-L",
            "eidon_central_color_retinal_photography-R",
            "eidon_central_color_retinal_photography-L",
            "eidon_central_autofluorescence_retinal_photography-R",
            "eidon_central_autofluorescence_retinal_photography-L",
            "eidon_central_infrared_retinal_photography-R",
            "eidon_central_infrared_retinal_photography-L",
        ]

    elif device == "flio":
        # Categories
        categories = [
            "fluorescence_lifetime_imaging_ophthalmoscopy-R",
            "fluorescence_lifetime_imaging_ophthalmoscopy-L",
        ]

    # Creating the dictionary
    patient_dict = {
        str(patient_id): {category: "" for category in categories}
        for patient_id in unique_study_ids
    }

    return patient_dict


def output_get_file_info(folders, device):
    """
    Retrieves patient information by reading DICOM files in the specified folders for a given device.

    Args:
        folders (list): List of folder paths containing DICOM files.
        device (str): The device type (e.g., "cirrus", "maestro2", "triton", etc.) used to filter the file names.

    Returns:
        list: A list of lists where each sublist contains patient ID and protocol laterality.
    """
    patient_info = []

    for folder in folders:
        file_list = imaging_utils.get_filtered_all_file_names(folder)
        file_list = [
            file
            for file in file_list
            if f"{device}" in file and not os.path.basename(file).startswith(".")
        ]
        for file in file_list:
            try:
                dataset = pydicom.dcmread(file)
                patient_id = dataset.PatientID
                protocol_name = dataset.ProtocolName.replace(" ", "_").replace("/", "_")
                protocol_laterality = protocol_name + "-" + dataset.ImageLaterality

                patient_info.append([patient_id, protocol_laterality])
            except:
                print(f"Error reading file: {file}")

    return patient_info


def output_fill_table(patient_dict, patient_info):
    """
    Fills the template dictionary with 'O' for the protocols found in the patient information and converts it into a DataFrame.

    Args:
        patient_dict (dict): The dictionary template for patients and protocols.
        patient_info (list): A list of patient IDs and protocol laterality pairs.

    Returns:
        pandas.DataFrame: A DataFrame where rows are patient IDs and columns represent the presence ('O') of each protocol.
    """
    for i in tqdm(range(len(patient_info))):
        patient_id = patient_info[i][0]

        protocol_laterality = patient_info[i][1]

        if patient_id in patient_dict.keys():
            if protocol_laterality in patient_dict[patient_id]:
                patient_dict[patient_id][protocol_laterality] = "O"

    df = pd.DataFrame.from_dict(patient_dict, orient="index")

    return df


def check_inventory(inputpathlist, outputpath, patient_ids, device, pilot):
    """
    Checks the inventory of patients and protocols from the DICOM files and outputs the result as an Excel file.

    Args:
        inputpathlist (list): List of folder paths containing DICOM files.
        outputpath (str): The output path where the Excel file will be saved.
        patient_ids (list): List of patient IDs to create the template dictionary.
        device (str): The device type used to define the categories.
        pilot (str): The name of the pilot or project to be included in the output file name.

    Returns:
        None: The function saves the inventory as an Excel file.
    """

    current_time = datetime.now()
    time = (
        current_time.strftime("%Y-%m-%d %H:%M%S")
        .replace(" ", "_")
        .replace(":", "_")
        .replace("-", "_")
    )

    patient_dict = output_make_template_dic_given_ids(patient_ids, device)
    patient_info = output_get_file_info(inputpathlist, device)

    # for i in tqdm(range(len(patient_info))):
    for i in tqdm(range(len(patient_info)), desc="Processing patients"):
        patient_id = patient_info[i][0]

        protocol_laterality = patient_info[i][1]

        if patient_id in patient_dict.keys():
            if protocol_laterality in patient_dict[patient_id]:
                patient_dict[patient_id][protocol_laterality] = "O"

    df = pd.DataFrame.from_dict(patient_dict, orient="index")
    df.to_excel(f"{outputpath}/{device}_inventory_{time}_{pilot}.xlsx", index=True)


def octa_uploaded_make_template_dic_given_ids(file, device):
    """
    Generates a dictionary template for a given device and list of unique study IDs extracted from a file, specifically for OCT-A images.

    Args:
        file (str): Path to the CSV file containing patient data.
        device (str): The device type (e.g., "Cirrus", "Maestro2", "Triton", etc.) which determines the categories.

    Returns:
        dict: A dictionary where the keys are patient IDs and the values are dictionaries with empty strings for each category and batch folder based on the device type.
    """

    # Reading the CSV file
    df = pd.read_csv(file, skiprows=2, header=None)

    # Extracting unique values from the "Participant Study ID" column
    # unique_study_ids = df["Participant Study ID"].unique()
    unique_study_ids = df[0].unique()

    # Displaying the unique study IDs
    print(len(unique_study_ids))

    if device == "Cirrus":
        # Categories
        categories = [
            "MAC-MacularCube512x128-R",
            "MAC-MacularCube512x128-L",
            "ONH-OpticDiscCube200x200-R",
            "ONH-OpticDiscCube200x200-L",
            "MAC-Angiography6x6mm-R",
            "MAC-Angiography6x6mm-L",
            "ONH-Angiography6x6mm-R",
            "ONH-Angiography6x6mm-L",
            "batch_foldername",
        ]

    elif device == "Maestro2":
        # Categories
        categories = [
            "3DH_Wide_512_128_12x9mm-R",
            "3DH_Wide_512_128_12x9mm-L",
            "3DH_Macula_512_128_6x6mm-R",
            "3DH_Macula_512_128_6x6mm-L",
            "3DH_Macula_360_360_6x6mm-R",
            "3DH_Macula_360_360_6x6mm-L",
            "batch_foldername",
        ]

    elif device == "Triton":
        # Categories
        categories = [
            "3DH_Wide_512_256_12x9mm-R",
            "3DH_Wide_512_256_12x9mm-L",
            "3DH_Macula_320_320_6x6mm-R",
            "3DH_Macula_320_320_6x6mm-L",
            "3DH_Macula_512_512_12x12mm-R",
            "3DH_Macula_512_512_12x12mm-L",
            "batch_foldername",
        ]

    elif device == "Spectralis":
        # Categories
        categories = [
            "spectralis_onh_rc_hr_oct-R",
            "spectralis_onh_rc_hr_oct-L",
            "spectralis_ppol_mac_hr_oct-R",
            "spectralis_ppol_mac_hr_oct-L",
            "spectralis_mac_20x20_hs_octa_oct-R",
            "spectralis_mac_20x20_hs_octa_oct-L",
            "batch_foldername",
        ]

    # Creating the dictionary
    patient_dict = {
        str(patient_id): {category: "" for category in categories}
        for patient_id in unique_study_ids
    }

    return patient_dict


def octa_uploaded_fill_table(patient_dict, patient_info):
    """
    Fills the OCT-A template dictionary with 'O' for the protocols found in the patient information and adds batch folder names. Converts it into a DataFrame.

    Args:
        patient_dict (dict): The dictionary template for patients and protocols.
        patient_info (list): A list of patient IDs, protocol laterality pairs, and batch folder names.

    Returns:
        pandas.DataFrame: A DataFrame where rows are patient IDs and columns represent the presence ('O') of each protocol and the batch folder name.
    """
    for i in range(len(patient_info)):
        patient_id = patient_info[i][0]

        protocol_laterality = patient_info[i][1]

        batch_foldername = patient_info[i][2]

        if patient_id in patient_dict.keys():
            if protocol_laterality in patient_dict[patient_id]:
                patient_dict[patient_id][protocol_laterality] = "O"

            patient_dict[patient_id]["batch_foldername"] = batch_foldername

    df = pd.DataFrame.from_dict(patient_dict, orient="index")

    return df


# ignore related
def copy_dcm_files_from_multiple_sources(source_folders, destination_folder):
    """
    Copies all `.dcm` files (that do not start with a dot) from multiple source directories
    and their subdirectories to a specified destination folder.

    Parameters:
    source_folders (list): A list of source directory paths from which `.dcm` files need to be copied.
    destination_folder (str): The destination directory path where `.dcm` files will be copied.

    The function will create the destination directory if it does not already exist. It only copies
    files with the `.dcm` extension that do not start with a '.' in their filename.

    Example:
    copy_dcm_files_from_multiple_sources(['/path/to/source1', '/path/to/source2'], '/path/to/destination')
    """
    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder)

    # Iterate through each source folder in the list
    for source_folder in source_folders:
        # Traverse through all directories and subdirectories of each source folder
        for root, dirs, files in os.walk(source_folder):
            for filename in files:
                # Construct full file path
                file_path = os.path.join(root, filename)

                # Check if it's a file (not a directory), doesn't start with '.' and ends with '.dcm'
                if (
                    os.path.isfile(file_path)
                    and not filename.startswith(".")
                    and filename.endswith(".dcm")
                ):
                    # Copy file to destination folder (keeping file name intact)
                    shutil.copy(file_path, os.path.join(destination_folder, filename))
                    print(f"Copied: {file_path} to {destination_folder}")


def save_ignore_file_names_to_txt(folder_path, output_path):
    """
    Save the base names of all files in the specified folder to a text file,
    with each base file name on a new line.

    Args:
    - folder_path (str): Path to the folder containing files.
    - output_file (str): Path to the output text file where the base file names will be saved.
    """
    current_time = datetime.now()
    time = (
        current_time.strftime("%Y-%m-%d %H:%M%S")
        .replace(" ", "_")
        .replace(":", "_")
        .replace("-", "_")
    )
    # Open the output file in write mode
    with open(
        f"{output_path}/ignore_post_processing_{time}.txt", "w", encoding="utf-8"
    ) as txt_file:
        # Loop through all files in the folder
        for file_name in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file_name)

            # Only process if it's a file (not a directory)
            if os.path.isfile(file_path):
                # Extract the base file name (without extension)
                base_file_name = os.path.splitext(file_name)[0]

                if base_file_name.startswith("."):
                    continue

                # If the base file name ends with '.dcm', remove '.dcm' from the base file name
                elif base_file_name.endswith(".dcm"):
                    base_file_name = base_file_name[:-4]  # Remove '.dcm'

                # Write the base file name to the output text file
                txt_file.write(base_file_name + "\n")
