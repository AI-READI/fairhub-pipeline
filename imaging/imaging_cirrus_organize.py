import os
import imaging.imaging_utils as imaging_utils
import imaging.imaging_classifying_rules as imaging_classifying_rules
import shutil
import pydicom


def filter_cirrus_files(folder, output):
    """
    Filter and process Cirrus files based on classification rules.

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

    if filtered_list:
        if all_dicom:

            check = imaging_utils.check_critical_info_from_files_in_folder(folder)

            if check == "pass":

                expected_status = imaging_utils.cirrus_check_files_expected(folder)

                if expected_status == "Unknown":

                    protocol = "unknown_protocol"
                    for root, dirs, files in os.walk(folder):
                        for file in files:
                            if file[0].isalpha():
                                file_path = os.path.join(root, file)
                                original_folder_basename = os.path.basename(
                                    os.path.dirname(file_path)
                                )
                                data = pydicom.dcmread(file_path)
                                laterality = data.ImageLaterality
                                patientid = data.PatientID

                                outputfolder = f"{output}/{protocol}/{protocol}_{patientid}_{laterality}_{file}"
                                os.makedirs(
                                    f"{output}/{protocol}/{protocol}_{patientid}_{laterality}_{original_folder_basename}",
                                    exist_ok=True,
                                )
                                shutil.copytree(
                                    folder,
                                    f"{output}/{protocol}/{protocol}_{patientid}_{laterality}_{original_folder_basename}",
                                    dirs_exist_ok=True,
                                )

                elif expected_status == "Expected":

                    for root, dirs, files in os.walk(folder):
                        first_file = files[0]
                        if "350x350" in first_file:
                            angio_files = [f for f in files if "Angio" in f]
                            if angio_files:
                                for root, dirs, files in os.walk(folder):
                                    for file in files:
                                        if file[0].isalpha():
                                            file_path = os.path.join(root, file)
                                            original_folder_basename = os.path.basename(
                                                os.path.dirname(file_path)
                                            )
                                            data = pydicom.dcmread(file_path)
                                            laterality = data.ImageLaterality
                                            patientid = data.PatientID
                                            protocol = (
                                                "cirrus"
                                                + "_"
                                                + str(data.ProtocolName)
                                                .lower()[:-7]
                                                .replace("-", "_")
                                                .replace(" ", "_")
                                                .removesuffix("_")
                                            )

                                            # Define the output folder
                                            outputfolder = f"{output}/{protocol}/{protocol}_{patientid}_{laterality}_{original_folder_basename}"

                                            # Ensure the output folder exists
                                            os.makedirs(outputfolder, exist_ok=True)

                                            # Create the destination path with just the filename, not the full path again
                                            new_filename = f"{protocol}_{patientid}_{laterality}_{file}"
                                            if (
                                                "StructuralEnface" in new_filename
                                                or (
                                                    "512x128" in new_filename
                                                    and "Seg.dcm" in new_filename
                                                )
                                                or (
                                                    "200x200" in new_filename
                                                    and "Seg.dcm" in new_filename
                                                )
                                            ):
                                                continue  # Skip this file and move to the next one

                                            destination = os.path.join(
                                                outputfolder, new_filename
                                            )

                                            # Copy the file
                                            shutil.copy2(file_path, destination)

                            else:
                                for root, dirs, files in os.walk(folder):
                                    for file in files:
                                        if file[0].isalpha():
                                            file_path = os.path.join(root, file)
                                            original_folder_basename = os.path.basename(
                                                os.path.dirname(file_path)
                                            )
                                            data = pydicom.dcmread(file_path)
                                            laterality = data.ImageLaterality
                                            patientid = data.PatientID
                                            protocol = "wrong_angio_protocol"

                                            # Define the output folder
                                            outputfolder = f"{output}/{protocol}/{protocol}_{patientid}_{laterality}_{original_folder_basename}"

                                            # Ensure the output folder exists
                                            os.makedirs(outputfolder, exist_ok=True)

                                            # Create the destination path with just the filename, not the full path again
                                            new_filename = f"{protocol}_{patientid}_{laterality}_{file}"
                                            if (
                                                "StructuralEnface" in new_filename
                                                or (
                                                    "512x128" in new_filename
                                                    and "Seg.dcm" in new_filename
                                                )
                                                or (
                                                    "200x200" in new_filename
                                                    and "Seg.dcm" in new_filename
                                                )
                                            ):
                                                continue
                                            destination = os.path.join(
                                                outputfolder, new_filename
                                            )

                                            # Copy the file
                                            shutil.copy2(file_path, destination)
                        else:
                            for root, dirs, files in os.walk(folder):
                                for file in files:
                                    if file[0].isalpha():
                                        file_path = os.path.join(root, file)
                                        original_folder_basename = os.path.basename(
                                            os.path.dirname(file_path)
                                        )
                                        data = pydicom.dcmread(file_path)
                                        laterality = data.ImageLaterality
                                        patientid = data.PatientID
                                        protocol = (
                                            "cirrus"
                                            + "_"
                                            + str(data.ProtocolName)
                                            .lower()[:-7]
                                            .replace("-", "_")
                                            .replace(" ", "_")
                                            .removesuffix("_")
                                        )

                                        # Define the output folder
                                        outputfolder = f"{output}/{protocol}/{protocol}_{patientid}_{laterality}_{original_folder_basename}"

                                        # Ensure the output folder exists
                                        os.makedirs(outputfolder, exist_ok=True)

                                        # Create the destination path with just the filename, not the full path again
                                        new_filename = f"{protocol}_{patientid}_{laterality}_{file}"
                                        if (
                                            "StructuralEnface" in new_filename
                                            or (
                                                "512x128" in new_filename
                                                and "Seg.dcm" in new_filename
                                            )
                                            or (
                                                "200x200" in new_filename
                                                and "Seg.dcm" in new_filename
                                            )
                                        ):
                                            continue
                                        destination = os.path.join(
                                            outputfolder, new_filename
                                        )

                                        # Copy the file
                                        shutil.copy2(file_path, destination)

            else:

                protocol = f"{check}"

                outputfolder = f"{output}/{protocol}"

                a = pydicom.dcmread(filtered_list[0])
                patientid = a.PatientID if hasattr(a, "PatientID") else "N/A"
                laterality = (
                    a.ImageLaterality if hasattr(a, "ImageLaterality") else "N/A"
                )

                # Ensure the output folder exists
                os.makedirs(outputfolder, exist_ok=True)

                # Construct the source folder path
                source_folder = f"{outputfolder}/{protocol}_{folder.split('/')[-1]}"
                os.makedirs(source_folder, exist_ok=True)

                # Copy the entire folder to the output directory
                shutil.copytree(source_folder, outputfolder, dirs_exist_ok=True)

            dic = {
                "Rule": protocol,
                "Patient ID": patientid,
                "Laterality": laterality,
                "Input": folder,
                "Output": outputfolder,
            }

        else:

            protocol = "invalid_dicom"

            outputfolder = f"{output}/{protocol}"

            # Ensure the output folder exists
            os.makedirs(outputfolder, exist_ok=True)

            # Construct the source folder path
            source_folder = f"{outputfolder}/{protocol}_{folder.split('/')[-1]}"
            os.makedirs(source_folder, exist_ok=True)

            # Copy the entire folder to the output directory
            shutil.copytree(source_folder, outputfolder, dirs_exist_ok=True)

            dic = {
                "Rule": protocol,
                "Patient ID": "N/A",
                "Laterality": "N/A",
                "Input": folder,
                "Output": outputfolder,
            }

    else:

        protocol = "no_files"
        outputfolder = f"{output}/{protocol}"

        # Ensure the output folder exists
        os.makedirs(outputfolder, exist_ok=True)

        # Construct the source folder path
        source_folder = f"{outputfolder}/{protocol}_{folder.split('/')[-1]}"
        os.makedirs(source_folder, exist_ok=True)

        # Copy the entire folder to the output directory
        shutil.copytree(source_folder, outputfolder, dirs_exist_ok=True)

        dic = {
            "Rule": protocol,
            "Patient ID": "N/A",
            "Laterality": "N/A",
            "Input": folder,
            "Output": outputfolder,
        }

    return dic
