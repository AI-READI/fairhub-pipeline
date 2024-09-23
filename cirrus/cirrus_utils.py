import os
import pydicom
import shutil
import imaging.imaging_utils as imaging_utils


device_folder_mapping = {
    "cirrus": "zeiss_cirrus",
}

cirrus_submodality_mapping = {
    "LSO": "ir",
    "Struc.": "oct",
    "Flow.": "flow_cube",
    "ProjectionRemoved": "enface_projection_removed",
    "AngioEnface.": "enface",
    "StructuralEnface": "enface_structural",
    "Seg": "segmentation",
}

name_mapping = {
    "mac_angiography": "cirrus_macula_6x6_octa",
    "onh_angiography": "cirrus_disc_6x6_octa",
    "mac_macular_cube_": "cirrus_mac_oct",
    "onh_optic_disc_cube_": "cirrus_disc_oct",
}

cirrus_modality_folder_mapping = {
    "ir_": "retinal_photography",
    "segmentation": "retinal_octa",
    "enface_projection": "retinal_octa",
    "enface_l": "retinal_octa",
    "enface_r": "retinal_octa",
    "_oct_oct_": "retinal_oct",
    "_octa_oct_": "retinal_oct",
    "flow_cube": "retinal_octa",
}

cirrus_submodality_folder_mapping = {
    "ir_": "ir",
    "_oct_oct_": "oct_structural_scan",
    "_octa_oct_": "oct_structural_scan",
    "flow_cube": "flow_cube",
    "segmentation": "segmentation",
    "enface_projection": "enface",
    "enface_l": "enface",
    "enface_r": "enface",
}

modality_folder_mapping = {
    "ir_": "retinal_photography",
    "cfp": "retinal_photography",
    "faf": "retinal_photography",
    "flow_cube": "retinal_octa",
    "segmentation": "retinal_octa",
    "enface": "retinal_octa",
    "_oct_": "retinal_oct",
    "flio": "retinal_flio",
    "Flow.": "retinal_octa",
}

submodality_folder_mapping = {
    "ir_": "ir",
    "cfp": "cfp",
    "faf": "faf",
    "_oct_l": "oct_structural_scan",
    "_oct_r": "oct_structural_scan",
    "flow_cube": "flow_cube",
    "Flow.": "flow_cube",
    "segmentation": "segmentation",
    "enface": "enface",
    "flio": "flio",
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


def format_cirrus_file(file, output):
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

    try:
        dataset = pydicom.dcmread(file)

    except Exception:
        full_dir_path = output + "/invalid_dicom"
        os.makedirs(full_dir_path, exist_ok=True)
        filename = os.path.basename(file)
        full_file_path = os.path.join(full_dir_path, filename)
        shutil.copy(file, full_file_path)
    else:
        # try:
        #     # Check if dataset has pixel data
        #     pixel = dataset.pixel_array
        # except Exception:
        #     # Handle case where pixel_array is not available
        #     full_dir_path = output + "/error_pixel_data/"
        #     os.makedirs(full_dir_path, exist_ok=True)
        #     filename = os.path.basename(file)
        #     full_file_path = os.path.join(full_dir_path, filename)
        #     shutil.copy(file, full_file_path)

        # else:
        id = imaging_utils.find_id(str(dataset.PatientID), str(dataset.PatientName))

        if id == "noid":
            full_dir_path = output + "/missing_critical_info/no_id/"
            os.makedirs(full_dir_path, exist_ok=True)
            filename = os.path.basename(file)
            full_file_path = os.path.join(full_dir_path, filename)
            dataset.save_as(full_file_path)

        else:
            uid = dataset.SOPInstanceUID

            dataset.PatientID = id
            dataset.PatientName = ""
            dataset.PatientSex = "M"
            dataset.PatientBirthDate = ""

            if "cirrus" in file:
                dataset.ProtocolName = dataset.ProtocolName

            else:
                protocol = "unkwnown"
                dataset.ProtocolName = protocol

            laterality = dataset.ImageLaterality.lower()
            patientid = id

            modality = get_description(file, name_mapping)

            submodality = ""

            if "cirrus" in file:
                submodality = get_description(file, cirrus_submodality_mapping)
                # n = f"{n}"
                submodality = f"{submodality}"

                filename = f"{id}_{modality}_{submodality}_{laterality}_{uid}.dcm"

            else:
                filename = f"{id}_{modality}_{laterality}_{uid}.dcm"

            if "cirrus" in filename:
                modality_folder = get_description(
                    filename, cirrus_modality_folder_mapping
                )
                submodality_folder = get_description(
                    filename, cirrus_submodality_folder_mapping
                )
                device_folder = get_description(filename, device_folder_mapping)
            else:
                modality_folder = get_description(filename, modality_folder_mapping)
                submodality_folder = get_description(
                    filename, submodality_folder_mapping
                )
                device_folder = get_description(filename, device_folder_mapping)

            folderpath = (
                f"/{modality_folder}/{submodality_folder}/{device_folder}/{id}/"
            )

            full_dir_path = output + folderpath

            os.makedirs(full_dir_path, exist_ok=True)

            full_file_path = os.path.join(full_dir_path, filename)

            dataset.save_as(full_file_path)

            return full_file_path
