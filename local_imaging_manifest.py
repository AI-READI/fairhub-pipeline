"""
Local Imaging Manifest Pipeline

Generate imaging manifests from local DICOM files without requiring pre-generated metadata.
This script reads DICOM files directly from a local folder and extracts the necessary
metadata to create manifests in the same format as the Azure-based pipeline.

"""

import os
import pydicom
import json
import logging
from tqdm import tqdm
import pandas as pd

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

Tag = pydicom.tag.Tag


def get_retinal_photography_path(file):
    parts = file.split(os.sep)

    # Find the index where 'retinal_photography' appears
    for i, part in enumerate(parts):
        if "retinal_photography" in part:
            new_path = "/".join(parts[i:])
            break

    return new_path


def get_retinal_oct_path(file):
    parts = file.split(os.sep)

    # Find the index where 'retinal_photography' appears
    for i, part in enumerate(parts):
        if "retinal_oct" in part:
            new_path = "/".join(parts[i:])
            break

    return new_path


def get_retinal_octa_path(file):
    parts = file.split(os.sep)

    # Find the index where 'retinal_photography' appears
    for i, part in enumerate(parts):
        if "retinal_octa" in part:
            new_path = "/".join(parts[i:])
            break

    return new_path


def get_dcm_files(folder_path):
    """
    Recursively find all DICOM files (.dcm) in a folder structure.

    Args:
        folder_path (str): Path to the root folder to search

    Returns:
        list: List of full paths to all DICOM files found
    """
    dcm_files = []
    logger.info(f"Scanning for DICOM files in: {folder_path}")

    # Walk through all directories and subdirectories
    for root, _, files in tqdm(os.walk(folder_path), desc="Scanning directories"):
        for file in files:
            # exclude hidden/system files and ensure .dcm extension
            if not file.startswith(".") and file.lower().endswith(".dcm"):
                dcm_files.append(os.path.join(root, file))

    logger.info(f"Found {len(dcm_files)} DICOM files")
    return dcm_files


def get_json_flow_files(root_folder):
    """
    Walk through a folder and collect all JSON files that:
      - end with '.json'
      - do NOT start with '.'
      - include 'flow' in the filename

    Args:
        root_folder (str): Path to the root folder to search

    Returns:
        list: List of full paths to matching JSON files
    """
    matches = []

    for root, _, files in tqdm(os.walk(root_folder), desc="Scanning for flow files"):
        for file in files:
            if (
                file.endswith(".json")
                and not file.startswith(".")
                and "flow" in file.lower()
            ):
                matches.append(os.path.join(root, file))

    logger.info(f"Found {len(matches)} flow JSON files")
    return matches


def get_json_enface_files(root_folder):
    """
    Walk through a folder and collect all JSON files that:
      - end with '.json'
      - do NOT start with '.'
      - include 'enface' in the filename

    Args:
        root_folder (str): Path to the root folder to search

    Returns:
        list: List of full paths to matching JSON files
    """
    matches = []
    logger.info(f"Searching for enface JSON files in: {root_folder}")

    for root, _, files in tqdm(os.walk(root_folder), desc="Scanning for enface files"):
        for file in files:
            if (
                file.endswith(".json")
                and not file.startswith(".")
                and "enface" in file.lower()
            ):
                matches.append(os.path.join(root, file))

    return matches


def get_json_segmentation_files(root_folder):
    """
    Walk through a folder and collect all JSON files that:
      - end with '.json'
      - do NOT start with '.'
      - include 'segmentation' in the filename

    Args:
        root_folder (str): Path to the root folder to search

    Returns:
        list: List of full paths to matching JSON files
    """
    matches = []

    for root, _, files in tqdm(
        os.walk(root_folder), desc="Scanning for segmentation files"
    ):
        for file in files:
            if (
                file.endswith(".json")
                and not file.startswith(".")
                and "segmentation" in file.lower()
            ):
                matches.append(os.path.join(root, file))

    return matches


def save_retinal_photography_metadata_in_json(retinal_photography_folder):
    """
    Process retinal photography DICOM files and extract metadata to JSON files.

    This function reads DICOM files from the retinal photography folder, extracts
    relevant metadata, and saves it as individual JSON files in a metadata folder.

    Args:
        retinal_photography_folder (str): Path to folder containing retinal photography DICOM files

    Returns:
        str: Path to the output metadata folder
    """
    logger.info(
        f"Processing retinal photography files from: {retinal_photography_folder}"
    )

    # Get all DICOM files in the folder
    files = get_dcm_files(retinal_photography_folder)

    # Create output folder path by modifying the input path
    parts = retinal_photography_folder.rstrip(os.sep).split(os.sep)
    parts[-2] = parts[-2] + "_metadata"
    output_folder = os.sep.join(parts)

    logger.info(f"Output metadata folder: {output_folder}")

    # Process each DICOM file
    for ir_file in tqdm(files, desc="Processing retinal photography files"):
        try:
            # Read DICOM file and extract metadata
            dicom_data = pydicom.dcmread(ir_file)

            # Create metadata dictionary with standardized fields
            dic = {
                "person_id": dicom_data.PatientID,
                "manufacturer": "Heidelberg",
                "manufacturers_model_name": "Spectralis",
                "laterality": dicom_data.ImageLaterality,
                "anatomic_region": "Macula, 20 x 20",
                "imaging": "Infrared Reflectance",
                "height": dicom_data.Rows,
                "width": dicom_data.Columns,
                "color_channel_dimension": "0",
                "sop_instance_uid": dicom_data.SOPInstanceUID,
                "filepath": get_retinal_photography_path(ir_file),
            }

            # Create filename by replacing dots with underscores
            filename = os.path.basename(ir_file).replace(".", "_")
            json_data = {filename: dic}

            # Ensure output directory exists
            os.makedirs(output_folder, exist_ok=True)

            # Save metadata as JSON file
            json_file_path = os.path.join(output_folder, f"{filename}.json")
            logger.debug(f"Saving metadata to: {json_file_path}")

            with open(json_file_path, "w") as json_file:
                json.dump(json_data, json_file)

        except Exception as e:
            logger.error(f"Error processing file {ir_file}: {str(e)}")
            continue

    logger.info(f"Completed processing {len(files)} retinal photography files")
    return output_folder


def find_filepath_by_uid(tsv_file, uid, uid_col="SOPInstanceUID", path_col="FilePath"):
    """
    Reads a TSV file and returns the 'FilePath' for matching 'SOPInstanceUID'.

    This function searches through a TSV manifest file to find the file path
    associated with a specific SOP Instance UID. It handles column name variations
    and case sensitivity issues.

    Parameters:
        tsv_file (str): path to the TSV file
        uid (str): UID to match
        uid_col (str): column name for UID (default: "SOPInstanceUID")
        path_col (str): column name for file path (default: "FilePath")

    Returns:
        str: matching file path for the given UID

    Raises:
        KeyError: if required columns are not found in the TSV file
    """
    logger.debug(f"Looking up UID {uid} in {tsv_file}")

    # Read TSV file with string data type to preserve UIDs
    df = pd.read_csv(tsv_file, sep="\t", dtype=str).fillna("")

    # Normalize column names to avoid case/underscore mismatches
    normalized = {c.lower().replace(" ", "").replace("_", ""): c for c in df.columns}
    n_uid = uid_col.lower().replace(" ", "").replace("_", "")
    n_fp = path_col.lower().replace(" ", "").replace("_", "")

    # Validate that required columns exist
    if n_uid not in normalized:
        raise KeyError(f"UID column '{uid_col}' not found. Columns: {list(df.columns)}")
    if n_fp not in normalized:
        raise KeyError(
            f"File path column '{path_col}' not found. Columns: {list(df.columns)}"
        )

    # Get the actual column names from the dataframe
    real_uid_col = normalized[n_uid]
    real_fp_col = normalized[n_fp]

    # Find matching rows and extract file paths
    matches = df.loc[df[real_uid_col] == uid, real_fp_col].tolist()

    if not matches:
        logger.warning(f"No file path found for UID: {uid}")
        return None

    logger.debug(f"Found file path for UID {uid}: {matches[0]}")
    return matches[0]


def save_retinal_oct_metadata_in_json(
    retinal_oct_folder, retinal_photography_manifest_tsv
):
    """
    Process retinal OCT DICOM files and extract metadata to JSON files.

    This function reads OCT DICOM files, extracts detailed metadata including
    pixel spacing, slice thickness, and reference information, then saves
    the metadata as individual JSON files.

    Args:
        retinal_oct_folder (str): Path to folder containing OCT DICOM files
        retinal_photography_manifest_tsv (str): Path to retinal photography manifest TSV file

    Returns:
        str: Path to the output metadata folder
    """
    logger.info(f"Processing retinal OCT files from: {retinal_oct_folder}")

    # Get all DICOM files in the folder
    files = get_dcm_files(retinal_oct_folder)

    # Create output folder path by modifying the input path
    parts = retinal_oct_folder.rstrip(os.sep).split(os.sep)
    parts[-2] = parts[-2] + "_metadata"
    output_folder = os.sep.join(parts)

    logger.info(f"Output metadata folder: {output_folder}")

    # Process each OCT DICOM file
    for oct_file in tqdm(files, desc="Processing retinal OCT files"):
        try:
            # Read DICOM file
            dicom_data = pydicom.dcmread(oct_file)

            # Extract detailed OCT-specific metadata
            dic = {
                "person_id": dicom_data.PatientID,
                "manufacturer": "Heidelberg",
                "manufacturers_model_name": "Spectralis",
                "anatomic_region": "Macula, 20 x 20",
                "imaging": "OCT",
                "laterality": dicom_data.ImageLaterality,
                "height": dicom_data.Rows,
                "width": dicom_data.Columns,
                "number_of_frames": dicom_data.NumberOfFrames,
                "pixel_spacing": dicom_data[0x52009229][0][0x00289110][0][
                    0x00280030
                ].value,
                "slice_thickness": dicom_data[0x52009229][0][0x00289110][0][
                    0x00180050
                ].value,
                "sop_instance_uid": dicom_data.SOPInstanceUID,
                "filepath": get_retinal_oct_path(oct_file),
                "reference_instance_uid": dicom_data[0x52009229][0][0x00081140][0][
                    0x00081155
                ].value,
                "reference_filepath": find_filepath_by_uid(
                    retinal_photography_manifest_tsv,
                    dicom_data[0x52009229][0][0x00081140][0][0x00081155].value,
                ),
            }

            # Create filename by replacing dots with underscores
            filename = os.path.basename(oct_file).replace(".", "_")
            json_data = {filename: dic}

            # Ensure output directory exists
            os.makedirs(output_folder, exist_ok=True)

            # Save metadata as JSON file
            json_file_path = os.path.join(output_folder, f"{filename}.json")
            logger.debug(f"Saving OCT metadata to: {json_file_path}")

            with open(json_file_path, "w") as json_file:
                json.dump(json_data, json_file, default=str)

        except Exception as e:
            logger.error(f"Error processing OCT file {oct_file}: {str(e)}")
            continue

    logger.info(f"Completed processing {len(files)} retinal OCT files")
    return output_folder


def save_octa_metadata_in_json(retinal_octa_folder):
    """
    Process retinal OCTA DICOM files and extract metadata to JSON files.

    This function handles three types of OCTA DICOM files:
    1. Flow cube files (SOP Class UID: 1.2.840.10008.5.1.4.1.1.77.1.5.8)
    2. En face files (SOP Class UID: 1.2.840.10008.5.1.4.1.1.77.1.5.7)
    3. Segmentation files (SOP Class UID: 1.2.840.10008.5.1.4.1.1.66.8)

    Each type has different metadata requirements and is processed accordingly.

    Args:
        retinal_octa_folder (str): Path to folder containing OCTA DICOM files

    Returns:
        str: Path to the output metadata folder
    """
    logger.info(f"Processing retinal OCTA files from: {retinal_octa_folder}")

    # Get all DICOM files in the folder
    files = get_dcm_files(retinal_octa_folder)

    # Create output folder path by modifying the input path
    parts = retinal_octa_folder.rstrip(os.sep).split(os.sep)
    parts[-2] = parts[-2] + "_metadata"
    output_folder = os.sep.join(parts)

    logger.info(f"Output metadata folder: {output_folder}")

    # Process each OCTA DICOM file
    for f in tqdm(files, desc="Processing retinal OCTA files"):

        try:
            # Read DICOM file to determine type
            dicom_data = pydicom.dcmread(f)

            # Process Flow Cube files (OCTA volume data)
            if dicom_data.SOPClassUID == "1.2.840.10008.5.1.4.1.1.77.1.5.8":
                logger.debug(f"Processing flow cube file: {f}")

                # Extract flow cube specific metadata
                dic = {
                    "participant_id": dicom_data.PatientID,
                    "filepath": get_retinal_octa_path(f),
                    "modality": "octa",
                    "submodality": "flow_cube",
                    "manufacturer": "Heidelberg",
                    "manufacturers_model_name": "Spectralis",
                    "laterality": dicom_data.Laterality,
                    "protocol": dicom_data.ProtocolName,
                    "height": dicom_data.Rows,
                    "width": dicom_data.Columns,
                    "number_of_frames": dicom_data.NumberOfFrames,
                    "sop_instance_uid": dicom_data.SOPInstanceUID,
                    "reference_oct_uid": dicom_data[0x00081115][1][0x0008114A][0][
                        0x00081155
                    ].value,
                    "reference_op_uid": dicom_data[0x00081115][0][0x0008114A][0][
                        0x00081155
                    ].value,
                    "content_time": dicom_data.ContentDate + dicom_data.ContentTime,
                    "sop_class_uid": dicom_data.SOPClassUID,
                }

                # Create filename and save metadata
                filename = os.path.basename(f).replace(".", "_")
                json_data = {filename: dic}
                os.makedirs(output_folder, exist_ok=True)
                json_file_path = os.path.join(output_folder, f"{filename}.json")

                with open(json_file_path, "w") as json_file:
                    json.dump(json_data, json_file)

            # Process En Face files (OCTA en face projections)
            elif dicom_data.SOPClassUID == "1.2.840.10008.5.1.4.1.1.77.1.5.7":
                logger.debug(f"Processing en face file: {f}")

                # Extract en face specific metadata including segmentation surfaces
                dic = {
                    "participant_id": dicom_data.PatientID,
                    "filepath": get_retinal_octa_path(f),
                    "modality": "octa",
                    "submodality": "enface",
                    "manufacturer": "Heidelberg",
                    "manufacturers_model_name": "Spectralis",
                    "laterality": dicom_data.ImageLaterality,
                    "protocol": dicom_data.ProtocolName,
                    "height": dicom_data.Rows,
                    "width": dicom_data.Columns,
                    "sop_instance_uid": dicom_data.SOPInstanceUID,
                    "ophthalmic_image_type": dicom_data[0x00221615][0][
                        0x00080104
                    ].value,
                    "En Face Retinal Segmentation Surface 1": str(
                        dicom_data["00221627"][0]["0008114C"][0]["0062000F"][0][
                            "00080104"
                        ].value
                    ).lower(),
                    "En Face Retinal Segmentation Surface 2": str(
                        dicom_data["00221627"][1]["0008114C"][0]["0062000F"][0][
                            "00080104"
                        ].value
                    ).lower(),
                    "content_time": dicom_data.ContentDate + dicom_data.ContentTime,
                    "sop_class_uid": dicom_data.SOPClassUID,
                    "oct_reference_instance_uid": dicom_data[0x00081115][1][0x0008114A][
                        0
                    ][0x00081155].value,
                    "op_reference_instance_uid": dicom_data[0x00081115][0][0x0008114A][
                        0
                    ][0x00081155].value,
                    "vol_reference_instance_uid": dicom_data[0x00081115][2][0x0008114A][
                        0
                    ][0x00081155].value,
                    "seg_reference_instance_uid": dicom_data[0x00081115][3][0x0008114A][
                        0
                    ][0x00081155].value,
                }

                # Create filename and save metadata
                filename = os.path.basename(f).replace(".", "_")
                json_data = {filename: dic}
                os.makedirs(output_folder, exist_ok=True)
                json_file_path = os.path.join(output_folder, f"{filename}.json")

                with open(json_file_path, "w") as json_file:
                    json.dump(json_data, json_file)

            # Process Segmentation files (OCTA segmentation data)
            elif dicom_data.SOPClassUID == "1.2.840.10008.5.1.4.1.1.66.8":
                logger.debug(f"Processing segmentation file: {f}")

                # Extract algorithm information to determine segmentation type
                raw_alg = (
                    dicom_data[0x00620002][0][0x00620009].value
                    if 0x00620002 in dicom_data
                    else ""
                )
                alg_name = (str(raw_alg) or "").lower()

                # Determine if segmentation is smoothed or not based on algorithm name
                segmentation_algorithm_type = (
                    "smoothed" if "smoothed" in alg_name else "not smoothed"
                )

                # Extract segmentation specific metadata
                dic = {
                    "participant_id": dicom_data.PatientID,
                    "filepath": get_retinal_octa_path(f),
                    "modality": "octa",
                    "submodality": "segmentation",
                    "manufacturer": "Heidelberg",
                    "manufacturers_model_name": "Spectralis",
                    "laterality": dicom_data.Laterality,
                    "protocol": dicom_data.ProtocolName,
                    "height": dicom_data.Rows,
                    "width": dicom_data.Columns,
                    "number_of_frames": dicom_data.NumberOfFrames,
                    "sop_instance_uid": dicom_data.SOPInstanceUID,
                    "oct_reference_instance_uid": dicom_data[0x00081115][1][0x0008114A][
                        0
                    ][0x00081155].value,
                    "segmentation_type": "Heightmap",
                    "segmentation_algorithm_type": segmentation_algorithm_type,
                    "content_time": dicom_data.ContentDate + dicom_data.ContentTime,
                    "sop_class_uid": dicom_data.SOPClassUID,
                }

                # Create filename and save metadata
                filename = os.path.basename(f).replace(".", "_")
                json_data = {filename: dic}
                os.makedirs(output_folder, exist_ok=True)
                json_file_path = os.path.join(output_folder, f"{filename}.json")

                with open(json_file_path, "w") as json_file:
                    json.dump(json_data, json_file)

        except Exception as e:
            logger.error(f"Error processing OCTA file {f}: {str(e)}")
            continue

    logger.info(f"Completed processing {len(files)} retinal OCTA files")
    return output_folder


def make_octa_manifest(
    folder, retinal_photography_manifest_tsv, retinal_oct_manifest_tsv, output_folder
):
    """
    Create a comprehensive OCTA manifest by correlating flow, enface, and segmentation files.

    This function processes OCTA metadata files and creates a unified manifest that
    links flow cube data with associated enface projections and segmentation data.
    It handles multiple enface types (superficial, deep, avascular complex) and
    different segmentation algorithms (smoothed/not smoothed).

    Args:
        folder (str): Path to folder containing OCTA metadata JSON files
        retinal_photography_manifest_tsv (str): Path to retinal photography manifest
        retinal_oct_manifest_tsv (str): Path to retinal OCT manifest
        output_folder (str): Path to folder where manifest will be saved

    Returns:
        str: Path to the created OCTA manifest TSV file
    """
    logger.info(f"Creating OCTA manifest from folder: {folder}")

    # Define column headers for the OCTA manifest
    column_headers = [
        "person_id",
        "manufacturer",
        "manufacturers_model_name",
        "anatomic_region",
        "imaging",
        "laterality",
        "flow_cube_height",
        "flow_cube_width",
        "flow_cube_number_of_frames",
        "associated_segmentation_type",
        "associated_segmentation_number_of_frames",
        "associated_enface_1_ophthalmic_image_type",
        "associated_enface_1_segmentation_surface_1",
        "associated_enface_1_segmentation_surface_2",
        "associated_enface_2_ophthalmic_image_type",
        "associated_enface_2_segmentation_surface_1",
        "associated_enface_2_segmentation_surface_2",
        "associated_enface_3_ophthalmic_image_type",
        "associated_enface_3_segmentation_surface_1",
        "associated_enface_3_segmentation_surface_2",
        "associated_enface_4_ophthalmic_image_type",
        "associated_enface_4_segmentation_surface_1",
        "associated_enface_4_segmentation_surface_2",
        "flow_cube_sop_instance_uid",
        "flow_cube_file_path",
        "associated_retinal_photography_sop_instance_uid",
        "associated_retinal_photography_file_path",
        "associated_structural_oct_sop_instance_uid",
        "associated_structural_oct_file_path",
        "associated_segmentation_sop_instance_uid",
        "associated_segmentation_file_path",
        "variant_segmentation_sop_instance_uid",
        "variant_segmentation_file_path",
        "associated_enface_1_sop_instance_uid",
        "associated_enface_1_file_path",
        "associated_enface_2_sop_instance_uid",
        "associated_enface_2_file_path",
        "associated_enface_2_projection_removed_sop_instance_uid",
        "associated_enface_2_projection_removed_filepath",
        "associated_enface_3_sop_instance_uid",
        "associated_enface_3_file_path",
        "associated_enface_3_projection_removed_sop_instance_uid",
        "associated_enface_3_projection_removed_filepath",
        "associated_enface_4_sop_instance_uid",
        "associated_enface_4_file_path",
        "associated_enface_4_projection_removed_sop_instance_uid",
        "associated_enface_4_projection_removed_filepath",
    ]

    # Get all flow JSON files (OCTA volume data)
    flow_json_files = get_json_flow_files(folder)
    logger.info(f"Found {len(flow_json_files)} flow files to process")

    # List to collect all manifest rows
    manifest_rows = []

    # Process each flow file and create manifest entries
    for flow in tqdm(flow_json_files, desc="Creating OCTA manifest entries"):

        try:
            # Load flow cube metadata
            with open(flow, "r") as f:
                data = json.load(f)
                flow_metadata = next(iter(data.values()))

                # Extract basic flow cube information
                person_id = flow_metadata["participant_id"]
                manufacturer = flow_metadata["manufacturer"]
                manufacturers_model_name = flow_metadata["manufacturers_model_name"]
                anatomic_region = "Macula, 20 x 20"
                imaging = "OCTA"
                laterality = flow_metadata["laterality"]

                # Extract flow cube specific data
                flow_cube_sop_instance_uid = flow_metadata["sop_instance_uid"]
                flow_cube_height = flow_metadata["height"]
                flow_cube_width = flow_metadata["width"]
                flow_cube_number_of_frames = flow_metadata["number_of_frames"]
                flow_cube_file_path = flow_metadata["filepath"]

                # Extract reference UIDs for associated files
                associated_retinal_photography_sop_instance_uid = flow_metadata[
                    "reference_op_uid"
                ]
                associated_structural_oct_sop_instance_uid = flow_metadata[
                    "reference_oct_uid"
                ]

            # Find associated enface files that reference this flow cube
            enface_json_files = get_json_enface_files(folder)
            matching_enface_files = []

            logger.debug(
                f"Searching for enface files matching flow cube UID: {flow_cube_sop_instance_uid}"
            )

            for enface_file in enface_json_files:
                with open(enface_file, "r") as f:
                    data = json.load(f)
                    enface_metadata = next(iter(data.values()), {})
                    # Check if this enface file references the current flow cube
                    if (
                        enface_metadata.get("vol_reference_instance_uid")
                        == flow_cube_sop_instance_uid
                    ):
                        matching_enface_files.append(enface_file)

            # Process matching enface files and categorize by type
            for matching_enface in matching_enface_files:
                with open(matching_enface, "r") as enface:
                    data = json.load(enface)
                    enface_metadata = next(iter(data.values()))
                    ophthalmic_image_type = enface_metadata["ophthalmic_image_type"]

                    # Process Superficial retina vasculature flow (Enface 1)
                    if ophthalmic_image_type == "Superficial retina vasculature flow":
                        logger.debug(
                            f"Processing superficial enface: {matching_enface}"
                        )

                        associated_enface_1_ophthalmic_image_type = enface_metadata[
                            "ophthalmic_image_type"
                        ]
                        associated_enface_1_segmentation_surface_1 = enface_metadata[
                            "En Face Retinal Segmentation Surface 1"
                        ]
                        associated_enface_1_segmentation_surface_2 = enface_metadata[
                            "En Face Retinal Segmentation Surface 2"
                        ]
                        associated_enface_1_sop_instance_uid = enface_metadata[
                            "sop_instance_uid"
                        ]
                        associated_enface_1_file_path = enface_metadata["filepath"]

                        logger.debug(
                            f"Superficial enface type: {associated_enface_1_ophthalmic_image_type}"
                        )
                        logger.debug(
                            f"Segmentation surfaces: {associated_enface_1_segmentation_surface_1}, {associated_enface_1_segmentation_surface_2}"
                        )
                        logger.debug(f"SOP UID: {associated_enface_1_sop_instance_uid}")
                        logger.debug(f"File path: {associated_enface_1_file_path}")

                    # Process Deep retina vasculature flow (Enface 2)
                    elif ophthalmic_image_type == "Deep retina vasculature flow":
                        logger.debug(f"Processing deep enface: {matching_enface}")

                        associated_enface_2_ophthalmic_image_type = enface_metadata[
                            "ophthalmic_image_type"
                        ]
                        associated_enface_2_segmentation_surface_1 = enface_metadata[
                            "En Face Retinal Segmentation Surface 1"
                        ]
                        associated_enface_2_segmentation_surface_2 = enface_metadata[
                            "En Face Retinal Segmentation Surface 2"
                        ]
                        associated_enface_2_sop_instance_uid = enface_metadata[
                            "sop_instance_uid"
                        ]
                        associated_enface_2_file_path = enface_metadata["filepath"]
                        # Projection removed files are not available for deep vasculature
                        associated_enface_2_projection_removed_sop_instance_uid = (
                            "Not Reported"
                        )
                        associated_enface_2_projection_removed_filepath = "Not Reported"

                        logger.debug(
                            f"Deep enface type: {associated_enface_2_ophthalmic_image_type}"
                        )
                        logger.debug(
                            f"Segmentation surfaces: {associated_enface_2_segmentation_surface_1}, {associated_enface_2_segmentation_surface_2}"
                        )
                        logger.debug(f"SOP UID: {associated_enface_2_sop_instance_uid}")
                        logger.debug(f"File path: {associated_enface_2_file_path}")
                        logger.debug(
                            f"Projection removed UID: {associated_enface_2_projection_removed_sop_instance_uid}"
                        )
                        logger.debug(
                            f"Projection removed path: {associated_enface_2_projection_removed_filepath}"
                        )

                    # Process Avascular complex flow (Enface 4)
                    elif ophthalmic_image_type == "Avascular complex flow":
                        logger.debug(
                            f"Processing avascular complex enface: {matching_enface}"
                        )

                        associated_enface_4_ophthalmic_image_type = enface_metadata[
                            "ophthalmic_image_type"
                        ]
                        associated_enface_4_segmentation_surface_1 = enface_metadata[
                            "En Face Retinal Segmentation Surface 1"
                        ]
                        associated_enface_4_segmentation_surface_2 = enface_metadata[
                            "En Face Retinal Segmentation Surface 2"
                        ]
                        associated_enface_4_sop_instance_uid = enface_metadata[
                            "sop_instance_uid"
                        ]
                        associated_enface_4_file_path = enface_metadata["filepath"]
                        # Projection removed files are not available for avascular complex
                        associated_enface_4_projection_removed_sop_instance_uid = (
                            "Not Reported"
                        )
                        associated_enface_4_projection_removed_filepath = "Not Reported"

                        logger.debug(
                            f"Avascular complex enface type: {associated_enface_4_ophthalmic_image_type}"
                        )
                        logger.debug(
                            f"Segmentation surfaces: {associated_enface_4_segmentation_surface_1}, {associated_enface_4_segmentation_surface_2}"
                        )
                        logger.debug(f"SOP UID: {associated_enface_4_sop_instance_uid}")
                        logger.debug(f"File path: {associated_enface_4_file_path}")
                        logger.debug(
                            f"Projection removed UID: {associated_enface_4_projection_removed_sop_instance_uid}"
                        )
                        logger.debug(
                            f"Projection removed path: {associated_enface_4_projection_removed_filepath}"
                        )

            # Find associated segmentation files that reference the structural OCT
            segmentation_json_files = get_json_segmentation_files(folder)
            matching_segmentation_files = []

            logger.debug(
                f"Searching for segmentation files matching OCT UID: {associated_structural_oct_sop_instance_uid}"
            )

            for segmentation_file in segmentation_json_files:
                with open(segmentation_file, "r") as f:
                    data = json.load(f)
                    seg_metadata = next(iter(data.values()), {})
                    # Check if this segmentation file references the structural OCT
                    if (
                        seg_metadata.get("oct_reference_instance_uid")
                        == associated_structural_oct_sop_instance_uid
                    ):
                        matching_segmentation_files.append(segmentation_file)

            # Process matching segmentation files and categorize by algorithm type
            for matching_segmentation in matching_segmentation_files:
                with open(matching_segmentation, "r") as segmentation:
                    data = json.load(segmentation)
                    seg_metadata = next(iter(data.values()))
                    algorithm_type = seg_metadata["segmentation_algorithm_type"]

                    # Process smoothed segmentation (primary segmentation)
                    if algorithm_type == "smoothed":
                        logger.debug(
                            f"Processing smoothed segmentation: {matching_segmentation}"
                        )

                        associated_segmentation_type = "Heightmap"
                        associated_segmentation_number_of_frames = seg_metadata[
                            "number_of_frames"
                        ]
                        associated_segmentation_sop_instance_uid = seg_metadata[
                            "sop_instance_uid"
                        ]
                        associated_segmentation_file_path = seg_metadata["filepath"]

                        logger.debug(
                            f"Segmentation frames: {associated_segmentation_number_of_frames}"
                        )
                        logger.debug(
                            f"Segmentation UID: {associated_segmentation_sop_instance_uid}"
                        )
                        logger.debug(
                            f"Segmentation path: {associated_segmentation_file_path}"
                        )

                    # Process not smoothed segmentation (variant segmentation)
                    elif algorithm_type == "not smoothed":
                        logger.debug(
                            f"Processing not smoothed segmentation: {matching_segmentation}"
                        )

                        variant_segmentation_sop_instance_uid = seg_metadata[
                            "sop_instance_uid"
                        ]
                        variant_segmentation_file_path = seg_metadata["filepath"]

                        logger.debug(
                            f"Variant segmentation UID: {variant_segmentation_sop_instance_uid}"
                        )
                        logger.debug(
                            f"Variant segmentation path: {variant_segmentation_file_path}"
                        )

            # Initialize Enface 3 fields (not used in current implementation)
            associated_enface_3_ophthalmic_image_type = "Not Reported"
            associated_enface_3_segmentation_surface_1 = "Not Reported"
            associated_enface_3_segmentation_surface_2 = "Not Reported"
            associated_enface_3_sop_instance_uid = "Not Reported"
            associated_enface_3_file_path = "Not Reported"
            associated_enface_3_projection_removed_sop_instance_uid = "Not Reported"
            associated_enface_3_projection_removed_filepath = "Not Reported"

            # Look up file paths for associated retinal photography and OCT files
            logger.debug(
                f"Looking up retinal photography file path for UID: {associated_retinal_photography_sop_instance_uid}"
            )
            associated_retinal_photography_file_path = find_filepath_by_uid(
                retinal_photography_manifest_tsv,
                associated_retinal_photography_sop_instance_uid,
            )

            logger.debug(
                f"Looking up OCT file path for UID: {associated_structural_oct_sop_instance_uid}"
            )
            associated_structural_oct_file_path = find_filepath_by_uid(
                retinal_oct_manifest_tsv, associated_structural_oct_sop_instance_uid
            )

            # Create comprehensive manifest row with all associated data
            one_row = [
                person_id,
                manufacturer,
                manufacturers_model_name,
                anatomic_region,
                imaging,
                laterality,
                flow_cube_height,
                flow_cube_width,
                flow_cube_number_of_frames,
                associated_segmentation_type,
                associated_segmentation_number_of_frames,
                associated_enface_1_ophthalmic_image_type,
                associated_enface_1_segmentation_surface_1,
                associated_enface_1_segmentation_surface_2,
                associated_enface_2_ophthalmic_image_type,
                associated_enface_2_segmentation_surface_1,
                associated_enface_2_segmentation_surface_2,
                associated_enface_3_ophthalmic_image_type,
                associated_enface_3_segmentation_surface_1,
                associated_enface_3_segmentation_surface_2,
                associated_enface_4_ophthalmic_image_type,
                associated_enface_4_segmentation_surface_1,
                associated_enface_4_segmentation_surface_2,
                flow_cube_sop_instance_uid,
                flow_cube_file_path,
                associated_retinal_photography_sop_instance_uid,
                associated_retinal_photography_file_path,
                associated_structural_oct_sop_instance_uid,
                associated_structural_oct_file_path,
                associated_segmentation_sop_instance_uid,
                associated_segmentation_file_path,
                variant_segmentation_sop_instance_uid,
                variant_segmentation_file_path,
                associated_enface_1_sop_instance_uid,
                associated_enface_1_file_path,
                associated_enface_2_sop_instance_uid,
                associated_enface_2_file_path,
                associated_enface_2_projection_removed_sop_instance_uid,
                associated_enface_2_projection_removed_filepath,
                associated_enface_3_sop_instance_uid,
                associated_enface_3_file_path,
                associated_enface_3_projection_removed_sop_instance_uid,
                associated_enface_3_projection_removed_filepath,
                associated_enface_4_sop_instance_uid,
                associated_enface_4_file_path,
                associated_enface_4_projection_removed_sop_instance_uid,
                associated_enface_4_projection_removed_filepath,
            ]

            # Add the manifest row to the list
            manifest_rows.append(one_row)

        except Exception as e:
            logger.error(f"Error processing flow file {flow}: {str(e)}")
            continue

    # Create DataFrame from collected rows and save to TSV file
    if manifest_rows:
        logger.info(f"Creating OCTA manifest with {len(manifest_rows)} rows")
        df = pd.DataFrame(manifest_rows, columns=column_headers)

        # Ensure output directory exists
        os.makedirs(output_folder, exist_ok=True)

        # Save manifest to TSV file
        octa_manifest_path = os.path.join(output_folder, "manifest.tsv")
        df.to_csv(octa_manifest_path, sep="\t", index=False)
        logger.info(f"OCTA manifest saved: {octa_manifest_path}")

        return octa_manifest_path
    else:
        logger.warning(
            "No manifest rows created - no OCTA manifest file will be generated"
        )
        return None


def main():
    """
    Main function to process local DICOM files and generate manifests.

    This function orchestrates the entire pipeline:
    1. Processes retinal photography DICOM files
    2. Processes retinal OCT DICOM files
    3. Processes retinal OCTA DICOM files
    4. Creates comprehensive manifests for each modality
    5. Generates a unified OCTA manifest linking all associated files

    The pipeline follows a specific order to ensure reference files are available
    when processing dependent files (OCT references photography, OCTA references both).
    """
    logger.info("Starting local imaging manifest pipeline")

    # Configuration - Update these paths as needed
    input_folder = r"C:\Users\sanjay\Downloads\Spectralis-processed"
    output_manifest_folder = r"C:\Users\sanjay\Downloads\Spectralis-manifests"

    # Validate input folder exists
    if not os.path.exists(input_folder):
        raise FileNotFoundError(f"Input folder {input_folder} does not exist")

    logger.info(f"Processing files from: {input_folder}")
    logger.info(f"Output manifests to: {output_manifest_folder}")

    # Step 1: Process retinal photography files
    logger.info("=" * 50)
    logger.info("STEP 1: Processing retinal photography files")
    logger.info("=" * 50)

    retinal_photography_folder = os.path.join(input_folder, "retinal_photography")
    retinal_photography_metadata_folder = save_retinal_photography_metadata_in_json(
        retinal_photography_folder
    )

    # Create retinal photography manifest
    logger.info("Creating retinal photography manifest...")
    rows = []
    for fname in tqdm(
        os.listdir(retinal_photography_metadata_folder),
        desc="Loading photography metadata",
    ):
        if fname.startswith(".") or not fname.endswith(".json"):
            continue
        fpath = os.path.join(retinal_photography_metadata_folder, fname)
        with open(fpath, "r") as f:
            data = json.load(f)
        for _, meta in data.items():
            rows.append(meta)

    df = pd.DataFrame(rows)
    output_folder = os.path.join(output_manifest_folder, "retinal_photography")
    os.makedirs(output_folder, exist_ok=True)
    retinal_photography_manifest_path = os.path.join(output_folder, "manifest.tsv")
    df.to_csv(retinal_photography_manifest_path, sep="\t", index=False)
    logger.info(
        f"Retinal photography manifest saved: {retinal_photography_manifest_path}"
    )

    # Step 2: Process retinal OCT files (depends on photography manifest)
    logger.info("=" * 50)
    logger.info("STEP 2: Processing retinal OCT files")
    logger.info("=" * 50)

    retinal_oct_folder = os.path.join(input_folder, "retinal_oct")
    retinal_oct_metadata_folder = save_retinal_oct_metadata_in_json(
        retinal_oct_folder, retinal_photography_manifest_path
    )

    # Create retinal OCT manifest
    logger.info("Creating retinal OCT manifest...")
    rows = []
    for fname in tqdm(
        os.listdir(retinal_oct_metadata_folder), desc="Loading OCT metadata"
    ):
        if fname.startswith(".") or not fname.endswith(".json"):
            continue
        fpath = os.path.join(retinal_oct_metadata_folder, fname)
        with open(fpath, "r") as f:
            data = json.load(f)
        for _, meta in data.items():
            rows.append(meta)

    df = pd.DataFrame(rows)
    output_folder = os.path.join(output_manifest_folder, "retinal_oct")
    os.makedirs(output_folder, exist_ok=True)
    retinal_oct_manifest_path = os.path.join(output_folder, "manifest.tsv")
    df.to_csv(retinal_oct_manifest_path, sep="\t", index=False)
    logger.info(f"Retinal OCT manifest saved: {retinal_oct_manifest_path}")

    # Step 3: Process retinal OCTA files (depends on both photography and OCT manifests)
    logger.info("=" * 50)
    logger.info("STEP 3: Processing retinal OCTA files")
    logger.info("=" * 50)

    retinal_octa_folder = os.path.join(input_folder, "retinal_octa")
    retinal_octa_metadata_folder = save_octa_metadata_in_json(retinal_octa_folder)

    # Step 4: Create comprehensive OCTA manifest linking all associated files
    logger.info("=" * 50)
    logger.info("STEP 4: Creating comprehensive OCTA manifest")
    logger.info("=" * 50)

    octa_output_folder = os.path.join(output_manifest_folder, "retinal_octa")
    octa_manifest_path = make_octa_manifest(
        retinal_octa_metadata_folder,
        retinal_photography_manifest_path,
        retinal_oct_manifest_path,
        octa_output_folder,
    )

    logger.info("=" * 50)
    logger.info("Pipeline completed successfully!")
    logger.info("=" * 50)
    logger.info(f"All manifests saved to: {output_manifest_folder}")
    logger.info("- retinal_photography/manifest.tsv")
    logger.info("- retinal_oct/manifest.tsv")
    if octa_manifest_path:
        logger.info("- retinal_octa/manifest.tsv")
    else:
        logger.info("- No OCTA manifest created (no data found)")


if __name__ == "__main__":
    main()
