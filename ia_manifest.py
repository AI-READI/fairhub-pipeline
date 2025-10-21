"""
Local Imaging Manifest Pipeline

Generate imaging manifests from local DICOM files without requiring pre-generated metadata.
This script reads DICOM files directly from a local folder and extracts the necessary
metadata to create manifests in the same format as the Azure-based pipeline.

Updated to match the metadata requirements from updated_spectralis_pipeline.py:
- Patient metadata fields (PatientSex, PatientBirthDate, PatientName, PatientID)
- Protocol name set to 'spectralis mac 20x20 hs octa'
- Manufacturer information for Heidelberg Spectralis
- Imaging type detection based on spectralis pipeline mapping (enface, heightmap, vol, op, opt)
- File naming convention matching spectralis pipeline format

@megasanjay for any questions
"""

import os
import time
import pandas as pd
import pydicom
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import logging
from tqdm import tqdm

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_dicom_files(folder_path: str) -> List[str]:
    """
    Recursively find all DICOM files in a folder.

    Args:
        folder_path (str): Path to the folder containing DICOM files

    Returns:
        List[str]: List of full paths to DICOM files
    """
    dicom_files = []
    folder_path = Path(folder_path)

    if not folder_path.exists():
        raise FileNotFoundError(f"Folder {folder_path} does not exist")

    logger.info(f"Scanning for DICOM files in: {folder_path}")

    # Walk through all subdirectories
    for root, dirs, files in tqdm(
        os.walk(folder_path), desc="Scanning directories", unit="dir", leave=False
    ):
        for file in files:
            if file.lower().endswith(".dcm") and not file.startswith("."):
                full_path = os.path.join(root, file)
                dicom_files.append(full_path)

    logger.info(f"Found {len(dicom_files)} DICOM files")

    # keep only 20 files where the file path contains the word "retinal_photography"
    d1 = [file for file in dicom_files if "retinal_photography" in file]
    d2 = [file for file in dicom_files if "retinal_oct\\" in file]
    d3 = [file for file in dicom_files if "retinal_octa\\" in file]
    dicom_files = d1[:20] + d2[:20] + d3[:20]

    return dicom_files


def extract_dicom_metadata(file_path: str) -> Optional[Dict]:
    """
    Extract metadata from a DICOM file.

    Args:
        file_path (str): Path to the DICOM file

    Returns:
        Optional[Dict]: Dictionary containing extracted metadata or None if extraction fails
    """
    try:
        # Read DICOM file
        ds = pydicom.dcmread(file_path)

        # Extract basic metadata
        full_patient_id = getattr(ds, "PatientID", "Unknown")
        # Extract patient_id from full_patient_id (format: site-patient_id)
        if "-" in full_patient_id:
            patient_id = full_patient_id.split("-")[1]
        else:
            patient_id = full_patient_id

        metadata = {
            "filepath": file_path,
            "filename": os.path.basename(file_path),
            "person_id": patient_id,
            "full_patient_id": full_patient_id,
            "manufacturer": "Heidelberg Engineering",
            "manufacturers_model_name": "Spectralis",
            "sop_instance_uid": getattr(ds, "SOPInstanceUID", "Unknown"),
            "sop_class_uid": getattr(ds, "SOPClassUID", "Unknown"),
            "study_date": getattr(ds, "StudyDate", "Unknown"),
            "study_time": getattr(ds, "StudyTime", "Unknown"),
            "series_description": getattr(ds, "SeriesDescription", "Unknown"),
            "laterality": getattr(
                ds, "ImageLaterality", getattr(ds, "Laterality", "Unknown")
            ).lower(),
            "protocol_name": "spectralis mac 20x20 hs octa",
            "patient_sex": "M",  # Standardized as per spectralis pipeline
            "patient_birth_date": "",  # Blanked as per spectralis pipeline
            "patient_name": "",  # Blanked as per spectralis pipeline
        }

        # Extract image dimensions
        if hasattr(ds, "Rows") and hasattr(ds, "Columns"):
            metadata["height"] = ds.Rows
            metadata["width"] = ds.Columns
        else:
            metadata["height"] = "Unknown"
            metadata["width"] = "Unknown"

        # Determine imaging type based on filename pattern (matching spectralis pipeline)
        filename = metadata["filename"].lower()

        # Add expected filename format based on spectralis pipeline
        expected_filename = f"{patient_id}_spectralis_mac_20x20_hs_octa_{{image_type}}_{metadata['laterality']}_{metadata['sop_instance_uid']}.dcm"
        metadata["expected_filename"] = expected_filename

        # Extract image type from filename pattern: patient_id_spectralis_mac_20x20_hs_octa_{image_type}_{laterality}_{sop_instance_uid}.dcm
        if "_spectralis_mac_20x20_hs_octa_" in filename:
            # Split filename to extract image type
            parts = filename.split("_")
            if len(parts) >= 6:
                image_type = parts[
                    5
                ]  # This should be the image type (enface, heightmap, vol, op, opt)

                # Map image types to imaging categories based on spectralis pipeline
                metadata["image_type"] = (
                    image_type  # Store the actual image type from filename
                )

                if image_type == "enface":
                    metadata["imaging"] = "OCTA"
                    metadata["anatomic_region"] = "retina"
                    metadata["ophthalmic_image_type"] = "enface"
                    # Initialize OCTA-specific fields
                    metadata["flow_cube_number_of_frames"] = 1
                    metadata["flow_cube_height"] = "Not reported"
                    metadata["flow_cube_width"] = "Not reported"
                    metadata["wavelength"] = "Not reported"
                    metadata["flow_cube_sop_instance_uid"] = metadata[
                        "sop_instance_uid"
                    ]
                    metadata["flow_cube_file_path"] = metadata["filepath"]
                    metadata["associated_segmentation_type"] = "Not reported"
                    metadata["associated_segmentation_number_of_frames"] = (
                        "Not reported"
                    )
                    metadata["associated_retinal_photography_sop_instance_uid"] = (
                        "Not reported"
                    )
                    metadata["associated_retinal_photography_file_path"] = (
                        "Not reported"
                    )
                    metadata["associated_structural_oct_sop_instance_uid"] = (
                        "Not reported"
                    )
                    metadata["associated_structural_oct_file_path"] = "Not reported"
                    metadata["associated_segmentation_sop_instance_uid"] = (
                        "Not reported"
                    )
                    metadata["associated_segmentation_file_path"] = "Not reported"
                    # Initialize enface fields (up to 4 enface images)
                    for i in range(1, 5):
                        metadata[f"associated_enface_{i}_ophthalmic_image_type"] = (
                            "Not reported"
                        )
                        metadata[f"associated_enface_{i}_segmentation_surface_1"] = (
                            "Not reported"
                        )
                        metadata[f"associated_enface_{i}_segmentation_surface_2"] = (
                            "Not reported"
                        )
                        metadata[f"associated_enface_{i}_sop_instance_uid"] = (
                            "Not reported"
                        )
                        metadata[f"associated_enface_{i}_file_path"] = "Not reported"
                        metadata[
                            f"associated_enface_{i}_projection_removed_sop_instance_uid"
                        ] = "Not reported"
                        metadata[
                            f"associated_enface_{i}_projection_removed_filepath"
                        ] = "Not reported"

                elif image_type == "segmentation":
                    metadata["imaging"] = "OCTA"
                    metadata["anatomic_region"] = "retina"
                    metadata["ophthalmic_image_type"] = "segmentation"
                    # Initialize OCTA-specific fields
                    metadata["flow_cube_number_of_frames"] = 1
                    metadata["flow_cube_height"] = "Not reported"
                    metadata["flow_cube_width"] = "Not reported"
                    metadata["wavelength"] = "Not reported"
                    metadata["flow_cube_sop_instance_uid"] = metadata[
                        "sop_instance_uid"
                    ]
                    metadata["flow_cube_file_path"] = metadata["filepath"]
                    metadata["associated_segmentation_type"] = "Not reported"
                    metadata["associated_segmentation_number_of_frames"] = (
                        "Not reported"
                    )
                    metadata["associated_retinal_photography_sop_instance_uid"] = (
                        "Not reported"
                    )
                    metadata["associated_retinal_photography_file_path"] = (
                        "Not reported"
                    )
                    metadata["associated_structural_oct_sop_instance_uid"] = (
                        "Not reported"
                    )
                    metadata["associated_structural_oct_file_path"] = "Not reported"
                    metadata["associated_segmentation_sop_instance_uid"] = (
                        "Not reported"
                    )
                    metadata["associated_segmentation_file_path"] = "Not reported"
                    # Initialize enface fields (up to 4 enface images)
                    for i in range(1, 5):
                        metadata[f"associated_enface_{i}_ophthalmic_image_type"] = (
                            "Not reported"
                        )
                        metadata[f"associated_enface_{i}_segmentation_surface_1"] = (
                            "Not reported"
                        )
                        metadata[f"associated_enface_{i}_segmentation_surface_2"] = (
                            "Not reported"
                        )
                        metadata[f"associated_enface_{i}_sop_instance_uid"] = (
                            "Not reported"
                        )
                        metadata[f"associated_enface_{i}_file_path"] = "Not reported"
                        metadata[
                            f"associated_enface_{i}_projection_removed_sop_instance_uid"
                        ] = "Not reported"
                        metadata[
                            f"associated_enface_{i}_projection_removed_filepath"
                        ] = "Not reported"

                elif image_type == "flow_cube":
                    metadata["imaging"] = "OCTA"
                    metadata["anatomic_region"] = "retina"
                    metadata["ophthalmic_image_type"] = "flow_cube"
                    # Extract OCTA-specific metadata
                    if hasattr(ds, "NumberOfFrames"):
                        metadata["flow_cube_number_of_frames"] = ds.NumberOfFrames
                    else:
                        metadata["flow_cube_number_of_frames"] = 1
                    # Extract flow cube dimensions
                    if hasattr(ds, "Rows") and hasattr(ds, "Columns"):
                        metadata["flow_cube_height"] = ds.Rows
                        metadata["flow_cube_width"] = ds.Columns
                    else:
                        metadata["flow_cube_height"] = "Not reported"
                        metadata["flow_cube_width"] = "Not reported"
                    # Extract wavelength information if available
                    if hasattr(ds, "ExcitationWavelength"):
                        metadata["wavelength"] = ds.ExcitationWavelength
                    else:
                        metadata["wavelength"] = "Not reported"
                    # Initialize OCTA-specific fields
                    metadata["flow_cube_sop_instance_uid"] = metadata[
                        "sop_instance_uid"
                    ]
                    metadata["flow_cube_file_path"] = metadata["filepath"]
                    metadata["associated_segmentation_type"] = "Not reported"
                    metadata["associated_segmentation_number_of_frames"] = (
                        "Not reported"
                    )
                    metadata["associated_retinal_photography_sop_instance_uid"] = (
                        "Not reported"
                    )
                    metadata["associated_retinal_photography_file_path"] = (
                        "Not reported"
                    )
                    metadata["associated_structural_oct_sop_instance_uid"] = (
                        "Not reported"
                    )
                    metadata["associated_structural_oct_file_path"] = "Not reported"
                    metadata["associated_segmentation_sop_instance_uid"] = (
                        "Not reported"
                    )
                    metadata["associated_segmentation_file_path"] = "Not reported"
                    # Initialize enface fields (up to 4 enface images)
                    for i in range(1, 5):
                        metadata[f"associated_enface_{i}_ophthalmic_image_type"] = (
                            "Not reported"
                        )
                        metadata[f"associated_enface_{i}_segmentation_surface_1"] = (
                            "Not reported"
                        )
                        metadata[f"associated_enface_{i}_segmentation_surface_2"] = (
                            "Not reported"
                        )
                        metadata[f"associated_enface_{i}_sop_instance_uid"] = (
                            "Not reported"
                        )
                        metadata[f"associated_enface_{i}_file_path"] = "Not reported"
                        metadata[
                            f"associated_enface_{i}_projection_removed_sop_instance_uid"
                        ] = "Not reported"
                        metadata[
                            f"associated_enface_{i}_projection_removed_filepath"
                        ] = "Not reported"

                elif image_type == "ir":
                    metadata["imaging"] = "retinal_photography"
                    metadata["anatomic_region"] = "retina"
                    metadata["ophthalmic_image_type"] = "Infrared Reflectance"
                    metadata["color_channel_dimension"] = 0  # Infrared is grayscale

                elif image_type == "oct":
                    metadata["imaging"] = "retinal_oct"
                    metadata["anatomic_region"] = "retina"
                    metadata["ophthalmic_image_type"] = "structural_oct"
                    # Extract OCT-specific metadata
                    if hasattr(ds, "NumberOfFrames"):
                        metadata["number_of_frames"] = ds.NumberOfFrames
                    else:
                        metadata["number_of_frames"] = 1
                    if hasattr(ds, "PixelSpacing"):
                        metadata["pixel_spacing"] = str(ds.PixelSpacing)
                    else:
                        metadata["pixel_spacing"] = "Unknown"
                    if hasattr(ds, "SliceThickness"):
                        metadata["slice_thickness"] = ds.SliceThickness
                    else:
                        metadata["slice_thickness"] = "Unknown"
                    # Check for reference to retinal photography
                    if hasattr(ds, "ReferencedSeriesSequence"):
                        try:
                            ref_seq = ds.ReferencedSeriesSequence[0]
                            if hasattr(ref_seq, "ReferencedInstanceSequence"):
                                ref_inst = ref_seq.ReferencedInstanceSequence[0]
                                metadata["reference_instance_uid"] = (
                                    ref_inst.ReferencedSOPInstanceUID
                                )
                        except (IndexError, AttributeError):
                            metadata["reference_instance_uid"] = "Unknown"
                    else:
                        metadata["reference_instance_uid"] = "Unknown"
                else:
                    # Unknown image type, default to unknown
                    metadata["imaging"] = "unknown"
                    metadata["anatomic_region"] = "unknown"
            else:
                # Filename doesn't match expected pattern, default to unknown
                metadata["imaging"] = "unknown"
                metadata["anatomic_region"] = "unknown"
        else:
            # Fallback to SOP Class UID based detection for non-spectralis files
            metadata["image_type"] = "unknown"  # Default for non-spectralis files
            sop_class = metadata["sop_class_uid"]
            if sop_class == "1.2.840.10008.5.1.4.1.1.77.1.5.1":
                # Determine specific imaging type from series description
                series_desc = metadata["series_description"].lower()
                if "infrared" in series_desc or "ir" in series_desc:
                    metadata["imaging"] = "retinal_photography"
                    metadata["ophthalmic_image_type"] = "Infrared Reflectance"
                    metadata["color_channel_dimension"] = 0  # Infrared is grayscale
                elif "color" in series_desc or "fundus" in series_desc:
                    metadata["imaging"] = "retinal_photography"
                    metadata["ophthalmic_image_type"] = "Color Fundus Photography"
                    metadata["color_channel_dimension"] = 3  # Color images
                else:
                    metadata["imaging"] = "retinal_photography"
                    metadata["ophthalmic_image_type"] = "retinal_photography"
                    # Extract color channel information for retinal photography
                    if hasattr(ds, "SamplesPerPixel"):
                        metadata["color_channel_dimension"] = ds.SamplesPerPixel
                    else:
                        metadata["color_channel_dimension"] = (
                            3  # Default for color images
                        )
                metadata["anatomic_region"] = "retina"
            elif sop_class == "1.2.840.10008.5.1.4.1.1.77.1.5.4":
                metadata["imaging"] = "retinal_oct"
                metadata["anatomic_region"] = "retina"
                metadata["ophthalmic_image_type"] = "structural_oct"
                # Extract OCT-specific metadata
                if hasattr(ds, "NumberOfFrames"):
                    metadata["number_of_frames"] = ds.NumberOfFrames
                else:
                    metadata["number_of_frames"] = 1
                if hasattr(ds, "PixelSpacing"):
                    metadata["pixel_spacing"] = str(ds.PixelSpacing)
                else:
                    metadata["pixel_spacing"] = "Unknown"
                if hasattr(ds, "SliceThickness"):
                    metadata["slice_thickness"] = ds.SliceThickness
                else:
                    metadata["slice_thickness"] = "Unknown"
                # Check for reference to retinal photography
                if hasattr(ds, "ReferencedSeriesSequence"):
                    try:
                        ref_seq = ds.ReferencedSeriesSequence[0]
                        if hasattr(ref_seq, "ReferencedInstanceSequence"):
                            ref_inst = ref_seq.ReferencedInstanceSequence[0]
                            metadata["reference_instance_uid"] = (
                                ref_inst.ReferencedSOPInstanceUID
                            )
                    except (IndexError, AttributeError):
                        metadata["reference_instance_uid"] = "Unknown"
                else:
                    metadata["reference_instance_uid"] = "Unknown"
            elif sop_class == "1.2.840.10008.5.1.4.1.1.77.1.5.8":
                metadata["imaging"] = "OCTA"
                metadata["anatomic_region"] = "retina"
                metadata["ophthalmic_image_type"] = "flow_cube"
                # Extract OCTA-specific metadata
                if hasattr(ds, "NumberOfFrames"):
                    metadata["flow_cube_number_of_frames"] = ds.NumberOfFrames
                else:
                    metadata["flow_cube_number_of_frames"] = 1
                # Extract flow cube dimensions
                if hasattr(ds, "Rows") and hasattr(ds, "Columns"):
                    metadata["flow_cube_height"] = ds.Rows
                    metadata["flow_cube_width"] = ds.Columns
                else:
                    metadata["flow_cube_height"] = "Not reported"
                    metadata["flow_cube_width"] = "Not reported"
                # Extract wavelength information if available
                if hasattr(ds, "ExcitationWavelength"):
                    metadata["wavelength"] = ds.ExcitationWavelength
                else:
                    metadata["wavelength"] = "Not reported"
                # Initialize OCTA-specific fields
                metadata["flow_cube_sop_instance_uid"] = metadata["sop_instance_uid"]
                metadata["flow_cube_file_path"] = metadata["filepath"]
                metadata["associated_segmentation_type"] = "Not reported"
                metadata["associated_segmentation_number_of_frames"] = "Not reported"
                metadata["associated_retinal_photography_sop_instance_uid"] = (
                    "Not reported"
                )
                metadata["associated_retinal_photography_file_path"] = "Not reported"
                metadata["associated_structural_oct_sop_instance_uid"] = "Not reported"
                metadata["associated_structural_oct_file_path"] = "Not reported"
                metadata["associated_segmentation_sop_instance_uid"] = "Not reported"
                metadata["associated_segmentation_file_path"] = "Not reported"
                # Initialize enface fields (up to 4 enface images)
                for i in range(1, 5):
                    metadata[f"associated_enface_{i}_ophthalmic_image_type"] = (
                        "Not reported"
                    )
                    metadata[f"associated_enface_{i}_segmentation_surface_1"] = (
                        "Not reported"
                    )
                    metadata[f"associated_enface_{i}_segmentation_surface_2"] = (
                        "Not reported"
                    )
                    metadata[f"associated_enface_{i}_sop_instance_uid"] = "Not reported"
                    metadata[f"associated_enface_{i}_file_path"] = "Not reported"
                    metadata[
                        f"associated_enface_{i}_projection_removed_sop_instance_uid"
                    ] = "Not reported"
                    metadata[f"associated_enface_{i}_projection_removed_filepath"] = (
                        "Not reported"
                    )
            else:
                # Unknown SOP Class, try to determine from series description
                series_desc = metadata["series_description"].lower()
                if "photography" in series_desc or "fundus" in series_desc:
                    if "infrared" in series_desc or "ir" in series_desc:
                        metadata["imaging"] = "retinal_photography"
                        metadata["ophthalmic_image_type"] = "Infrared Reflectance"
                        metadata["color_channel_dimension"] = 0
                    elif "color" in series_desc:
                        metadata["imaging"] = "retinal_photography"
                        metadata["ophthalmic_image_type"] = "Color Fundus Photography"
                        metadata["color_channel_dimension"] = 3
                    else:
                        metadata["imaging"] = "retinal_photography"
                        metadata["ophthalmic_image_type"] = "retinal_photography"
                        metadata["color_channel_dimension"] = 3
                    metadata["anatomic_region"] = "retina"
                elif "oct" in series_desc:
                    metadata["imaging"] = "retinal_oct"
                    metadata["anatomic_region"] = "retina"
                    metadata["ophthalmic_image_type"] = "structural_oct"
                    metadata["number_of_frames"] = 1
                    metadata["pixel_spacing"] = "Unknown"
                    metadata["slice_thickness"] = "Unknown"
                    metadata["reference_instance_uid"] = "Unknown"
                elif (
                    "octa" in series_desc
                    or "angiography" in series_desc
                    or "flow" in series_desc
                ):
                    metadata["imaging"] = "OCTA"
                    metadata["anatomic_region"] = "retina"
                    metadata["ophthalmic_image_type"] = "flow_cube"
                    metadata["flow_cube_number_of_frames"] = 1
                    metadata["flow_cube_height"] = "Not reported"
                    metadata["flow_cube_width"] = "Not reported"
                    metadata["wavelength"] = "Not reported"
                    # Initialize OCTA-specific fields
                    metadata["flow_cube_sop_instance_uid"] = metadata[
                        "sop_instance_uid"
                    ]
                    metadata["flow_cube_file_path"] = metadata["filepath"]
                    metadata["associated_segmentation_type"] = "Not reported"
                    metadata["associated_segmentation_number_of_frames"] = (
                        "Not reported"
                    )
                    metadata["associated_retinal_photography_sop_instance_uid"] = (
                        "Not reported"
                    )
                    metadata["associated_retinal_photography_file_path"] = (
                        "Not reported"
                    )
                    metadata["associated_structural_oct_sop_instance_uid"] = (
                        "Not reported"
                    )
                    metadata["associated_structural_oct_file_path"] = "Not reported"
                    metadata["associated_segmentation_sop_instance_uid"] = (
                        "Not reported"
                    )
                    metadata["associated_segmentation_file_path"] = "Not reported"
                    # Initialize enface fields (up to 4 enface images)
                    for i in range(1, 5):
                        metadata[f"associated_enface_{i}_ophthalmic_image_type"] = (
                            "Not reported"
                        )
                        metadata[f"associated_enface_{i}_segmentation_surface_1"] = (
                            "Not reported"
                        )
                        metadata[f"associated_enface_{i}_segmentation_surface_2"] = (
                            "Not reported"
                        )
                        metadata[f"associated_enface_{i}_sop_instance_uid"] = (
                            "Not reported"
                        )
                        metadata[f"associated_enface_{i}_file_path"] = "Not reported"
                        metadata[
                            f"associated_enface_{i}_projection_removed_sop_instance_uid"
                        ] = "Not reported"
                        metadata[
                            f"associated_enface_{i}_projection_removed_filepath"
                        ] = "Not reported"
                else:
                    metadata["imaging"] = "unknown"
                    metadata["anatomic_region"] = "unknown"

        return metadata

    except Exception as e:
        logger.error(f"Failed to extract metadata from {file_path}: {str(e)}")
        return None


def process_dicom_files(
    dicom_files: List[str],
) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    """
    Process DICOM files and categorize them by imaging type.

    Args:
        dicom_files (List[str]): List of DICOM file paths

    Returns:
        Tuple[List[Dict], List[Dict], List[Dict]]: Three lists containing metadata for
        retinal photography, OCT, and OCTA files respectively
    """
    retinal_photography_data = []
    retinal_oct_data = []
    retinal_octa_data = []

    total_files = len(dicom_files)

    logger.info(f"Processing {total_files} DICOM files...")

    for file_path in tqdm(dicom_files, desc="Processing DICOM files", unit="file"):
        metadata = extract_dicom_metadata(file_path)
        if metadata is None:
            continue

        # Categorize by imaging type
        imaging_type = metadata.get("imaging", "unknown")
        if imaging_type == "retinal_photography":
            retinal_photography_data.append(metadata)
        elif imaging_type == "retinal_oct":
            retinal_oct_data.append(metadata)
        elif imaging_type == "OCTA":
            retinal_octa_data.append(metadata)
        else:
            logger.warning(f"Unknown imaging type for file: {file_path}")

    logger.info("Processing complete. Found:")
    logger.info(f"  - Retinal Photography: {len(retinal_photography_data)} files")
    logger.info(f"  - Retinal OCT: {len(retinal_oct_data)} files")
    logger.info(f"  - Retinal OCTA: {len(retinal_octa_data)} files")

    return retinal_photography_data, retinal_oct_data, retinal_octa_data


def create_manifest(data: List[Dict], output_folder: str, imaging_type: str) -> str:
    """
    Create a manifest TSV file for a specific imaging type.

    Args:
        data (List[Dict]): List of metadata dictionaries for the specific imaging type
        output_folder (str): Output folder path
        imaging_type (str): Type of imaging (retinal_photography, retinal_oct, retinal_octa)

    Returns:
        str: Path to the created manifest file
    """
    if not data:
        logger.warning(f"No data available for {imaging_type} manifest")
        return None

    # Create DataFrame
    df = pd.DataFrame(data)

    # For OCTA manifests, ensure we have all required columns in the correct order
    if imaging_type == "retinal_octa":
        df = ensure_octa_manifest_columns(df)

    # Sort by person_id and filepath
    df = df.sort_values(["person_id", "filepath"], ascending=[True, True])

    # Create output directory for this imaging type
    imaging_folder = os.path.join(output_folder, imaging_type)
    os.makedirs(imaging_folder, exist_ok=True)

    # Define output file path
    manifest_file = os.path.join(imaging_folder, "manifest.tsv")

    # Save to TSV
    df.to_csv(manifest_file, sep="\t", index=False)

    logger.info(
        f"Created {imaging_type} manifest with {len(df)} entries: {manifest_file}"
    )

    return manifest_file


def ensure_octa_manifest_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure the OCTA manifest DataFrame has all required columns in the correct order.

    Args:
        df (pd.DataFrame): Input DataFrame with OCTA data

    Returns:
        pd.DataFrame: DataFrame with all required columns in correct order
    """
    # Define the exact column order for OCTA manifest
    required_columns = [
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

    # Add missing columns with default values
    for col in required_columns:
        if col not in df.columns:
            df[col] = "Not reported"

    # Reorder columns to match the required order
    df = df[required_columns]

    return df


def process_octa_relationships(
    octa_data: List[Dict], all_data: List[Dict]
) -> List[Dict]:
    """
    Process OCTA files to establish relationships between flow cubes, segmentation,
    enface images, and associated structural OCT and retinal photography files.

    Args:
        octa_data (List[Dict]): List of OCTA metadata dictionaries
        all_data (List[Dict]): List of all metadata dictionaries for cross-referencing

    Returns:
        List[Dict]: Updated OCTA data with established relationships
    """
    # Create mappings for cross-referencing
    uid_to_filepath = {}
    person_to_files = {}

    for item in all_data:
        uid = item.get("sop_instance_uid")
        filepath = item.get("filepath")
        person_id = item.get("person_id")

        if uid and filepath:
            uid_to_filepath[uid] = filepath

        if person_id:
            if person_id not in person_to_files:
                person_to_files[person_id] = []
            person_to_files[person_id].append(item)

    # Process each OCTA file to establish relationships
    for octa_item in octa_data:
        person_id = octa_item.get("person_id")
        if not person_id or person_id not in person_to_files:
            continue

        person_files = person_to_files[person_id]
        enface_count = 0

        # Find associated files for this person
        for file_item in person_files:
            file_imaging = file_item.get("imaging", "")
            file_uid = file_item.get("sop_instance_uid")
            file_path = file_item.get("filepath", "")
            file_ophthalmic_type = file_item.get("ophthalmic_image_type", "")

            # Match retinal photography (including Infrared Reflectance and Color Fundus Photography)
            if file_imaging in [
                "retinal_photography",
                "Infrared Reflectance",
                "Color Fundus Photography",
            ]:
                octa_item["associated_retinal_photography_sop_instance_uid"] = file_uid
                octa_item["associated_retinal_photography_file_path"] = file_path

            # Match structural OCT
            elif file_imaging == "retinal_oct":
                octa_item["associated_structural_oct_sop_instance_uid"] = file_uid
                octa_item["associated_structural_oct_file_path"] = file_path

            # Match segmentation files
            elif file_ophthalmic_type == "segmentation":
                octa_item["associated_segmentation_sop_instance_uid"] = file_uid
                octa_item["associated_segmentation_file_path"] = file_path
                # Try to extract segmentation type from series description
                series_desc = file_item.get("series_description", "").lower()
                if "heightmap" in series_desc:
                    octa_item["associated_segmentation_type"] = "Heightmap"
                elif "thickness" in series_desc:
                    octa_item["associated_segmentation_type"] = "Thickness"
                else:
                    octa_item["associated_segmentation_type"] = "Segmentation"

            # Match enface files
            elif file_ophthalmic_type == "enface" and enface_count < 4:
                enface_count += 1
                octa_item[f"associated_enface_{enface_count}_sop_instance_uid"] = (
                    file_uid
                )
                octa_item[f"associated_enface_{enface_count}_file_path"] = file_path

                # Try to extract enface type from series description
                series_desc = file_item.get("series_description", "").lower()
                if "superficial" in series_desc:
                    octa_item[
                        f"associated_enface_{enface_count}_ophthalmic_image_type"
                    ] = "Superficial vascular plexus flow"
                elif "deep" in series_desc:
                    octa_item[
                        f"associated_enface_{enface_count}_ophthalmic_image_type"
                    ] = "Deep capillary plexus flow"
                elif (
                    "choriocapillaris" in series_desc
                    or "choriocapillary" in series_desc
                ):
                    octa_item[
                        f"associated_enface_{enface_count}_ophthalmic_image_type"
                    ] = "Choriocapillaris vasculature flow"
                elif "avascular" in series_desc:
                    octa_item[
                        f"associated_enface_{enface_count}_ophthalmic_image_type"
                    ] = "Avascular complex flow"
                else:
                    octa_item[
                        f"associated_enface_{enface_count}_ophthalmic_image_type"
                    ] = "Enface flow"

    return octa_data


def add_reference_filepaths(
    oct_data: List[Dict], photography_data: List[Dict]
) -> List[Dict]:
    """
    Add reference filepaths to OCT data by matching reference UIDs with photography UIDs.

    Args:
        oct_data (List[Dict]): List of OCT metadata dictionaries
        photography_data (List[Dict]): List of photography metadata dictionaries

    Returns:
        List[Dict]: Updated OCT data with reference filepaths
    """
    # Create a mapping from SOP Instance UID to filepath for photography files
    uid_to_filepath = {}
    for photo in photography_data:
        uid = photo.get("sop_instance_uid")
        filepath = photo.get("filepath")
        if uid and filepath:
            uid_to_filepath[uid] = filepath

    # Add reference filepaths to OCT data
    for oct_item in oct_data:
        ref_uid = oct_item.get("reference_instance_uid")
        if ref_uid and ref_uid in uid_to_filepath:
            oct_item["reference_filepath"] = uid_to_filepath[ref_uid]
        else:
            oct_item["reference_filepath"] = "Unknown"

    return oct_data


def create_metadata_folder_structure(
    all_data: List[Dict], input_folder: str, output_folder: str
) -> None:
    """
    Create spectralis-metadata folder and save each DICOM file's metadata as JSON
    at the root level with filename.json format.

    Args:
        all_data (List[Dict]): List of all metadata dictionaries
        input_folder (str): Input folder path containing DICOM files
        output_folder (str): Output folder path for metadata
    """
    metadata_folder = os.path.join(output_folder, "spectralis-metadata")

    logger.info(f"Creating metadata folder: {metadata_folder}")

    # Create the metadata folder if it doesn't exist
    os.makedirs(metadata_folder, exist_ok=True)

    for metadata in tqdm(all_data, desc="Creating metadata files", unit="file"):
        try:
            # Get just the filename from the filepath
            filename = os.path.basename(metadata["filepath"])

            # Replace .dcm extension with .json
            json_filename = filename.replace(".dcm", ".json")

            # Create the full path in the metadata folder
            metadata_file_path = os.path.join(metadata_folder, json_filename)

            # Save metadata as JSON
            with open(metadata_file_path, "w") as f:
                json.dump(metadata, f, indent=2)

        except Exception as e:
            logger.error(
                f"Failed to create metadata file for {metadata.get('filepath', 'unknown')}: {str(e)}"
            )

    logger.info(f"Created metadata files in: {metadata_folder}")


def main():
    """
    Main function to process local DICOM files and generate manifests.
    """

    # Start timing
    start_time = time.time()
    start_datetime = datetime.now()

    logger.info(f"Script started at: {start_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("Mode: Local DICOM file processing")
    logger.info("")

    # Configuration
    input_folder = r"C:\Users\sanjay\Downloads\Spectralis-processed"
    output_folder = r"C:\Users\sanjay\Downloads\Spectralis-manifests"
    manifest_folder = r"C:\Users\sanjay\Downloads\manifest"

    try:
        # Get all DICOM files
        dicom_files = get_dicom_files(input_folder)

        if not dicom_files:
            logger.error("No DICOM files found in the specified folder")
            return

        # Process DICOM files
        photography_data, oct_data, octa_data = process_dicom_files(dicom_files)

        # Add reference filepaths to OCT data
        if oct_data and photography_data:
            oct_data = add_reference_filepaths(oct_data, photography_data)

        # Process OCTA relationships
        if octa_data:
            # Combine all data for cross-referencing
            all_data_for_octa = photography_data + oct_data + octa_data
            octa_data = process_octa_relationships(octa_data, all_data_for_octa)

        # Create separate manifests for each imaging type
        manifest_paths = []

        # Create retinal_photography manifest
        if photography_data:
            manifest_path = create_manifest(
                photography_data, manifest_folder, "retinal_photography"
            )
            if manifest_path:
                manifest_paths.append(manifest_path)

        # Create retinal_oct manifest
        if oct_data:
            manifest_path = create_manifest(oct_data, manifest_folder, "retinal_oct")
            if manifest_path:
                manifest_paths.append(manifest_path)

        # Create retinal_octa manifest
        if octa_data:
            manifest_path = create_manifest(octa_data, manifest_folder, "retinal_octa")
            if manifest_path:
                manifest_paths.append(manifest_path)

        # Create metadata folder structure with individual JSON files
        all_data = photography_data + oct_data + octa_data
        if all_data:
            create_metadata_folder_structure(all_data, input_folder, output_folder)

        # Show summary
        total_time = time.time() - start_time
        end_datetime = datetime.now()

        logger.info("")
        logger.info("=" * 60)
        logger.info("EXECUTION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Start time: {start_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"End time: {end_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Total execution time: {total_time:.2f} seconds")
        logger.info(f"Input folder: {input_folder}")
        logger.info(f"Output folder: {output_folder}")
        logger.info(f"Total DICOM files processed: {len(dicom_files)}")
        logger.info(f"Retinal Photography files: {len(photography_data)}")
        logger.info(f"Retinal OCT files: {len(oct_data)}")
        logger.info(f"Retinal OCTA files: {len(octa_data)}")
        logger.info(f"Total files in all manifests: {len(all_data)}")

        if manifest_paths:
            logger.info("")
            logger.info("Created files:")
            for manifest_path in manifest_paths:
                logger.info(f"  - Manifest: {manifest_path}")
            logger.info(
                f"  - Metadata folder: {os.path.join(output_folder, 'spectralis-metadata')}"
            )

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
