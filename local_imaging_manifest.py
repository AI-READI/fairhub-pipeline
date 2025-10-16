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


# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

Tag = pydicom.tag.Tag


def find_all_referenced_tags(ds, tag, path=None):
    """
    Recursively find all instances of a specific DICOM tag in a dataset.

    Args:
        ds: pydicom Dataset
        tag: pydicom Tag to search for
        path: List representing the current path in the dataset (used for recursion)

    Returns:
        List of tuples containing (path, value) for each found tag
    """
    if path is None:
        path = []

    hits = []

    for elem in ds:
        if elem.tag == tag:
            hits.append(
                ("/".join(path) or "<root>", elem.value)
            )  # value is a UID string
        if elem.VR == "SQ":
            for idx, item in enumerate(elem.value):
                hits += find_all_referenced_tags(
                    item, tag, path + [elem.keyword or str(elem.tag), str(idx)]
                )
    return hits


def main():
    """
    Main function to process local DICOM files and generate manifests.
    """

    # Configuration
    input_folder = r"C:\Users\sanjay\Downloads\Spectralis-processed"
    output_folder = r"C:\Users\sanjay\Downloads\Spectralis-manifests"

    # read the input folder and get the list of files
    retinal_oct_files = []
    retinal_octa_files = []
    retinal_photography_files = []

    if not os.path.exists(input_folder):
        raise FileNotFoundError(f"Input folder {input_folder} does not exist")

    logger.info(f"Scanning for DICOM files in: {input_folder}")

    # do the oct files first
    for root, dirs, files in tqdm(
        os.walk(os.path.join(input_folder, "retinal_oct")),
        desc="Scanning retinal OCT files",
        unit="file",
        leave=False,
    ):
        for file in files:
            if file.endswith(".dcm"):
                retinal_oct_files.append(os.path.join(root, file))

    retinal_oct_files = retinal_oct_files[:1]

    logger.info(f"Found {len(retinal_oct_files)} retinal OCT files")

    # person_id	manufacturer	manufacturers_model_name	anatomic_region	imaging	laterality	height	width	number_of_frames	pixel_spacing	slice_thickness	sop_instance_uid	filepath	reference_instance_uid	reference_filepath
    # 1001	Heidelberg	Spectralis	Optic Disc	OCT	L	496	768	27	Varies by frame	Not reported	1.3.6.1.4.1.33437.11.4.7587979.98316546453556.22400.4.1	/retinal_oct/structural_oct/heidelberg_spectralis/1001/1001_spectralis_onh_rc_hr_oct_oct_l_1.3.6.1.4.1.33437.11.4.7587979.98316546453556.22400.4.1.dcm	1.3.6.1.4.1.33437.11.4.7587979.98316546453553.22400.4.0.0	/retinal_photography/ir/heidelberg_spectralis/1001/1001_spectralis_onh_rc_hr_oct_ir_l_1.3.6.1.4.1.33437.11.4.7587979.98316546453553.22400.4.0.0.dcm

    # Create a dataobject for the retinal oct files
    retinal_oct_data = []
    for file in tqdm(
        retinal_oct_files, desc="Processing retinal OCT files", unit="file"
    ):
        # read the dicom file to extract the metadata
        ds = pydicom.dcmread(file)

        patient_id = ds.PatientID
        laterality = ds.Laterality.strip("_").upper()
        height = ds.Rows
        width = ds.Columns
        number_of_frames = ds.NumberOfFrames
        sop_instance_uid = ds.SOPInstanceUID

        pixel_spacing = list(
            map(
                float,
                ds.SharedFunctionalGroupsSequence[0]
                .PixelMeasuresSequence[0]
                .PixelSpacing,
            )
        )

        slice_thickness = float(
            ds.SharedFunctionalGroupsSequence[0].PixelMeasuresSequence[0].SliceThickness
        )

        # Find referenced instance UID (tag 0x0008, 0x1155)
        referenced_tags = find_all_referenced_tags(ds, Tag(0x0008, 0x1155))
        if referenced_tags:
            reference_instance_uid = referenced_tags[0][1]
        else:
            reference_instance_uid = "Unknown"
            logger.warning(f"No referenced instance UID found for {sop_instance_uid}")

        potential_reference_filename = f"{patient_id}_spectralis_mac_20x20_hs_octa_ir_{laterality}_{reference_instance_uid}.dcm"
        potential_reference_filepath = os.path.join(
            input_folder,
            "retinal_photography",
            "ir",
            "heidelberg_spectralis",
            patient_id,
            potential_reference_filename,
        )

        if os.path.exists(potential_reference_filepath):
            reference_filepath = potential_reference_filepath
            reference_filepath = potential_reference_filepath.split(
                "Spectralis-processed"
            )[1].replace("\\", "/")
        else:
            reference_filepath = "Unknown"

        filepath = file.split("Spectralis-processed")[1].replace("\\", "/")

        data = {
            "person_id": patient_id,
            "manufacturer": "Heidelberg",
            "manufacturers_model_name": "Spectralis",
            "anatomic_region": "TBD",
            "imaging": "OCT",
            "laterality": laterality,
            "height": height,
            "width": width,
            "number_of_frames": number_of_frames,
            "pixel_spacing": pixel_spacing,
            "slice_thickness": slice_thickness,
            "sop_instance_uid": sop_instance_uid,
            "filepath": filepath,
            "reference_instance_uid": reference_instance_uid,
            "reference_filepath": reference_filepath,
        }
        retinal_oct_data.append(data)

    # write the retinal oct data to a json file
    manifest_file_path = os.path.join(output_folder, "retinal_oct", "manifest")
    os.makedirs(os.path.dirname(manifest_file_path), exist_ok=True)

    with open(f"{manifest_file_path}.json", "w") as f:
        json.dump(retinal_oct_data, f, indent=4)

    with open(f"{manifest_file_path}.tsv", "w") as f:
        f.write(
            "person_id\tmanufacturer\tmanufacturers_model_name\tanatomic_region\timaging\tlaterality\theight\twidth\tnumber_of_frames\tpixel_spacing\tslice_thickness\tsop_instance_uid\tfilepath\treference_instance_uid\treference_filepath\n"
        )
        for data in retinal_oct_data:
            f.write(
                f"{data['person_id']}\t{data['manufacturer']}\t{data['manufacturers_model_name']}\t{data['anatomic_region']}\t{data['imaging']}\t{data['laterality']}\t{data['height']}\t{data['width']}\t{data['number_of_frames']}\t{data['pixel_spacing']}\t{data['slice_thickness']}\t{data['sop_instance_uid']}\t{data['filepath']}\t{data['reference_instance_uid']}\t{data['reference_filepath']}\n"
            )
    logger.info(
        f"Wrote retinal OCT manifests to {manifest_file_path}.json and {manifest_file_path}.tsv"
    )

    # person_id	manufacturer	manufacturers_model_name	anatomic_region	imaging	laterality	flow_cube_height	flow_cube_width	flow_cube_number_of_frames	associated_segmentation_type	associated_segmentation_number_of_frames	associated_enface_1_ophthalmic_image_type	associated_enface_1_segmentation_surface_1	associated_enface_1_segmentation_surface_2	associated_enface_2_ophthalmic_image_type	associated_enface_2_segmentation_surface_1	associated_enface_2_segmentation_surface_2	associated_enface_3_ophthalmic_image_type	associated_enface_3_segmentation_surface_1	associated_enface_3_segmentation_surface_2	associated_enface_4_ophthalmic_image_type	associated_enface_4_segmentation_surface_1	associated_enface_4_segmentation_surface_2	flow_cube_sop_instance_uid	flow_cube_file_path	associated_retinal_photography_sop_instance_uid	associated_retinal_photography_file_path	associated_structural_oct_sop_instance_uid	associated_structural_oct_file_path	associated_segmentation_sop_instance_uid	associated_segmentation_file_path	associated_enface_1_sop_instance_uid	associated_enface_1_file_path	associated_enface_2_sop_instance_uid	associated_enface_2_file_path	associated_enface_2_projection_removed_sop_instance_uid	associated_enface_2_projection_removed_filepath	associated_enface_3_sop_instance_uid	associated_enface_3_file_path	associated_enface_3_projection_removed_sop_instance_uid	associated_enface_3_projection_removed_filepath	associated_enface_4_sop_instance_uid	associated_enface_4_file_path	associated_enface_4_projection_removed_sop_instance_uid	associated_enface_4_projection_removed_filepath
    # 1001	Topcon	Maestro2	Macula, 6 x 6	OCTA	R	885	360	360	Heightmap	9	Superficial vascular plexus flow	Ilm - internal limiting membrane	Outer surface of ipl	Deep capillary plexus flow	Outer surface of ipl	Outer surface of ipl	Choriocapillaris vasculature flow	Outer surface of the rpe	Outer surface of the rpe	Avascular complex flow	Outer surface of ipl	Outer surface of the rpe	2.16.840.1.114517.10.5.1.4.907063120230727170230.3.1	/retinal_octa/flow_cube/topcon_maestro2/1001/1001_maestro2_macula_6x6_octa_flow_cube_r_2.16.840.1.114517.10.5.1.4.907063120230727170230.3.1.dcm	2.16.840.1.114517.10.5.1.4.907063120230727170230.2.1	/retinal_photography/ir/topcon_maestro2/1001/1001_maestro2_macula_6x6_octa_ir_r_2.16.840.1.114517.10.5.1.4.907063120230727170230.2.1.dcm	2.16.840.1.114517.10.5.1.4.907063120230727170230.1.1	/retinal_oct/structural_oct/topcon_maestro2/1001/1001_maestro2_macula_6x6_octa_oct_r_2.16.840.1.114517.10.5.1.4.907063120230727170230.1.1.dcm	2.16.840.1.114517.10.5.1.4.907063120230727170230.7.3	/retinal_octa/segmentation/topcon_maestro2/1001/1001_maestro2_macula_6x6_octa_segmentation_r_2.16.840.1.114517.10.5.1.4.907063120230727170230.7.3.dcm	2.16.840.1.114517.10.5.1.4.907063120230727170230.6.3	/retinal_octa/enface/topcon_maestro2/1001/1001_maestro2_macula_6x6_octa_enface_r_2.16.840.1.114517.10.5.1.4.907063120230727170230.6.3.dcm	2.16.840.1.114517.10.5.1.4.907063120230727170230.6.4	/retinal_octa/enface/topcon_maestro2/1001/1001_maestro2_macula_6x6_octa_enface_r_2.16.840.1.114517.10.5.1.4.907063120230727170230.6.4.dcm	Not reported	Not reported	2.16.840.1.114517.10.5.1.4.907063120230727170230.6.5	/retinal_octa/enface/topcon_maestro2/1001/1001_maestro2_macula_6x6_octa_enface_r_2.16.840.1.114517.10.5.1.4.907063120230727170230.6.5.dcm	Not reported	Not reported	2.16.840.1.114517.10.5.1.4.907063120230727170230.6.80	/retinal_octa/enface/topcon_maestro2/1001/1001_maestro2_macula_6x6_octa_enface_r_2.16.840.1.114517.10.5.1.4.907063120230727170230.6.80.dcm	Not reported	Not reported

    # do the octa files next
    for root, dirs, files in tqdm(
        os.walk(os.path.join(input_folder, "retinal_octa")),
        desc="Scanning retinal OCTA files",
        unit="file",
        leave=False,
    ):
        for file in files:
            if file.endswith(".dcm"):
                retinal_octa_files.append(os.path.join(root, file))

    # find a file with segmentation in the path
    retinal_octa_files = [file for file in retinal_octa_files if "enface" in file]
    retinal_octa_files = retinal_octa_files[:1]

    logger.info(f"Found {len(retinal_octa_files)} retinal OCTA files")

    retinal_octa_data = []
    for file in tqdm(
        retinal_octa_files, desc="Processing retinal OCTA files", unit="file"
    ):
        # read the dicom file to extract the metadata
        ds = pydicom.dcmread(file)

        # write ds to file for debugging
        with open("dev.txt", "w") as f:
            f.write(str(ds))

        patient_id = ds.PatientID
        manufacturer = "Heidelberg"
        manufacturers_model_name = "Spectralis"
        anatomic_region = "TBD"
        imaging = "OCTA"
        laterality = ds.Laterality.strip("_").upper()

        flow_cube_height = ds.Rows or "Not reported"
        flow_cube_width = ds.Columns or "Not reported"
        flow_cube_number_of_frames = ds.NumberOfFrames or "Not reported"

        filepath = file.split("Spectralis-processed")[1].replace("\\", "/")

        associated_segmentation_type = "Heightmap"
        associated_segmentation_number_of_frames = "TBD"

        associated_enface_1_ophthalmic_image_type = "TBD"
        associated_enface_1_segmentation_surface_1 = "TBD"
        associated_enface_1_segmentation_surface_2 = "TBD"
        associated_enface_2_ophthalmic_image_type = "TBD"
        associated_enface_2_segmentation_surface_1 = "TBD"
        associated_enface_2_segmentation_surface_2 = "TBD"
        associated_enface_3_ophthalmic_image_type = "TBD"
        associated_enface_3_segmentation_surface_1 = "TBD"
        associated_enface_3_segmentation_surface_2 = "TBD"
        associated_enface_4_ophthalmic_image_type = "TBD"
        associated_enface_4_segmentation_surface_1 = "TBD"
        associated_enface_4_segmentation_surface_2 = "TBD"
        flow_cube_sop_instance_uid = ds.SOPInstanceUID or "Not reported"
        flow_cube_file_path = filepath
        associated_retinal_photography_sop_instance_uid = ""
        associated_retinal_photography_file_path = ""
        associated_structural_oct_sop_instance_uid = "TBD   "
        associated_structural_oct_file_path = "TBD"
        associated_segmentation_sop_instance_uid = "TBD"
        associated_segmentation_file_path = "TBD"
        associated_enface_1_sop_instance_uid = "TBD"
        associated_enface_1_file_path = "TBD"
        associated_enface_2_sop_instance_uid = "TBD"
        associated_enface_2_file_path = "TBD"
        associated_enface_2_projection_removed_sop_instance_uid = "TBD"
        associated_enface_2_projection_removed_filepath = "TBD"
        associated_enface_3_sop_instance_uid = "TBD"
        associated_enface_3_file_path = "TBD"
        associated_enface_3_projection_removed_sop_instance_uid = "TBD"
        associated_enface_3_projection_removed_filepath = "TBD"
        associated_enface_4_sop_instance_uid = "TBD"
        associated_enface_4_file_path = "TBD"
        associated_enface_4_projection_removed_sop_instance_uid = "TBD"
        associated_enface_4_projection_removed_filepath = "TBD"

        filepath = file.split("Spectralis-processed")[1].replace("\\", "/")

        data = {
            "person_id": patient_id,
            "manufacturer": manufacturer,
            "manufacturers_model_name": manufacturers_model_name,
            "anatomic_region": anatomic_region,
            "imaging": imaging,
            "laterality": laterality,
            "flow_cube_height": flow_cube_height,
            "flow_cube_width": flow_cube_width,
            "flow_cube_number_of_frames": flow_cube_number_of_frames,
            "associated_segmentation_type": associated_segmentation_type,
            "associated_segmentation_number_of_frames": associated_segmentation_number_of_frames,
            "associated_enface_1_ophthalmic_image_type": associated_enface_1_ophthalmic_image_type,
            "associated_enface_1_segmentation_surface_1": associated_enface_1_segmentation_surface_1,
            "associated_enface_1_segmentation_surface_2": associated_enface_1_segmentation_surface_2,
            "associated_enface_2_ophthalmic_image_type": associated_enface_2_ophthalmic_image_type,
            "associated_enface_2_segmentation_surface_1": associated_enface_2_segmentation_surface_1,
            "associated_enface_2_segmentation_surface_2": associated_enface_2_segmentation_surface_2,
            "associated_enface_3_ophthalmic_image_type": associated_enface_3_ophthalmic_image_type,
            "associated_enface_3_segmentation_surface_1": associated_enface_3_segmentation_surface_1,
            "associated_enface_3_segmentation_surface_2": associated_enface_3_segmentation_surface_2,
            "associated_enface_4_ophthalmic_image_type": associated_enface_4_ophthalmic_image_type,
            "associated_enface_4_segmentation_surface_1": associated_enface_4_segmentation_surface_1,
            "associated_enface_4_segmentation_surface_2": associated_enface_4_segmentation_surface_2,
            "flow_cube_sop_instance_uid": flow_cube_sop_instance_uid,
            "flow_cube_file_path": flow_cube_file_path,
            "associated_retinal_photography_sop_instance_uid": associated_retinal_photography_sop_instance_uid,
            "associated_retinal_photography_file_path": associated_retinal_photography_file_path,
            "associated_structural_oct_sop_instance_uid": associated_structural_oct_sop_instance_uid,
            "associated_structural_oct_file_path": associated_structural_oct_file_path,
            "associated_segmentation_sop_instance_uid": associated_segmentation_sop_instance_uid,
            "associated_segmentation_file_path": associated_segmentation_file_path,
            "associated_enface_1_sop_instance_uid": associated_enface_1_sop_instance_uid,
            "associated_enface_1_file_path": associated_enface_1_file_path,
            "associated_enface_2_sop_instance_uid": associated_enface_2_sop_instance_uid,
            "associated_enface_2_file_path": associated_enface_2_file_path,
            "associated_enface_2_projection_removed_sop_instance_uid": associated_enface_2_projection_removed_sop_instance_uid,
            "associated_enface_2_projection_removed_filepath": associated_enface_2_projection_removed_filepath,
            "associated_enface_3_sop_instance_uid": associated_enface_3_sop_instance_uid,
            "associated_enface_3_file_path": associated_enface_3_file_path,
            "associated_enface_3_projection_removed_sop_instance_uid": associated_enface_3_projection_removed_sop_instance_uid,
            "associated_enface_3_projection_removed_filepath": associated_enface_3_projection_removed_filepath,
            "associated_enface_4_sop_instance_uid": associated_enface_4_sop_instance_uid,
            "associated_enface_4_file_path": associated_enface_4_file_path,
            "associated_enface_4_projection_removed_sop_instance_uid": associated_enface_4_projection_removed_sop_instance_uid,
            "associated_enface_4_projection_removed_filepath": associated_enface_4_projection_removed_filepath,
        }
        retinal_octa_data.append(data)

    # write the retinal octa data to a json file
    manifest_file_path = os.path.join(output_folder, "retinal_octa", "manifest")
    os.makedirs(os.path.dirname(manifest_file_path), exist_ok=True)
    with open(f"{manifest_file_path}.json", "w") as f:
        json.dump(retinal_octa_data, f, indent=4)

    with open(f"{manifest_file_path}.tsv", "w") as f:
        f.write(
            "person_id\tmanufacturer\tmanufacturers_model_name\tanatomic_region\timaging\tlaterality\tflow_cube_height\tflow_cube_width\tflow_cube_number_of_frames\tassociated_segmentation_type\tassociated_segmentation_number_of_frames\tassociated_enface_1_ophthalmic_image_type\tassociated_enface_1_segmentation_surface_1\tassociated_enface_1_segmentation_surface_2\tassociated_enface_2_ophthalmic_image_type\tassociated_enface_2_segmentation_surface_1\tassociated_enface_2_segmentation_surface_2\tassociated_enface_3_ophthalmic_image_type\tassociated_enface_3_segmentation_surface_1\tassociated_enface_3_segmentation_surface_2\tassociated_enface_4_ophthalmic_image_type\tassociated_enface_4_segmentation_surface_1\tassociated_enface_4_segmentation_surface_2\tflow_cube_sop_instance_uid\tflow_cube_file_path\tassociated_retinal_photography_sop_instance_uid\tassociated_retinal_photography_file_path\tassociated_structural_oct_sop_instance_uid\tassociated_structural_oct_file_path\tassociated_segmentation_sop_instance_uid\tassociated_segmentation_file_path\tassociated_enface_1_sop_instance_uid\tassociated_enface_1_file_path\tassociated_enface_2_sop_instance_uid\tassociated_enface_2_file_path\tassociated_enface_2_projection_removed_sop_instance_uid\tassociated_enface_2_projection_removed_filepath\tassociated_enface_3_sop_instance_uid\tassociated_enface_3_file_path\tassociated_enface_3_projection_removed_sop_instance_uid\tassociated_enface_3_projection_removed_filepath\tassociated_enface_4_sop_instance_uid\tassociated_enface_4_file_path\tassociated_enface_4_projection_removed_sop_instance_uid\tassociated_enface_4_projection_removed_filepath\n"
        )
        for data in retinal_octa_data:
            f.write(
                f"{data['person_id']}\t{data['manufacturer']}\t{data['manufacturers_model_name']}\t{data['anatomic_region']}\t{data['imaging']}\t{data['laterality']}\t{data['flow_cube_height']}\t{data['flow_cube_width']}\t{data['flow_cube_number_of_frames']}\t{data['associated_segmentation_type']}\t{data['associated_segmentation_number_of_frames']}\t{data['associated_enface_1_ophthalmic_image_type']}\t{data['associated_enface_1_segmentation_surface_1']}\t{data['associated_enface_1_segmentation_surface_2']}\t{data['associated_enface_2_ophthalmic_image_type']}\t{data['associated_enface_2_segmentation_surface_1']}\t{data['associated_enface_2_segmentation_surface_2']}\t{data['associated_enface_3_ophthalmic_image_type']}\t{data['associated_enface_3_segmentation_surface_1']}\t{data['associated_enface_3_segmentation_surface_2']}\t{data['associated_enface_4_ophthalmic_image_type']}\t{data['associated_enface_4_segmentation_surface_1']}\t{data['associated_enface_4_segmentation_surface_2']}\t{data['flow_cube_sop_instance_uid']}\t{data['flow_cube_file_path']}\t{data['associated_retinal_photography_sop_instance_uid']}\t{data['associated_retinal_photography_file_path']}\t{data['associated_structural_oct_sop_instance_uid']}\t{data['associated_structural_oct_file_path']}\t{data['associated_segmentation_sop_instance_uid']}\t{data['associated_segmentation_file_path']}\t{data['associated_enface_1_sop_instance_uid']}\t{data['associated_enface_1_file_path']}\t{data['associated_enface_2_sop_instance_uid']}\t{data['associated_enface_2_file_path']}\t{data['associated_enface_2_projection_removed_sop_instance_uid']}\t{data['associated_enface_2_projection_removed_filepath']}\t{data['associated_enface_3_sop_instance_uid']}\t{data['associated_enface_3_file_path']}\t{data['associated_enface_3_projection_removed_sop_instance_uid']}\t{data['associated_enface_3_projection_removed_filepath']}\t{data['associated_enface_4_sop_instance_uid']}\t{data['associated_enface_4_file_path']}\t{data['associated_enface_4_projection_removed_sop_instance_uid']}\t{data['associated_enface_4_projection_removed_filepath']}\n"
            )

    logger.info(
        f"Wrote retinal OCTA manifests to {manifest_file_path}.json and {manifest_file_path}.tsv"
    )

    # person_id	manufacturer	manufacturers_model_name	laterality	anatomic_region	imaging	height	width	color_channel_dimension	sop_instance_uid	filepath
    # 1001	Heidelberg	Spectralis	L	Optic Disc	Infrared Reflectance	1536	1536	0	1.3.6.1.4.1.33437.11.4.7587979.98316546453553.22400.4.0.0	/retinal_photography/ir/

    # do the photography files last
    for root, dirs, files in tqdm(
        os.walk(os.path.join(input_folder, "retinal_photography")),
        desc="Scanning retinal photography files",
        unit="file",
        leave=False,
    ):
        for file in files:
            if file.endswith(".dcm"):
                retinal_photography_files.append(os.path.join(root, file))

    retinal_photography_files = retinal_photography_files[:1]

    logger.info(f"Found {len(retinal_photography_files)} retinal photography files")

    retinal_photography_data = []
    for file in tqdm(
        retinal_photography_files,
        desc="Processing retinal photography files",
        unit="file",
    ):
        # read the dicom file to extract the metadata
        ds = pydicom.dcmread(file)

        patient_id = ds.PatientID
        manufacturer = "Heidelberg"
        manufacturers_model_name = "Spectralis"
        laterality = ds.Laterality.strip("_").upper()
        anatomic_region = "TBD"
        imaging = "Infrared Reflectance"
        height = ds.Rows or "Not reported"
        width = ds.Columns or "Not reported"

        color_channel_dimension = "TBD"
        sop_instance_uid = ds.SOPInstanceUID or "Not reported"
        filepath = file.split("Spectralis-processed")[1].replace("\\", "/")

        data = {
            "person_id": patient_id,
            "manufacturer": manufacturer,
            "manufacturers_model_name": manufacturers_model_name,
            "laterality": laterality,
            "anatomic_region": anatomic_region,
            "imaging": imaging,
            "height": height,
            "width": width,
            "color_channel_dimension": color_channel_dimension,
            "sop_instance_uid": sop_instance_uid,
            "filepath": filepath,
        }
        retinal_photography_data.append(data)

    # write the retinal photography data to a json file
    manifest_file_path = os.path.join(output_folder, "retinal_photography", "manifest")
    os.makedirs(os.path.dirname(manifest_file_path), exist_ok=True)
    with open(f"{manifest_file_path}.json", "w") as f:
        json.dump(retinal_photography_data, f, indent=4)

    with open(f"{manifest_file_path}.tsv", "w") as f:
        f.write(
            "person_id\tmanufacturer\tmanufacturers_model_name\tlaterality\tanatomic_region\timaging\theight\twidth\tcolor_channel_dimension\tsop_instance_uid\tfilepath\n"
        )
        for data in retinal_photography_data:
            f.write(
                f"{data['person_id']}\t{data['manufacturer']}\t{data['manufacturers_model_name']}\t{data['laterality']}\t{data['anatomic_region']}\t{data['imaging']}\t{data['height']}\t{data['width']}\t{data['color_channel_dimension']}\t{data['sop_instance_uid']}\t{data['filepath']}\n"
            )
    logger.info(
        f"Wrote retinal photography manifests to {manifest_file_path}.json and {manifest_file_path}.tsv"
    )


if __name__ == "__main__":
    main()
