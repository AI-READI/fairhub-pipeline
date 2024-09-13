import os
import pydicom
import imaging.imaging_utils as imaging_utils
import json


oct_mapping = {
    "maestro2_3d_wide_oct": [
        "Topcon",
        "Maestro2",
        "Wide Field",
        "OCT",
    ],
    "maestro2_macula_6x6_oct": ["Topcon", "Maestro2", "Macula, 6 x 6", "OCT"],
    "maestro2_3d_macula_oct": ["Topcon", "Maestro2", "Macula", "OCT"],
    "triton_3d_radial_oct": ["Topcon", "Triton", "Optic Disc", "OCT"],
    "triton_macula_6x6_oct": ["Topcon", "Triton", "Macula, 6 x 6", "OCT"],
    "triton_macula_12x12_oct": ["Topcon", "Triton", "Macula, 12 x 12", "OCT"],
    "spectralis_onh_rc_hr_oct": ["Heidelberg", "Spectralis", "Optic Disc", "OCT"],
    "spectralis_ppol_mac_hr_oct": ["Heidelberg", "Spectralis", "Macula", "OCT"],
    "cirrus_disc_6x6_octa_oct": ["Zeiss", "Cirrus", "Optic Disc, 6 x 6", "OCT"],
    "cirrus_macula_6x6_octa_oct": ["Zeiss", "Cirrus", "Macula, 6 x 6", "OCT"],
    "cirrus_disc_oct": ["Zeiss", "Cirrus", "Optic Disc", "OCT"],
    "cirrus_mac_oct": ["Zeiss", "Cirrus", "Macula", "OCT"],
}

retinal_photography_mapping = {
    "optomed_mac_or_disk_centered_cfp": [
        "Optomed",
        "Aurora",
        "Macula or Optic Disc",
        "Color Photography",
        "3",
    ],
    "eidon_mosaic_cfp": ["iCare", "Eidon", "Mosaic", "Color Photography", "3"],
    "eidon_uwf_central_faf": ["iCare", "Eidon", "Macula", "Autofluorescence", "3"],
    "eidon_uwf_central_ir": ["iCare", "Eidon", "Macula", "Infrared Reflectance", "3"],
    "eidon_uwf_nasal_cfp": ["iCare", "Eidon", "Nasal", "Color Photography", "3"],
    "eidon_uwf_temporal_cfp": [
        "iCare",
        "Eidon",
        "Temporal Periphery",
        "Color Photography",
        "3",
    ],
    "eidon_uwf_central_cfp": ["iCare", "Eidon", "Macula", "Color Photography", "3"],
    "maestro2_3d_wide": ["Topcon", "Maestro2", "Wide Field", "Color Photography", "3"],
    "maestro2_macula_6x6": [
        "Topcon",
        "Maestro2",
        "Macula, 6 x 6",
        "Infrared Reflectance",
        "3",
    ],
    "maestro2_3d_macula": ["Topcon", "Maestro2", "Macula", "Color Photography", "3"],
    "triton_3d_radial": ["Topcon", "Triton", "Optic Disc", "Color Photography", "3"],
    "triton_macula_6x6": [
        "Topcon",
        "Triton",
        "Macula, 6 x 6",
        "Color Photography",
        "3",
    ],
    "triton_macula_12x12": [
        "Topcon",
        "Triton",
        "Macula, 12 x 12",
        "Color Photography",
        "3",
    ],
    "spectralis_onh_rc_hr_ir": [
        "Heidelberg",
        "Spectralis",
        "Optic Disc",
        "Infrared Reflectance",
        "0",
    ],
    "spectralis_ppol_mac_hr_ir": [
        "Heidelberg",
        "Spectralis",
        "Macula",
        "Infrared Reflectance",
        "0",
    ],
    "spectralis_ppol_mac_hr_ir": [
        "Heidelberg",
        "Spectralis",
        "Macula",
        "Infrared Reflectance",
        "0",
    ],
    "cirrus_disc": [
        "Zeiss",
        "Cirrus",
        "Optic Disc",
        "Infrared Reflectance",
        "0",
    ],
    "cirrus_mac": [
        "Zeiss",
        "Cirrus",
        "Macula",
        "Infrared Reflectance",
        "0",
    ],
}


def get_list_from_filename_retinal_photography(filename):
    """
    Extracts the list of details for retinal photography based on the filename.

    Args:
        filename (str): The name of the file to parse.

    Returns:
        list or None: A list containing manufacturer, device, anatomic region, imaging type, and color channel dimension
                      if the filename matches a key in the retinal_photography_mapping, otherwise None.
    """
    for key in retinal_photography_mapping:
        if key in filename:
            return retinal_photography_mapping[key]
    return None


def get_list_from_filename_oct(filename):
    """
    Extracts the list of details for OCT images based on the filename.

    Args:
        filename (str): The name of the file to parse.

    Returns:
        list or None: A list containing manufacturer, device, anatomic region, and imaging type
                      if the filename matches a key in the oct_mapping, otherwise None.
    """
    for key in oct_mapping:
        if key in filename:
            return oct_mapping[key]
    return None


def meta_data_save(filename, output_folder):
    """
    Extracts metadata from a DICOM file and saves it as a JSON file in the specified output folder.

    The function reads the DICOM file, identifies whether it is a retinal photography or OCT based on SOPClassUID,
    extracts relevant metadata, and saves it as a JSON file in the output folder.

    Args:
        filename (str): Full path to the DICOM *.dcm file.
        output_folder (str): Full path to the folder where the output metadata JSON file will be saved.

    Returns:
        dict: A dictionary containing the extracted metadata.
    """

    dataset = pydicom.dcmread(filename)

    if dataset.SOPClassUID == "1.2.840.10008.5.1.4.1.1.77.1.5.1":

        start_index = filename.find("/retinal_photography")
        file = filename[start_index:]

        # Extracting metadata
        patient_id = dataset.get("PatientID", "")

        try:
            manufacturer = get_list_from_filename_retinal_photography(filename)[0]
        except:
            print(f"{filename}")
        manufacturer = get_list_from_filename_retinal_photography(filename)[0]
        device = get_list_from_filename_retinal_photography(filename)[1]
        anatomic_region = get_list_from_filename_retinal_photography(filename)[2]
        imaging = get_list_from_filename_retinal_photography(filename)[3]
        color_channel_dimension = get_list_from_filename_retinal_photography(filename)[
            4
        ]

        laterality = next(
            (
                laterality.strip("_").upper()
                for laterality in ["_l_", "_r_"]
                if laterality in filename
            ),
            "unknown_laterality",
        )

        height = str(dataset.Rows)
        width = str(dataset.Columns)
        sop_instance_uid = dataset.SOPInstanceUID
        rule = dataset.ProtocolName

        dic = {
            "participant_id": patient_id,
            "manufacturer": manufacturer,
            "manufacturers_model_name": device,
            "laterality": laterality,
            "anatomic_region": anatomic_region,
            "imaging": imaging,
            "height": height,
            "width": width,
            "color_channel_dimension": color_channel_dimension,
            "filepath": file,
            "sop_instance_uid": sop_instance_uid,
            "protocol": rule,
            "content_time": dataset.ContentDate + dataset.ContentTime,
            "sop_class_uid": dataset.SOPClassUID,
        }

        filename = file.split("/")[-1].replace(".", "_")

        json_data = {filename: dic}

        os.makedirs(f"{output_folder}/retinal_photography", exist_ok=True)

        with open(
            f"{output_folder}/retinal_photography/{filename}.json", "w"
        ) as json_file:
            json.dump(json_data, json_file)

        print(json_data)

        return dic

    if dataset.SOPClassUID == "1.2.840.10008.5.1.4.1.1.77.1.5.4":

        start_index = filename.find("/retinal_oct")
        file = filename[start_index:]

        # Extracting metadata
        patient_id = dataset.get("PatientID", "")

        try:
            manufacturer = get_list_from_filename_oct(filename)[0]
        except:
            print(f"{filename}")

        manufacturer = get_list_from_filename_oct(filename)[0]
        device = get_list_from_filename_oct(filename)[1]
        anatomic_region = get_list_from_filename_oct(filename)[2]
        imaging = get_list_from_filename_oct(filename)[3]

        laterality = next(
            (
                laterality.strip("_").upper()
                for laterality in ["_l_", "_r_"]
                if laterality in filename
            ),
            "unknown_laterality",
        )

        height = str(dataset.Rows)
        width = str(dataset.Columns)
        number_of_frames = str(dataset.NumberOfFrames)

        if "spectralis_onh_rc_hr_oct" in filename:
            pixel_spacing = "Varies by frame"
            slice_thickness = "Not reported"

        else:
            pixel_spacing = str(dataset[0x52009229][0][0x00289110][0][0x00280030].value)
            slice_thickness = str(
                dataset[0x52009229][0][0x00289110][0][0x00180050].value
            )

        sop_instance_uid = dataset.SOPInstanceUID
        reference_instance_uid = dataset[0x52009229][0][0x00081140][0][0x00081155].value

        dic = {
            "participant_id": patient_id,
            "manufacturer": manufacturer,
            "manufacturers_model_name": device,
            "anatomic_region": anatomic_region,
            "imaging": imaging,
            "laterality": laterality,
            "height": height,
            "width": width,
            "number_of_frames": number_of_frames,
            "pixel_spacing": pixel_spacing,
            "slice_thickness": slice_thickness,
            "filepath": file,
            "sop_instance_uid": sop_instance_uid,
            "reference_retinal_photography_image_instance_uid": reference_instance_uid,
            "protocol": dataset.ProtocolName,
            "content_time": dataset.ContentDate + dataset.ContentTime,
            "sop_class_uid": dataset.SOPClassUID,
        }

        filename = file.split("/")[-1].replace(".", "_")

        json_data = {filename: dic}

        os.makedirs(f"{output_folder}/retinal_oct", exist_ok=True)

        with open(f"{output_folder}/retinal_oct/{filename}.json", "w") as json_file:
            json.dump(json_data, json_file)

        print(json_data)

        return dic

    if dataset.SOPClassUID == "1.2.840.10008.5.1.4.1.1.77.1.5.8":

        start_index = filename.find("/retinal_octa")
        file = filename[start_index:]

        # Extracting metadata
        patient_id = dataset.get("PatientID", "")

        modality = "octa" if "_octa/" in file else "unknown_modality"

        sub_modality = next(
            (
                submodality.replace("_", " ")
                for submodality in ["_flow_cube_raw_", "_flow_cube_"]
                if submodality in filename
            ),
            "unknown_submodality",
        )

        manufacturer = next(
            (
                manufacturer
                for manufacturer in ["topcon", "zeiss"]
                if manufacturer in file
            ),
            "unknown_manufacturer",
        )

        device = next(
            (device for device in ["maestro2", "triton", "cirrus"] if device in file),
            "unknown_device",
        )

        laterality = next(
            (
                laterality.strip("_").upper()
                for laterality in ["_l_", "_r_"]
                if laterality in filename
            ),
            "unknown_laterality",
        )

        protocol = dataset.ProtocolName
        height = dataset.Rows
        width = dataset.Columns
        number_of_frames = dataset.NumberOfFrames
        sop_instance_uid = dataset.SOPInstanceUID

        dic = {
            "filepath": file,
            "patient id": patient_id,
            "modality": modality,
            "submodality": sub_modality,
            "manufacturer": manufacturer,
            "device": device,
            "laterality": laterality,
            "protocol": protocol,
            "height": height,
            "width": width,
            "number of frames": number_of_frames,
            "sop instance uid": sop_instance_uid,
            "content_time": dataset.ContentDate + dataset.ContentTime,
            "sop_class_uid": dataset.SOPClassUID,
        }
        filename = file.split("/")[-1].replace(".", "_")

        json_data = {filename: dic}

        os.makedirs(f"{output_folder}/retinal_octa", exist_ok=True)

        with open(f"{output_folder}/retinal_octa/{filename}.json", "w") as json_file:
            json.dump(json_data, json_file)

        print(json_data)

        return dic

    if dataset.SOPClassUID == "1.2.840.10008.5.1.4.xxxxx.1":

        start_index = filename.find("/retinal_octa")
        file = filename[start_index:]

        # Extracting metadata
        patient_id = dataset.get("PatientID", "")

        modality = "octa" if "octa/" in file else "unknown_modality"

        sub_modality = next(
            (
                submodality.strip("/")
                for submodality in ["/segmentation/"]
                if submodality in filename
            ),
            "unknown_submodality",
        )

        manufacturer = next(
            (
                manufacturer
                for manufacturer in ["topcon", "zeiss"]
                if manufacturer in file
            ),
            "unknown_manufacturer",
        )

        device = next(
            (device for device in ["maestro2", "triton", "cirrus"] if device in file),
            "unknown_device",
        )

        laterality = next(
            (
                laterality.strip("_")
                for laterality in ["_l_", "_r_"]
                if laterality in filename
            ),
            "unknown_laterality",
        )

        protocol = dataset.ProtocolName
        height = dataset.Rows
        width = dataset.Columns
        number_of_frames = dataset.NumberOfFrames
        sop_instance_uid = dataset.SOPInstanceUID

        dic = {
            "filepath": file,
            "patient id": patient_id,
            "modality": modality,
            "submodality": sub_modality,
            "manufacturer": manufacturer,
            "device": device,
            "laterality": laterality,
            "protocol": protocol,
            "height": height,
            "width": width,
            "number of frames": number_of_frames,
            "sop instance uid": sop_instance_uid,
            "segmentation type": "Heightmap",
            "content time": dataset.ContentDate + dataset.ContentTime,
            "sop class uid": dataset.SOPClassUID,
        }

        filename = file.split("/")[-1].replace(".", "_")

        json_data = {filename: dic}

        os.makedirs(f"{output_folder}/retinal_octa", exist_ok=True)

        with open(f"{output_folder}/retinal_octa/{filename}.json", "w") as json_file:
            json.dump(json_data, json_file)

        print(json_data)

        return dic

    if dataset.SOPClassUID == "1.2.840.10008.5.1.4.1.1.77.1.5.7":

        start_index = filename.find("/retinal_octa")
        file = filename[start_index:]

        # Extracting metadata
        patient_id = dataset.get("PatientID", "")

        modality = "octa" if "octa/" in file else "unknown_modality"

        sub_modality = next(
            (
                submodality.strip("/")
                for submodality in ["/enface/"]
                if submodality in file
            ),
            "unknown_submodality",
        )

        manufacturer = next(
            (
                manufacturer
                for manufacturer in ["topcon", "zeiss"]
                if manufacturer in file
            ),
            "unknown_manufacturer",
        )

        device = next(
            (device for device in ["maestro2", "triton", "cirrus"] if device in file),
            "unknown_device",
        )

        laterality = next(
            (
                laterality.strip("_")
                for laterality in ["_l_", "_r_"]
                if laterality in filename
            ),
            "unknown_laterality",
        )

        protocol = dataset.ProtocolName
        height = dataset.Rows
        width = dataset.Columns

        sop_instance_uid = dataset.SOPInstanceUID

        ophthalmic_image_type = dataset["00221615"][0]["00080104"].value

        if len(dataset["0022EEE0"].value) == 1:
            layer1 = dataset["0022EEE0"][0]["0022EEE2"][0]["0062000F"][0][
                "00080104"
            ].value
            layer2 = ""

        elif len(dataset["0022EEE0"].value) == 2:
            layer1 = dataset["0022EEE0"][0]["0022EEE2"][0]["0062000F"][0][
                "00080104"
            ].value
            layer2 = dataset["0022EEE0"][1]["0022EEE2"][0]["0062000F"][0][
                "00080104"
            ].value

        dic = {
            "filepath": file,
            "Patient ID": patient_id,
            "Modality": modality,
            "Submodality": sub_modality,
            "Manufacturer": manufacturer,
            "Device": device,
            "Laterality": laterality,
            "Protocol": protocol,
            "Height": height,
            "Width": width,
            "sop_instance_uid": sop_instance_uid,
            "Ophthalmic_image_type": ophthalmic_image_type,
            "En Face Retinal Segmentation Surface 1": str(layer1).lower(),
            "En Face Retinal Segmentation Surface 2": str(layer2).lower(),
            "content time": dataset.ContentDate + dataset.ContentTime,
            "sop class uid": dataset.SOPClassUID,
        }

        filename = file.split("/")[-1].replace(".", "_")

        json_data = {filename: dic}

        os.makedirs(f"{output_folder}/retinal_octa", exist_ok=True)

        with open(f"{output_folder}/retinal_octa/{filename}.json", "w") as json_file:
            json.dump(json_data, json_file)

        print(json_data)

        return dic
