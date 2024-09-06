import os
import pydicom
import imaging.imaging_utils as imaging_utils
import json


def meta_data_save(filename, output_folder):
    """
    Extracts metadata from a DICOM file and saves it as a JSON file in the specified output folder.

    The function reads the DICOM file, extracts relevant metadata, and saves it as a JSON file in the output folder.

    Args:
        filename (str): Full path to the DICOM *.dcm file.
        output_folder (str): Full path to the folder where the output metadata JSON file will be saved.

    Returns:
        dict: A dictionary containing the extracted metadata.
    """

    dataset = pydicom.dcmread(filename)

    start_index = filename.find("/retinal_flio")
    file = filename[start_index:]

    patient_id = dataset.get("PatientID", "")

    manufacturer = "Heidelberg"
    device = "Flio"

    wavelength = next(
        (
            wavelength.replace("_", " ").capitalize()
            for wavelength in ["short_wavelength", "long_wavelength"]
            if wavelength in filename
        ),
        "unknown_wavelength",
    )

    laterality = next(
        (
            laterality.strip("_").upper()
            for laterality in ["_l_", "_r_"]
            if laterality in filename
        ),
        "unknown_laterality",
    )

    height = dataset.Rows
    width = dataset.Columns
    number_of_frames = dataset.NumberOfFrames
    sop_instance_uid = dataset.SOPInstanceUID

    dic = {
        "participant_id": patient_id,
        "manufacturer": manufacturer,
        "manufacturers_model_name": device,
        "laterality": laterality,
        "wavelength": wavelength,
        "height": height,
        "width": width,
        "number_of_frames": number_of_frames,
        "filepath": file,
        "sop_instance_uid": sop_instance_uid,
    }

    filename = file.split("/")[-1].replace(".", "_")

    json_data = {filename: dic}

    print(json_data)

    os.makedirs(f"{output_folder}/retinal_flio", exist_ok=True)

    with open(f"{output_folder}/retinal_flio/{filename}.json", "w") as json_file:
        json.dump(json_data, json_file)

    return dic
