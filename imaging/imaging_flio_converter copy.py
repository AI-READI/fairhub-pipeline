import os
import pydicom
from bs4 import BeautifulSoup
import flio_reader
from pydicom.dataset import Dataset, FileMetaDataset
from pydicom.uid import ImplicitVRLittleEndian
from flio_reader import get_array
import numpy as np
import json


def get_all_file_names(folder_path):
    """
    Get all file names in a specified folder, including subdirectories.

    Args:
        folder_path (str): The path to the folder.

    Returns:
        list: A list of file paths for all files in the folder and subdirectories.
    """
    file_names = []

    for root, dirs, files in os.walk(folder_path):
        for file in files:
            file_names.append(os.path.join(root, file))

    return file_names


def print_list(file_names):
    """
    Print all file names in a list, each on a new line.

    Args:
        file_names (list): List of file paths to print.
    """
    for file_name in file_names:
        print(file_name, end="\n")


def extract_html_and_sdt(folder_path):
    """
    Extract HTML and SDT files from a specified folder, organized by laterality (OD or OS).

    Args:
        folder_path (str): The path to the folder containing files.

    Returns:
        dict: A dictionary with keys "R" and "L" for right and left eye files respectively,
              each containing a list of file paths.

    Raises:
        ValueError: If there are not exactly one HTML file and one SDT file for each eye.
    """
    files_list = get_all_file_names(folder_path)

    result = {"R": [], "L": []}

    for file in files_list:
        if "OD" in file:
            if file.endswith(".html"):
                result["R"].append(file)
            elif file.endswith(".sdt"):
                result["R"].append(file)
        elif "OS" in file:
            if file.endswith(".html"):
                result["L"].append(file)
            elif file.endswith(".sdt"):
                result["L"].append(file)

    if len(result["R"]) != 2 or len(result["L"]) != 2:
        raise ValueError(
            "There should be exactly one HTML file and one SDT file for each subfolder."
        )
    return result


def extract_dicom_info_from_html(html_file):
    """
    Extract DICOM metadata from an HTML file.

    Args:
        html_file (str): The path to the HTML file.

    Returns:
        dict: A dictionary containing extracted DICOM metadata.
    """
    with open(html_file, "r") as file:
        html_content = file.read()
        soup = BeautifulSoup(html_content, "html.parser")

        # Extracting FLIO HTML table data
        tables = soup.find_all("table")
        all_table_data = []
        for table in tables:
            table_data = []
            rows = table.find_all("tr")
            for row in rows:
                row_data = [
                    cell.get_text().strip() for cell in row.find_all(["td", "th"])
                ]
                table_data.append(row_data)
            all_table_data.append(table_data)

        # Extracting FLIO HTML unordered list data
        ul_elements = soup.find_all("ul")
        lines = []
        for ul in ul_elements:
            li_elements = ul.find_all("li")
            for li in li_elements:
                bullet_point = li.get_text(strip=True)
                line_parts = bullet_point.split(":")
                lines.extend(line_parts)

        laterality = lines[7]
        if "OD" in laterality:
            laterality = "R"
        elif "OS" in laterality:
            laterality = "L"

        # Extracting FLIO HTML paragraph data
        first_paragraph = soup.find("p")
        paragraph_text = first_paragraph.get_text()
        words = paragraph_text.split()
        first_word = words[0]
        second_word = words[1] + words[2]
        mapping_info = first_word + " " + second_word.replace(":", "")
        focus = lines[1].replace(" ", "")
        cam_sn = lines[3].replace(" ", "")
        pws_sn = lines[5].replace(" ", "")

        # Extracting patient info
        patient_name = (
            all_table_data[0][2][1].replace("-", "").replace(",", "-").replace(" ", "")
        )
        patient_ID = patient_name
        patient_sex = all_table_data[0][3][1].upper()[0]
        bdate = all_table_data[0][4][1]
        patient_birthdate = (
            bdate[6]
            + bdate[7]
            + bdate[8]
            + bdate[9]
            + bdate[0]
            + bdate[1]
            + bdate[3]
            + bdate[4]
        )

        # Extracting content date, content time, and scan duration
        data = all_table_data[1][2][1]
        content_date = (
            data[0]
            + data[1]
            + data[2]
            + data[3]
            + data[5]
            + data[6]
            + data[8]
            + data[9]
        )
        content_time = (
            data[12] + data[13] + data[15] + data[16] + data[18] + data[19] + ".000"
        )

        scan_duration = str(
            int(all_table_data[1][3][1][4]) * 60
            + int(all_table_data[1][3][1][6]) * 10
            + int(all_table_data[1][3][1][7])
        )
        mode = all_table_data[1][4][1]

        # Extracting photon per pixel information
        short_wavelength_minimal_photons_per_pixel = all_table_data[1][7][1]
        long_wavelength_minimal_photons_per_pixel = all_table_data[1][7][2]
        short_wavelength_maximal_photons_per_pixel = all_table_data[1][8][1]
        long_wavelength_maximal_photons_per_pixel = all_table_data[1][8][2]
        short_wavelength_photons_per_pixel = all_table_data[1][9][1]
        long_wavelength_photons_per_pixel = all_table_data[1][9][2]
        short_wavelength_processed_frames = all_table_data[1][10][1]
        long_wavelength_processed_frames = all_table_data[1][10][2]
        short_wavelength_valid_photons_per_frame = all_table_data[1][11][1]
        long_wavelength_valid_photons_per_frame = all_table_data[1][11][2]
        short_wavelength_invalid_photons_per_frame = all_table_data[1][12][1]
        long_wavelength_invalid_photons_per_frame = all_table_data[1][12][2]

    # Constructing the dictionary
    dicom_info = {
        "PatientName": patient_name,
        "PatientID": patient_ID,
        "PatientSex": patient_sex,
        "PatientBirthDate": patient_birthdate,
        "focus": focus,
        "cam_sn": cam_sn,
        "pws_sn": pws_sn,
        "ContentDate": content_date,
        "ContentTime": content_time,
        "ScanDuration": scan_duration,
        "Mode": mode,
        "Laterality": laterality,
        "ShortWavelengthMinimalPhotonsPerPixel": short_wavelength_minimal_photons_per_pixel,
        "LongWavelengthMinimalPhotonsPerPixel": long_wavelength_minimal_photons_per_pixel,
        "ShortWavelengthMaximalPhotonsPerPixel": short_wavelength_maximal_photons_per_pixel,
        "LongWavelengthMaximalPhotonsPerPixel": long_wavelength_maximal_photons_per_pixel,
        "ShortWavelengthPhotonsPerPixel": short_wavelength_photons_per_pixel,
        "LongWavelengthPhotonsPerPixel": long_wavelength_photons_per_pixel,
        "ShortWavelengthProcessedFrames": short_wavelength_processed_frames,
        "LongWavelengthProcessedFrames": long_wavelength_processed_frames,
        "ShortWavelengthValidPhotonsPerFrame": short_wavelength_valid_photons_per_frame,
        "LongWavelengthValidPhotonsPerFrame": long_wavelength_valid_photons_per_frame,
        "ShortWavelengthInvalidPhotonsPerFrame": short_wavelength_invalid_photons_per_frame,
        "LongWavelengthInvalidPhotonsPerFrame": long_wavelength_invalid_photons_per_frame,
        "MappingInfo": mapping_info,
    }

    return dicom_info


def make_min_info_dicom_from_sdt(sdtpath):
    """
    Create minimal DICOM datasets from an SDT file.

    Args:
        sdtpath (str): The path to the SDT file.

    Returns:
        tuple: Two DICOM datasets, one for short wavelength and one for long wavelength.
    """

    def create_dataset(ds):
        ds.PatientName = ""
        ds.PatientID = "temp"
        ds.Rows = 256
        ds.Columns = 256
        ds.SamplesPerPixel = 1
        ds.PhotometricInterpretation = "MONOCHROME2"
        ds.BitsAllocated = 16
        ds.BitsStored = 16
        ds.PixelRepresentation = 0
        ds.NumberOfFrames = 1024
        ds.is_little_endian = True
        ds.is_implicit_VR = False
        return ds

    file_meta = FileMetaDataset()
    file_meta.MediaStorageSOPClassUID = "1.2.840.10008.5.1.4.1.1.77.1.5.2"
    file_meta.TransferSyntaxUID = pydicom.uid.ExplicitVRLittleEndian
    file_meta.ImplementationClassUID = "1.2.6.1.3.2.278883.2"

    file_meta.MediaStorageSOPInstanceUID = ""

    ds_short = Dataset()
    ds_short.file_meta = file_meta
    ds_short = create_dataset(ds_short)
    ds_short.SOPClassUID = "1.2.840.10008.5.1.4.1.1.77.1.5.2"
    ds_short.SOPInstanceUID = ""

    file_meta1 = FileMetaDataset()
    file_meta1.MediaStorageSOPClassUID = "1.2.840.10008.5.1.4.1.1.77.1.5.2"
    file_meta1.TransferSyntaxUID = pydicom.uid.ExplicitVRLittleEndian
    file_meta1.ImplementationClassUID = "1.2.6.1.3.2.278883.2"

    file_meta1.MediaStorageSOPInstanceUID = ""

    ds_long = Dataset()
    ds_long.file_meta = file_meta1
    ds_long = create_dataset(ds_long)
    ds_long.SOPClassUID = "1.2.840.10008.5.1.4.1.1.77.1.5.2"
    ds_long.SOPInstanceUID = ""

    array1, array2 = get_array(sdtpath)

    ds_short.PixelData = np.transpose(
        array1.reshape(256, 256, 1024), (2, 0, 1)
    ).tobytes()
    ds_long.PixelData = np.transpose(
        array2.reshape(256, 256, 1024), (2, 0, 1)
    ).tobytes()

    return ds_short, ds_long


def short_add_html_sdt_info(dataset, sdt, dicom_info, output):
    """
    Add HTML and SDT information to a short wavelength DICOM dataset.

    Args:
        dataset (Dataset): The DICOM dataset to modify.
        sdt (str): The path to the SDT file.
        dicom_info (dict): Dictionary containing extracted DICOM metadata.
        output (str): Path to save the modified DICOM dataset.

    Returns:
        str: The output path where the DICOM dataset is saved.
    """
    dicom = dataset

    # Setting patient information
    dicom.PatientName = ""
    dicom.PatientID = dicom_info["PatientID"]
    dicom.PatientSex = "M"
    dicom.PatientBirthDate = ""
    dicom.ContentDate = dicom_info["ContentDate"]
    dicom.ContentTime = dicom_info["ContentTime"]

    # Adding FLIO HTML information
    dicom.add(pydicom.DataElement((0x0073, 0x0010), "LO", "FLIO HTML information"))
    dicom.add_new(pydicom.tag.Tag(0x0073, 0x1001), "LO", dicom_info["focus"])
    dicom.add_new(pydicom.tag.Tag(0x0073, 0x1002), "LO", dicom_info["cam_sn"])
    dicom.add_new(pydicom.tag.Tag(0x0073, 0x1003), "LO", dicom_info["pws_sn"])
    dicom.add_new(pydicom.tag.Tag(0x0073, 0x1004), "LO", dicom_info["MappingInfo"])

    dicom.add_new(pydicom.tag.Tag(0x0073, 0x1005), "LO", dicom_info["ScanDuration"])
    dicom.add_new(pydicom.tag.Tag(0x0073, 0x1006), "LO", dicom_info["Mode"])
    dicom.add_new(
        pydicom.tag.Tag(0x0073, 0x1007),
        "LO",
        dicom_info["ShortWavelengthMinimalPhotonsPerPixel"],
    )

    dicom.add_new(
        pydicom.tag.Tag(0x0073, 0x1008),
        "LO",
        dicom_info["ShortWavelengthMaximalPhotonsPerPixel"],
    )

    dicom.add_new(
        pydicom.tag.Tag(0x0073, 0x1009),
        "LO",
        dicom_info["ShortWavelengthPhotonsPerPixel"],
    )

    dicom.add_new(
        pydicom.tag.Tag(0x0073, 0x100A),
        "LO",
        dicom_info["ShortWavelengthProcessedFrames"],
    )

    dicom.add_new(
        pydicom.tag.Tag(0x0073, 0x100B),
        "LO",
        dicom_info["ShortWavelengthValidPhotonsPerFrame"],
    )

    dicom.add_new(
        pydicom.tag.Tag(0x0073, 0x100C),
        "LO",
        dicom_info["ShortWavelengthInvalidPhotonsPerFrame"],
    )

    # Adding FLIO SDT information
    r = flio_reader.dump_metadata(sdt)
    base_group_number = 0x0075
    dicom.add(pydicom.DataElement((0x0075, 0x0010), "LO", "FLIO SDT information"))

    start_index = 1
    end_index = 15
    index = 0

    # Iterate until the nth item is reached
    while index < end_index:
        start_index += 1
        index += 1
        key, value = list(r["flio_measurement_description_block_0"].items())[
            start_index
        ]  # Get key-value pair at current index
        private_tag = pydicom.tag.Tag(base_group_number, 0x1000 + index)
        dicom.add_new(private_tag, "LO", str(value))

    # Setting additional attributes
    dicom.ImageLaterality = dicom_info["Laterality"]
    dicom.Manufacturer = "Heidelberg Engineering"
    dicom.ManufacturerModelName = "FLIO"
    dicom.DeviceSerialNumber = r["flio_measurement_description_block_0"]["mod_ser_no"]
    dicom.StudyDescription = "Short Wavelength 498nm - 560nm"

    os.makedirs(os.path.dirname(output), exist_ok=True)
    return dicom.save_as(output, write_like_original=False)


def long_add_html_sdt_info(dataset, sdt, dicom_info, output):
    """
    Add HTML and SDT information to a long wavelength DICOM dataset.

    Args:
        dataset (Dataset): The DICOM dataset to modify.
        sdt (str): The path to the SDT file.
        dicom_info (dict): Dictionary containing extracted DICOM metadata.
        output (str): Path to save the modified DICOM dataset.

    Returns:
        str: The output path where the DICOM dataset is saved.
    """
    dicom = dataset

    # Setting patient information
    dicom.PatientName = ""
    dicom.PatientID = dicom_info["PatientID"]
    dicom.PatientSex = "M"
    dicom.PatientBirthDate = ""
    dicom.ContentDate = dicom_info["ContentDate"]
    dicom.ContentTime = dicom_info["ContentTime"]

    # Adding FLIO HTML information
    dicom.add(pydicom.DataElement((0x0073, 0x0010), "LO", "FLIO HTML information"))
    dicom.add_new(pydicom.tag.Tag(0x0073, 0x1001), "LO", dicom_info["focus"])
    dicom.add_new(pydicom.tag.Tag(0x0073, 0x1002), "LO", dicom_info["cam_sn"])
    dicom.add_new(pydicom.tag.Tag(0x0073, 0x1003), "LO", dicom_info["pws_sn"])
    dicom.add_new(pydicom.tag.Tag(0x0073, 0x1004), "LO", dicom_info["MappingInfo"])

    dicom.add_new(pydicom.tag.Tag(0x0073, 0x1005), "LO", dicom_info["ScanDuration"])
    dicom.add_new(pydicom.tag.Tag(0x0073, 0x1006), "LO", dicom_info["Mode"])

    dicom.add_new(
        pydicom.tag.Tag(0x0073, 0x1007),
        "LO",
        dicom_info["LongWavelengthMinimalPhotonsPerPixel"],
    )

    dicom.add_new(
        pydicom.tag.Tag(0x0073, 0x1008),
        "LO",
        dicom_info["LongWavelengthMaximalPhotonsPerPixel"],
    )

    dicom.add_new(
        pydicom.tag.Tag(0x0073, 0x1009),
        "LO",
        dicom_info["LongWavelengthPhotonsPerPixel"],
    )

    dicom.add_new(
        pydicom.tag.Tag(0x0073, 0x100A),
        "LO",
        dicom_info["LongWavelengthProcessedFrames"],
    )

    dicom.add_new(
        pydicom.tag.Tag(0x0073, 0x100B),
        "LO",
        dicom_info["LongWavelengthValidPhotonsPerFrame"],
    )

    dicom.add_new(
        pydicom.tag.Tag(0x0073, 0x100C),
        "LO",
        dicom_info["LongWavelengthInvalidPhotonsPerFrame"],
    )

    # Adding FLIO SDT information
    r = flio_reader.dump_metadata(sdt)
    base_group_number = 0x0075
    dicom.add(pydicom.DataElement((0x0075, 0x0010), "LO", "FLIO SDT information"))

    start_index = 1
    end_index = 15
    index = 0

    # Iterate until the nth item is reached
    while index < end_index:
        start_index += 1
        index += 1
        key, value = list(r["flio_measurement_description_block_1"].items())[
            start_index
        ]  # Get key-value pair at current index
        private_tag = pydicom.tag.Tag(base_group_number, 0x1000 + index)
        dicom.add_new(private_tag, "LO", str(value))

    # Setting additional attributes
    dicom.ImageLaterality = dicom_info["Laterality"]
    dicom.Manufacturer = "Heidelberg Engineering"
    dicom.ManufacturerModelName = "FLIO"
    dicom.DeviceSerialNumber = r["flio_measurement_description_block_0"]["mod_ser_no"]
    dicom.StudyDescription = "Long Wavelength 560nm - 720nm"

    os.makedirs(os.path.dirname(output), exist_ok=True)

    return dicom.save_as(output, write_like_original=False)


def make_flio_dicom(inputsdt, inputhtml, output, json_path):
    """
    Create FLIO DICOM files from SDT and HTML files.

    Args:
        inputsdt (str): Path to the SDT file.
        inputhtml (str): Path to the HTML file.
        output (str): Directory path to save the output DICOM files.
        json_path (str): Path to the JSON file containing UID information.

    Returns:
        dict: A dictionary with the status of the short and long wavelength DICOM file conversions.
    """

    a, b = make_min_info_dicom_from_sdt(inputsdt)
    dicom_info = extract_dicom_info_from_html(inputhtml)

    with open(json_path, "r") as file:
        data = json.load(file)

    patientid = dicom_info["PatientID"][-4:]
    content_time = str(dicom_info["ContentTime"])[0:5]
    laterality = dicom_info["Laterality"]

    uid_short = data[patientid][laterality]["short_uid"]
    uid_short = uid_short[:-5] + content_time

    uid_long = data[patientid][laterality]["long_uid"]
    uid_long = uid_long[:-5] + content_time

    a.file_meta.MediaStorageSOPInstanceUID = uid_short

    a.SOPInstanceUID = uid_short
    a.StudyInstanceUID = uid_short
    a.SeriesInstanceUID = uid_short
    a.SynchronizationFrameOfReferenceUID = uid_short

    b.file_meta.MediaStorageSOPInstanceUID = uid_long

    b.SOPInstanceUID = uid_long
    b.StudyInstanceUID = uid_long
    b.SeriesInstanceUID = uid_long
    b.SynchronizationFrameOfReferenceUID = uid_long

    patientid = dicom_info["PatientID"][-4:]
    laterality = dicom_info["Laterality"].lower()

    # Define output file paths
    short_output_path = (
        f"{output}/{patientid}_flio_short_wavelength_{laterality}_{uid_short}.dcm"
    )
    long_output_path = (
        f"{output}/{patientid}_flio_long_wavelength_{laterality}_{uid_long}.dcm"
    )

    # Process short wavelength
    try:
        short_add_html_sdt_info(a, inputsdt, dicom_info, short_output_path)
        short_status = "complete", short_output_path.split("/")[-1]
    except Exception as e:
        short_status = f"error: {e}"

    # Process long wavelength
    try:
        long_add_html_sdt_info(b, inputsdt, dicom_info, long_output_path)
        long_status = "complete", long_output_path.split("/")[-1]
    except Exception as e:
        long_status = f"error: {e}"

    # Create and print the dictionary with completion status
    dic = {
        "Input SDT": inputsdt.split("/")[-2:],
        "Input HTML": inputhtml.split("/")[-2:],
        "Short wavelength conversion": short_status,
        "Long wavelength conversion": long_status,
    }

    print(dic)
    return dic


KEEP = 0
BLANK = 1
HARMONIZE = 2


class ConversionRule:
    """
    Represents a conversion rule for processing data.

    This class defines a rule used for data conversion and processing. It contains attributes
    such as the rule's name, header elements, individual elements, and sequences of elements.

    Attributes:
        name (str): The name of the conversion rule.
        header_elements (list): List of Element instances representing header elements.
        elements (list): List of Element instances representing individual elements.
        sequences (list): List of Sequence instances representing sequences of elements.

    Methods:
        header_tags(): Extracts unique tags from header elements.
        tags(): Extracts unique tags from individual elements.
        sequence_tags(): Generates a dictionary of sequence tags and associated element tags.

    """

    def __init__(self, name, headers, elements, sequences):
        self.name = name
        self.header_elements = headers
        self.elements = elements
        self.sequences = sequences

    def header_tags(self):
        headertags = set()
        for header_element in self.header_elements:
            headertags.add(header_element.tag)

        return list(headertags)

    def tags(self):
        tags = set()
        for element in self.elements:
            tags.add(element.tag)
        return list(tags)

    def sequence_tags(self):
        tags_dict = {}
        for sequence in self.sequences:
            element_tags = [element.tag for element in sequence.elements]
            tags_dict[sequence.tag] = element_tags

        return tags_dict


class Element:
    """
    Represents an individual data element.

    This class defines an individual data element with attributes such as its name, tag,
    value representation (vr), decision, and harmonized value.

    Attributes:
        name (str): The name of the data element.
        tag (str): The tag associated with the data element.
        vr (str): The value representation of the data element.
        decision (int): The decision related to the data element (default is 0).
        harmonized_value (int): The harmonized value of the data element (default is 0).
    """

    def __init__(self, name, tag, vr, decision=0, harmonized_value=0):
        self.name = name
        self.tag = tag
        self.vr = vr
        self.decision = decision
        self.harmonized_value = harmonized_value


class ElementList:
    """
    Represents a list of related data elements.

    This class defines a list of related data elements with attributes such as its name,
    tag, value representation (vr), and the list of elements.

    Attributes:
        name (str): The name of the element list.
        tag (str): The tag associated with the element list.
        vr (str): The value representation of the element list.
        elements (list): List of Element instances representing the data elements in the list (default is an empty list).
    """

    def __init__(self, name, tag, vr, elements=None):
        self.name = name
        self.tag = tag
        self.vr = vr
        self.elements = elements if elements is not None else []


flio = ConversionRule(
    "flio",
    # DICOM header elements
    headers=[
        Element("FileMetaInformationGroupLength", "00020000", "UL"),
        Element("FileMetaInformationVersion", "00020001", "OB"),
        Element("MediaStorageSOPClassUID", "00020002", "UI"),
        Element("MediaStorageSOPInstanceUID", "00020003", "UI"),
        Element("TransferSyntaxUID", "00020010", "UI"),
        Element("ImplementationClassUID", "00020012", "UI"),
        Element("ImplementationVersionName", "00020013", "SH"),
    ],
    # DICOM elements
    elements=[
        Element("PatientName", "00100010", "PN"),
        Element("PatientID", "00100020", "LO"),
        Element("PatientBirthDate", "00100030", "DA"),
        Element("PatientSex", "00100040", "CS"),
        Element("StudyInstanceUID", "0020000D", "UI"),
        Element("StudyDate", "00080020", "DM"),
        Element("StudyTime", "00080030", "TM"),
        Element("ReferringPhysicianName", "00080090", "PN", BLANK),
        Element("StudyID", "00200010", "SH", BLANK),
        Element("AccessionNumber", "00080050", "SH", BLANK),
        Element("Modality", "00080060", "CS", HARMONIZE, "FLIO"),
        Element("SeriesInstanceUID", "0020000E", "UI"),
        Element("SeriesNumber", "00200011", "IS"),
        Element("SynchronizationFrameOfReferenceUID", "00200200", "UI"),
        Element("SynchronizationTrigger", "0018106A", "CS", HARMONIZE, "NO TRIGGER"),
        Element("AcquisitionTimeSynchronized", "00181800", "CS", HARMONIZE, "NO"),
        Element("Manufacturer", "00080070", "LO"),
        Element("ManufacturerModelName", "00081090", "LO"),
        Element("DeviceSerialNumber", "00181000", "LO"),
        Element("SoftwareVersions", "00181020", "LO"),
        Element("InstanceNumber", "00200013", "IS", HARMONIZE, "1"),
        Element("PatientOrientation", "00200020", "CS"),
        Element("BurnedInAnnotation", "00280301", "CS", HARMONIZE, "NO"),
        Element("StudyDescription", "00081030", "LO"),
        Element("SamplesPerPixel", "00280002", "US"),
        Element("PhotometricInterpretation", "00280004", "CS"),
        Element("Rows", "00280010", "US"),
        Element("Columns", "00280011", "US"),
        Element("BitsAllocated", "00280100", "US"),
        Element("BitsStored", "00280101", "US"),
        Element("HighBit", "00280102", "US", HARMONIZE, 15),
        Element("PixelRepresentation", "00280103", "US"),
        Element("PlanarConfiguration", "00280006", "US"),
        Element("FrameTime", "00181063", "DS", HARMONIZE, "0.0000125"),
        Element("FrameTimeVector", "00181065", "DS", HARMONIZE, "0.0000125"),
        Element("StartTrim", "00082142", "IS", HARMONIZE, "1"),
        Element("StopTrim", "00082143", "IS", HARMONIZE, "1024"),
        Element("NumberOfFrames", "00280008", "IS"),
        Element("FrameIncrementPointer", "00280009", "AT", HARMONIZE, "00181063"),
        Element("ImageType", "00080008", "CS"),
        Element("PixelSpacing", "00280030", "DS"),
        Element("ContentTime", "00080033", "TM"),
        Element("ContentDate", "00080023", "DA"),
        Element("AcquisitionDateTime", "0008002A", "DT"),
        Element("LossyImageCompression", "00282110", "CS", HARMONIZE, "NO"),
        Element("LossyImageCompressionRatio", "00282112", "DS"),
        Element("LossyImageCompressionMethod", "00282114", "CS"),
        Element("PresentationLUTShape", "20500020", "CS", HARMONIZE, "IDENTITY"),
        Element("BurnedInAnnotation", "00280301", "CS", HARMONIZE, "NO"),
        Element("ImageLaterality", "00200062", "CS"),
        Element("PatientEyeMovementCommanded", "00220005", "CS", HARMONIZE, "NO"),
        Element("EmmetropicMagnification", "0022000A", "FL"),
        Element("IntraOcularPressure", "0022000B", "FL"),
        Element("DegreeofDilation", "0022000E", "FL"),
        Element("HorizontalFieldOfView", "0022000C", "FL"),
        Element("PupilDilated", "0022000D", "CS", HARMONIZE, "YES"),
        Element("DetectorType", "00187004", "CS"),
        Element(
            "SOPClassUID",
            "00080016",
            "UI",
            HARMONIZE,
            "1.2.840.10008.5.1.4.1.1.77.1.5.2",
        ),
        Element("SOPInstanceUID", "00080018", "UI"),
        Element("SpecificCharacterSet", "00080005", "CS", HARMONIZE, "ISO_IR 192"),
        Element("LightPathFilterPass-ThroughWavelgnth", "00220001", "US"),
        Element("LightPathFilterPassBand", "00220002", "US"),
        Element("ImagePathFilterPass-ThroughWavelength", "00220003", "US"),
        Element("ImagePathFilterPassBand", "00220004", "US"),
        Element("CameraAngleOfView", "0022001E", "FL"),
    ],
    # DICOM sequences
    sequences=[
        ElementList("LightPathFilterTypeStackCodeSequence", "00220017", "SQ"),
        ElementList("ImagePathFilterTypeStackCodeSequence", "00220018", "SQ"),
        ElementList("RefractiveStateSequence", "0022001B", "SQ"),
        ElementList("MydriaticAgentSequence", "00220058", "SQ"),
        ElementList(
            "LensesCodeSequence",
            "00220019",
            "SQ",
            [
                Element("CodeValue", "00080100", "SH"),
                Element("CodingSchemeDesignator", "00080102", "SH"),
                Element("CodeMeaning", "00080104", "LO"),
            ],
        ),
        ElementList(
            "IlluminationTypeCodeSequence",
            "00220016",
            "SQ",
            [
                Element("CodeValue", "00080100", "SH", HARMONIZE, "xxxx2"),
                Element("CodingSchemeDesignator", "00080102", "SH", HARMONIZE, "DCM"),
                Element("CodeMeaning", "00080104", "LO", HARMONIZE, "Shortpulselaser"),
            ],
        ),
        ElementList(
            "AnatomicRegionSequence",
            "00082218",
            "SQ",
            [
                Element("CodeValue", "00080100", "SH", HARMONIZE, "5665001"),
                Element("CodingSchemeDesignator", "00080102", "SH", HARMONIZE, "SCT"),
                Element("CodeMeaning", "00080104", "LO", HARMONIZE, "Retina"),
            ],
        ),
        ElementList(
            "AcquisitionDeviceTypeCodeSequence",
            "00220015",
            "SQ",
            [
                Element("CodeValue", "00080100", "SH", HARMONIZE, "xxxx1"),
                Element("CodingSchemeDesignator", "00080102", "SH", HARMONIZE, "DCM"),
                Element("CodeMeaning", "00080104", "LO", HARMONIZE, "FLIO"),
            ],
        ),
    ],
)


def process_tags(tags, dicom):
    """
    Process DICOM tags and create a dictionary of DicomEntry instances.

    This function processes a list of DICOM tags from a given DICOM dictionary and constructs
    a dictionary of DicomEntry instances representing the metadata associated with each tag.

    Args:
        tags (list): List of DICOM tags to process.
        dicom (dict): The DICOM dictionary containing the metadata.

    Returns:
        dict: Dictionary where keys are DICOM tags and values are DicomEntry instances.

    """

    output = dict()
    for tag in tags:
        if tag in dicom:
            element_name = pydicom.tag.Tag(tag)
            vr = dicom[tag]["vr"]
            value = dicom[tag].get(
                "Value", []
            )  # Assign [] as value if "Value" key is not present

            if not value or not isinstance(value[0], dict):
                output[tag] = DicomEntry(tag, element_name, vr, value)
            else:
                data = dicom[tag]["Value"][0]
                keys_list = list(data.keys())
                nested_output = process_tags(keys_list, data)
                output[tag] = DicomEntry(tag, element_name, vr, [nested_output])
    return output


class DicomEntry:
    """
    Represents a DICOM metadata entry.

    This class encapsulates information about a single DICOM metadata entry, including its
    tag, name, value representation (vr), and associated value.

    Attributes:
        tag (str): The DICOM tag of the metadata entry.
        name (pydicom.tag.Tag): The name (tag) associated with the metadata entry.
        vr (str): The value representation of the metadata entry.
        value (list): The value associated with the metadata entry.

    Methods:
        is_empty(): Checks if the value of the metadata entry is empty.

    """

    def __init__(self, tag, name, vr, value):
        self.tag = tag
        self.name = name
        self.vr = vr
        self.value = value

    def is_empty(self):
        return len(self.value) == 0


def convert_bytes(obj):
    if isinstance(obj, bytes):
        return obj.decode("utf-8")  # Decode bytes to string
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def extract_dicom_dict(file, tags):
    """
    Extract DICOM metadata and information from a DICOM file.

    This function reads a DICOM file, extracts metadata, header elements, and specific
    tags from it. It then processes the tags to create a dictionary representation of
    the DICOM metadata, along with information about transfer syntax and pixel data.

    Args:
        file (str): The path to the DICOM file.
        tags (list): List of DICOM tags to process.

    Returns:
        tuple: A tuple containing:
            - dict: Dictionary representation of DICOM metadata with processed tags.
            - list: List of transfer syntax information.
            - bytes: Pixel data from the DICOM file.

    Raises:
        FileNotFoundError: If the specified DICOM file does not exist.

    """
    if not os.path.exists(file):
        raise FileNotFoundError(f"File {file} not found.")

    output = dict()
    output["filepath"] = file

    dataset = pydicom.dcmread(file)

    dataset.PatientOrientation = ["L", "F"]
    dataset.StudyDate = dataset.ContentDate
    dataset.StudyTime = dataset.ContentTime
    dataset.SoftwareVersions = dataset["00731004"].value
    dataset.ImageType = ["ORIGINAL", "PRIMARY"]
    dataset.AcquisitionDateTime = str(dataset.ContentDate) + str(dataset.ContentTime)

    if "short" in file:
        dataset.ImagePathFilterPassBand = 498, 560

    else:
        dataset.ImagePathFilterPassBand = 560, 720

    header_elements = {
        "00020000": {
            "vr": "UL",
            "Value": [dataset.file_meta.FileMetaInformationGroupLength],
        },
        "00020001": {
            "vr": "OB",
            "Value": [dataset.file_meta.FileMetaInformationVersion],
        },
        "00020002": {"vr": "UI", "Value": [dataset.file_meta.MediaStorageSOPClassUID]},
        "00020003": {
            "vr": "UI",
            "Value": [dataset.file_meta.MediaStorageSOPInstanceUID],
        },
        "00020010": {
            "vr": "UI",
            "Value": [dataset.file_meta.TransferSyntaxUID],
        },
        "00020012": {
            "vr": "UI",
            "Value": [dataset.file_meta.ImplementationClassUID],
        },
        "00020013": {
            "vr": "SH",
            "Value": [dataset.file_meta.ImplementationVersionName],
        },
    }
    json_dict = {}
    json_dict.update(header_elements)
    info = dataset.to_json_dict()

    json_dict.update(info)

    dicom = json_dict

    output = process_tags(tags, dicom)

    transfersyntax = [dataset.is_little_endian, dataset.is_implicit_VR]
    pixeldata = dataset.PixelData

    return output, transfersyntax, pixeldata


# Function to add a tag to a dataset
def add_tag(dataset, tag, VR, value=""):
    dataset.add_new(tag, VR, value)


# Define the tags to be extracted from the source DICOM file
tags_to_extract = [
    "00730010",
    "00731001",
    "00731002",
    "00731003",
    "00731004",
    "00731005",
    "00731006",
    "00731007",
    "00731008",
    "00731009",
    "0073100a",
    "0073100b",
    "0073100c",
    "00750010",
    "00751001",
    "00751002",
    "00751003",
    "00751004",
    "00751005",
    "00751006",
    "00751007",
    "00751008",
    "00751009",
    "0075100a",
    "0075100b",
    "0075100c",
    "0075100d",
    "0075100e",
    "0075100f",
]


def write_dicom(protocol, dicom_dict_list, file_path, input):
    """
    Write DICOM data to a new DICOM file.

    This function takes a protocol, a list of DICOM dictionaries, and a file path. It constructs
    a new DICOM dataset using the provided protocol and DICOM dictionaries. The dataset is then
    written to a new DICOM file at the specified path.

    Args:
        protocol (ConversionRule): The ConversionRule instance containing processing instructions.
        dicom_dict_list (list): List containing DICOM dictionaries and related information.
        file_path (str): The path to the new DICOM file to be created.

    """
    headertags = protocol.header_tags()
    tags = protocol.tags()
    sequencetags = protocol.sequence_tags()

    file_meta = pydicom.Dataset()

    for headertag in headertags:
        value = dicom_dict_list[0][headertag].value
        element_name = pydicom.datadict.keyword_for_tag(
            dicom_dict_list[0][headertag].tag
        )
        setattr(file_meta, element_name, value)

    dataset = pydicom.Dataset()
    dataset.file_meta = file_meta

    for tag in tags:
        for element in protocol.elements:
            if element.tag == tag:
                desired_element = element

        if desired_element.decision == BLANK:
            value = []

        elif desired_element.decision == HARMONIZE:
            value = [desired_element.harmonized_value]

        elif tag in dicom_dict_list[0]:
            value = dicom_dict_list[0][tag].value

        else:
            value = []

        element_name = (
            pydicom.datadict.keyword_for_tag(dicom_dict_list[0][tag].tag)
            if tag in dicom_dict_list[0]
            else pydicom.datadict.keyword_for_tag(tag)
        )
        setattr(dataset, element_name, value)

    dataset.is_little_endian = dicom_dict_list[1][0]
    dataset.is_implicit_VR = dicom_dict_list[1][1]
    dataset.PixelData = dicom_dict_list[2]

    keys = list(sequencetags.keys())

    for key in keys:
        # print(f"{key}")
        # print(f"{dicom_dict_list[0]}")
        for sequence in protocol.sequences:
            if sequence.tag == key:
                desired_sequence = sequence

        if key in dicom_dict_list[0]:
            "yes-1"
            sequencetag = key
            seq = pydicom.Sequence()
            elementkeys = sequencetags[sequencetag]

            if dicom_dict_list[0][key].value:
                x = dicom_dict_list[0][key].value[0]
                key_list = list(x.keys())

                item = pydicom.Dataset()
                for elementkey in elementkeys:
                    for element in desired_sequence.elements:
                        if element.tag == elementkey:
                            desired_element = element
                    if elementkey in key_list and desired_element.decision == BLANK:
                        value = []
                    elif (
                        elementkey in key_list and desired_element.decision == HARMONIZE
                    ):
                        value = desired_element.harmonized_value
                    elif elementkey in key_list:
                        value = dicom_dict_list[0][key].value[0][elementkey].value
                    element_tag = (
                        dicom_dict_list[0][sequencetag].value[0][elementkey].tag
                    )
                    element_name = pydicom.datadict.keyword_for_tag(element_tag)
                    setattr(item, element_name, value)
                seq.append(item)

                value = seq
                element_name = pydicom.datadict.keyword_for_tag(
                    dicom_dict_list[0][key].tag
                )
                setattr(dataset, element_name, value)

            else:
                value = seq
                element_name = pydicom.datadict.keyword_for_tag(
                    dicom_dict_list[0][key].tag
                )
                setattr(dataset, element_name, value)
        else:
            value = pydicom.Sequence()
            element_name = pydicom.datadict.keyword_for_tag(key)
            setattr(dataset, element_name, value)

    ##########
    source_ds = pydicom.dcmread(input)
    extracted_tags = []
    for tag in tags_to_extract:
        if tag in source_ds:
            element = source_ds[tag]
            extracted_tags.append((tag, element.VR, element.value))

    # Add each extracted tag to the target dataset
    for tag, VR, value in extracted_tags:
        add_tag(dataset, tag, VR, value)

    pydicom.filewriter.write_file(file_path, dataset, write_like_original=False)


def convert_dicom(input, output):
    """
    Convert DICOM data from an input file to an output file using a conversion rule.

    This function facilitates the conversion of DICOM data from an input file to an output file.
    It uses a specified conversion rule to process the data and writes the converted data to the
    output file.

    Args:
        input (str): The path to the input DICOM file.
        output (str): The path to the output DICOM file to be created.

    """
    conversion_rule = flio
    tags = (
        conversion_rule.header_tags()
        + conversion_rule.tags()
        + list(conversion_rule.sequence_tags().keys())
    )
    x = extract_dicom_dict(input, tags)

    filename = input.split("/")[-1]

    try:
        # Attempt to write the converted DICOM file
        write_dicom(conversion_rule, x, f"{output}/converted_{filename}", input)

        dic = {
            "Input": input.split("/")[-1],
            "Output": f"converted_{filename}",
            "Error": "None",
        }

        print(dic)

        return dic

    except Exception as e:
        dic = {
            "Input": input.split("/")[-1],
            "Output": f"converted_{filename}",
            "Error": f"error: {e}",
        }

        print(dic)
        return dic
