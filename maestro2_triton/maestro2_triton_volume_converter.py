import os

import pydicom
from maestro2_triton.maestro2_triton_volume_converter_functional_groups import (
    acquisition_method_algorithm_sequence,
    octb_scan_analysis_acquisition_parameters_sequence,
    shared_functional_groups_sequence,
    per_frame_functional_groups_sequence,
    dimension_index_sequence,
)


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

    def __init__(self, name, header_elements, elements, sequences):
        self.name = name
        self.header_elements = header_elements
        self.elements = elements
        self.sequences = sequences

    def header_tags(self):
        """
        Extracts unique tags from header elements.

        :return: List of unique header element tags.
        """
        headertags = set()
        for header_element in self.header_elements:
            headertags.add(header_element.tag)
        return list(headertags)

    def tags(self):
        """
        Extracts unique tags from individual elements.

        :return: List of unique element tags.
        """
        tags = set()
        for element in self.elements:
            tags.add(element.tag)

        return list(tags)

    def sequence_tags(self):
        """
        Generates a dictionary of sequence tags and associated element tags.

        :return: Dictionary with sequence tags as keys and lists of lists of element tags as values.
        """
        tags_dict = {}
        for sequence in self.sequences:
            element_lists_tags = []
            for element_list in sequence.element_lists:
                element_tags = [element.tag for element in element_list]
                element_lists_tags.append(element_tags)
            tags_dict[sequence.tag] = element_lists_tags

        return tags_dict


class HeaderElement:
    """
    Represents a header element in a dataset.

    This class defines a header element with attributes such as its name, tag,
    and value representation (vr).

    Attributes:
        name (str): The name of the header element.
        tag (str): The tag associated with the header element.
        vr (str): The value representation of the header element.
    """

    def __init__(self, name, tag, vr):
        self.name = name
        self.tag = tag
        self.vr = vr


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


class Sequence:
    """
    Represents a sequence of data elements in a dataset.

    This class defines a sequence of data elements with attributes such as its name, tag,
    value representation (vr), and associated element lists and sub-sequences.

    Attributes:
        name (str): The name of the sequence.
        tag (str): The tag associated with the sequence.
        vr (str): The value representation of the sequence.
        element_lists (list): List of ElementList instances representing element lists.
        sequences (list): List of nested Sequence instances.
    """

    def __init__(self, name, tag, vr, *element_lists, sequences=None):
        self.name = name
        self.tag = tag
        self.vr = vr
        self.element_lists = element_lists if element_lists else []
        self.sequences = sequences if sequences is not None else []


octa_volume = ConversionRule(
    "OCTA Volume",
    [
        HeaderElement("FileMetaInformationGroupLength", "00020000", "UL"),
        HeaderElement("FileMetaInformationVersion", "00020001", "OB"),
        HeaderElement("MediaStorageSOPClassUID", "00020002", "UI"),
        HeaderElement("MediaStorageSOPInstanceUID", "00020003", "UI"),
        HeaderElement("TransferSyntaxUID", "00020010", "UI"),
        HeaderElement("ImplementationClassUID", "00020012", "UI"),
        HeaderElement("ImplementationVersionName", "00020013", "SH"),
    ],
    [
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
        Element("SeriesInstanceUID", "0020000E", "UI"),
        Element("SeriesNumber", "00200011", "IS"),
        Element("Modality", "00080060", "CS"),
        Element("FrameofReferenceUID", "00200052", "UI"),
        Element("PositionReferenceIndicator", "00201040", "LO"),
        Element("Manufacturer", "00080070", "LO"),
        Element("ManufacturerModelName", "00081090", "LO"),
        Element("DeviceSerialNumber", "00181000", "LO"),
        Element("SoftwareVersions", "00181020", "LO"),
        Element("SamplePerPixel", "00280002", "US"),
        Element("PhotometricInterpretation", "00280004", "CS"),
        Element("Rows", "00280010", "US"),
        Element("Columns", "00280011", "US"),
        Element("BitsAllocated", "00280100", "US"),
        Element("BitsStored", "00280101", "US"),
        Element("HighBit", "00280102", "US"),
        Element("PixelRepresentation", "00280103", "US"),
        Element("ImageType", "00080008", "CS"),
        Element("InstanceNumber", "00200013", "IS"),
        Element("ContentDate", "00080023", "DA"),
        Element("ContentTime", "00080033", "TM"),
        Element("BitsAllocated", "00280100", "US"),
        Element("BitsStored", "00280101", "US"),
        Element("HighBit", "00280102", "US"),
        Element("SamplesPerPixel", "00280002", "US"),
        Element("PhotometricInterpretation", "00280004", "CS"),
        Element("PixelRepresentation", "00280103", "US"),
        Element("PresentationLUTShape", "20500020", "CS"),
        Element("LossyImageCompression", "00282110", "CS"),
        Element("LossyImageCompressionRatio", "00282112", "DS"),
        Element("LossyImageCompressionMethod", "00282114", "CS"),
        Element("BurnedInAnnotation", "00280301", "CS"),
        Element("RecognizableVisualFeatures", "00280302", "CS"),
        Element("ConcatenationFrameOffsetNumber", "00209228", "UL"),
        Element("InConcatenationNumber", "00209162", "US"),
        Element("InConcatenationTotalNumber", "00209163", "US"),
        Element("InstanceNumber", "00200013", "IS"),
        Element("ContentDate", "00080023", "DA"),
        Element("ContentTime", "00080033", "TM"),
        Element("NumberOfFrames", "00280008", "IS"),
        Element("SOPClassUID", "00080016", "UI"),
        Element("SOPInstanceUID", "00080018", "UI"),
        Element("SpecificCharacterSet", "00080005", "CS"),
        Element("ImageLaterality", "00200062", "CS"),
        Element("PresentationIntentType ", "00080068", "CS"),
        Element("Series Description", "0008103E", "LO"),
    ],
    [
        Sequence(
            "DimensionOrganizationSequence",
            "00209221",
            "SQ",
            [Element("DimensionOrganizationUID", "00209164", "UI")],
        ),
    ],
)


def process_tags(tags, dicom):
    """
    Process DICOM tags and create a structured output.

    This function processes a list of DICOM tags and their values, creating a structured output
    containing information about each tag and its associated values.

    Args:
        tags (list): List of DICOM tags to be processed.
        dicom (dict): Dictionary containing DICOM tag information.

    Returns:
        dict: A structured output containing processed DICOM tag information.
    """
    output = dict()
    for tag in tags:
        if tag in dicom:
            element_name = pydicom.datadict.keyword_for_tag(tag)
            vr = dicom[tag]["vr"]
            value = dicom[tag].get("Value", [])

            if not value or not isinstance(value[0], dict):
                output[tag] = DicomEntry(tag, element_name, vr, value)
            else:
                nested_output = []
                for item in value:
                    keys_list = list(item.keys())
                    nested_output.append(process_tags(keys_list, item))
                output[tag] = DicomEntry(tag, element_name, vr, nested_output)
    return output


class DicomEntry:
    """
    Represents an entry containing DICOM tag information.

    This class defines an entry containing attributes such as the DICOM tag, its name,
    value representation (vr), and the associated value.

    Attributes:
        tag (str): The DICOM tag associated with the entry.
        name (str): The name of the DICOM tag.
        vr (str): The value representation of the DICOM tag.
        value (Any): The value associated with the DICOM tag.

    Methods:
        is_empty(): Checks if the value of the DICOM entry is empty.
    """

    def __init__(self, tag, name, vr, value):
        self.tag = tag
        self.name = name
        self.vr = vr
        self.value = value

    def is_empty(self):
        return len(self.value) == 0


def extract_dicom_dict(file, tags):
    """
    Extract DICOM information from a file and create a structured dictionary.

    This function reads a DICOM file, extracts relevant header and data information,
    and creates a structured dictionary containing header elements, metadata,
    and processed DICOM tag information.

    Args:
        file (str): Path to the DICOM file.
        tags (list): List of DICOM tags to be processed.

    Returns:
        tuple: A tuple containing the structured dictionary, transfer syntax information,
               and pixel data of the DICOM file.
    """
    if not os.path.exists(file):
        raise FileNotFoundError(f"File {file} not found.")

    output = dict()
    output["filepath"] = file

    dataset = pydicom.dcmread(file)

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
        "00020010": {"vr": "UI", "Value": [dataset.file_meta.TransferSyntaxUID]},
        "00020012": {"vr": "UI", "Value": [dataset.file_meta.ImplementationClassUID]},
        "00020013": {
            "vr": "SH",
            "Value": [dataset.file_meta.ImplementationVersionName],
        },
    }

    json_dict = {}
    json_dict.update(header_elements)
    info = dataset.to_json_dict()

    patient_name = dataset.PatientName
    info["00100010"]["Value"] = [patient_name]
    physician_name = dataset.ReferringPhysicianName
    info["00080090"]["Value"] = [physician_name]

    json_dict.update(info)

    dicom = json_dict

    output = process_tags(tags, dicom)

    transfersyntax = [dataset.is_little_endian, dataset.is_implicit_VR]
    pixeldata = dataset.PixelData

    return output, transfersyntax, pixeldata


def write_dicom(protocol, dicom_dict_list, file_path):
    """
    Write DICOM data based on the given protocol and dictionary.

    This function takes a conversion protocol, processed DICOM data dictionary, and an output
    file path. It writes the DICOM data to the specified output file using the provided protocol
    and dictionary.

    Args:
        protocol (ConversionRule): The conversion protocol specifying the structure of the DICOM data.
        dicom_dict_list (tuple): A tuple containing the structured DICOM dictionary,
                                transfer syntax information, and pixel data.
        file_path (str): Path to the output DICOM file.
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
        for sequence in protocol.sequences:
            if sequence.tag == key:
                desired_sequence = sequence

        if key in dicom_dict_list[0]:
            sequencetag = key

            seq = pydicom.Sequence()
            elementkeys = sequencetags[sequencetag]

            if dicom_dict_list[0][key].value:
                x = dicom_dict_list[0][key].value[0]
                key_list = list(x.keys())

                item = pydicom.Dataset()

                if isinstance(elementkeys, list):
                    for i in range(len(elementkeys)):
                        for elementkey in elementkeys[i]:
                            for sequence in desired_sequence.sequences:
                                for elements in sequence.element_lists:
                                    for element in elements:
                                        if element.tag == elementkey:
                                            desired_element = element

                            if (
                                elementkey in key_list
                                and desired_element.decision == BLANK
                            ):
                                value = []

                            elif (
                                elementkey in key_list
                                and desired_element.decision == HARMONIZE
                            ):
                                value = desired_element.harmonized_value

                            elif elementkey in key_list:
                                value = (
                                    dicom_dict_list[0][key].value[0][elementkey].value
                                )

                            element_name = pydicom.datadict.keyword_for_tag(
                                dicom_dict_list[0][sequencetag].value[0][elementkey].tag
                            )
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

        acquisition_method_algorithm_sequence(dataset, dicom_dict_list)
        octb_scan_analysis_acquisition_parameters_sequence(dataset, dicom_dict_list)
        shared_functional_groups_sequence(dataset, dicom_dict_list)
        per_frame_functional_groups_sequence(dataset, dicom_dict_list)
        dimension_index_sequence(dataset, dicom_dict_list)

    pydicom.filewriter.write_file(file_path, dataset, write_like_original=False)


def convert_dicom(input, output):
    """
    Convert DICOM data using a specific conversion rule.

    This function takes an input DICOM file, applies the conversion rule specified in the variable
    'conversion_rule', and writes the converted DICOM data to the output file.

    Args:
        input (str): Path to the input DICOM file.
        output (str): Path to the output DICOM file.
    """
    conversion_rule = octa_volume
    tags = (
        conversion_rule.header_tags()
        + conversion_rule.tags()
        + list(conversion_rule.sequence_tags().keys())
        + ["00221423", "00221640", "52009229", "52009230"]
    )
    x = extract_dicom_dict(input, tags)

    filename = input.split("/")[-1]

    write_dicom(conversion_rule, x, f"{output}/converted_{filename}")
