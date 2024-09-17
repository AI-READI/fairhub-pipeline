import os
import pydicom
from cirrus.cirrus_enface_converter_functional_groups import (
    source_image_sequence,
    derivation_algorithm_sequence,
    enface_volume_descriptor_sequence,
    referenced_series_sequence,
    ophthalmic_image_type_code_sequence,
    ophthalmic_frame_location_sequence,
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
        """
        Initializes a HeaderElement instance.

        Args:
            name (str): The name of the header element.
            tag (str): The tag associated with the header element.
            vr (str): The value representation of the header element.
        """
        self.name = name
        self.tag = tag
        self.vr = vr


class Element:
    """
    Represents an element in a dataset.

    This class defines an element with attributes such as its name, tag, value representation (vr),
    decision, and harmonized value.

    Attributes:
        name (str): The name of the element.
        tag (str): The tag associated with the element.
        vr (str): The value representation of the element.
        decision (int, optional): The decision value associated with the element. Default is 0.
        harmonized_value (int, optional): The harmonized value of the element. Default is 0.
    """

    def __init__(self, name, tag, vr, decision=0, harmonized_value=0):
        """
        Initializes an Element instance.

        Args:
            name (str): The name of the element.
            tag (str): The tag associated with the element.
            vr (str): The value representation of the element.
            decision (int, optional): The decision value associated with the element. Default is 0.
            harmonized_value (int, optional): The harmonized value of the element. Default is 0.
        """
        self.name = name
        self.tag = tag
        self.vr = vr
        self.decision = decision
        self.harmonized_value = harmonized_value


class Sequence:
    """
    Represents a sequence in a dataset.

    This class defines a sequence with attributes such as its name, tag, value representation (vr),
    and a list of elements or nested sequences.

    Attributes:
        name (str): The name of the sequence.
        tag (str): The tag associated with the sequence.
        vr (str): The value representation of the sequence.
        element_lists (list): The list of elements associated with the sequence.
        sequences (list, optional): The list of nested sequences within the sequence. Default is an empty list.
    """

    def __init__(self, name, tag, vr, *element_lists, sequences=None):
        """
        Initializes a Sequence instance.

        Args:
            name (str): The name of the sequence.
            tag (str): The tag associated with the sequence.
            vr (str): The value representation of the sequence.
            *element_lists (list): Variable length argument list of elements associated with the sequence.
            sequences (list, optional): The list of nested sequences within the sequence. Default is None.
        """
        self.name = name
        self.tag = tag
        self.vr = vr
        self.element_lists = element_lists if element_lists else []
        self.sequences = sequences if sequences is not None else []


enface = ConversionRule(
    "En Face",
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
        Element("Modality", "00080060", "CS"),
        Element("SeriesInstanceUID", "0020000E", "UI"),
        Element("SeriesNumber", "00200011", "IS"),
        Element("FrameofReferenceUID", "00200052", "UI"),
        Element("PositionReferenceIndicator", "00201040", "LO"),
        Element("Manufacturer", "00080070", "LO"),
        Element("ManufacturerModelName", "00081090", "LO"),
        Element("DeviceSerialNumber", "00181000", "LO"),
        Element("SoftwareVersions", "00181020", "LO"),
        Element("InstanceNumber", "00200013", "IS"),
        Element("PatientOrientation", "00200020", "CS"),
        Element("BurnedInAnnotation", "00280301", "CS"),
        Element("ImageComments", "00204000", "LT"),
        Element("SamplesPerPixel", "00280002", "US"),
        Element("Rows", "00280010", "US"),
        Element("PhotometricInterpretation", "00280004", "CS"),
        Element("Columns", "00280011", "US"),
        Element("BitsAllocated", "00280100", "US"),
        Element("BitsStored", "00280101", "US"),
        Element("HighBit", "00280102", "US"),
        Element("PixelRepresentation", "00280103", "US"),
        Element("ImageType", "00080008", "CS"),
        Element("InstanceNumber", "00200013", "IS"),
        Element("PixelSpacing", "00280030", "DS"),
        Element("ImageOrientationPatient", "00200037", "DS"),
        Element("ContentTime", "00080033", "TM"),
        Element("ContentDate", "00080023", "DA"),
        Element("OphthalmicImageTypeDescription", "00221616", "LO"),
        Element("WindowCenter", "00281050", "DS"),
        Element("WindowWidth", "00281051", "DS"),
        Element("LossyImageCompression", "00282110", "CS"),
        Element("LossyImageCompressionRatio", "00282112", "DS"),
        Element("LossyImageCompressionMethod", "00282114", "CS"),
        Element("PresentationLUTShape", "20500020", "CS"),
        Element("RecognizableVisualFeatures", "00280302", "CS"),
        Element("ImageLaterality", "00200062", "CS"),
        Element("OphthalmicAnatomicReferencePointXCoordinate", "00221624", "FL"),
        Element("OphthalmicAnatomicReferencePointYCoordinate", "00221626", "FL"),
        Element("SOPClassUID", "00080016", "UI"),
        Element("SOPInstanceUID", "00080018", "UI"),
        Element("SpecificCharacter Set", "00080005", "CS"),
        Element("ProtocolName", "00181030", "LO"),
    ],
    [
        Sequence(
            "AnatomicRegionSequence",
            "00082218",
            "SQ",
            [
                Element("CodeValue", "00080100", "SH", HARMONIZE, "5665001"),
                Element("CodingSchemeDesignator", "00080102", "SH", HARMONIZE, "SCT"),
                Element("CodeMeaning", "00080104", "LO", HARMONIZE, "Retina"),
            ],
        ),
        Sequence(
            "PrimaryAnatomicStructureSequence",
            "00082228",
            "SQ",
            [
                Element("CodeValue", "00080100", "SH"),
                Element("CodingSchemeDesignator", "00080102", "SH"),
                Element("CodeMeaning", "00080104", "LO"),
            ],
        ),
        Sequence(
            "RelativeImagePositionCodeSequence",
            "0022001D",
            "SQ",
            [
                Element("CodeValue", "00080100", "SH"),
                Element("CodingSchemeDesignator", "00080102", "SH"),
                Element("CodeMeaning", "00080104", "LO"),
            ],
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
    Represents a DICOM entry in a dataset.

    This class defines a DICOM entry with attributes such as its tag, name, value representation (vr),
    and value. It also includes a method to check if the value is empty.

    Attributes:
        tag (str): The tag associated with the DICOM entry.
        name (str): The name of the DICOM entry.
        vr (str): The value representation of the DICOM entry.
        value (str): The value of the DICOM entry.
    """

    def __init__(self, tag, name, vr, value):
        """
        Initializes a DicomEntry instance.

        Args:
            tag (str): The tag associated with the DICOM entry.
            name (str): The name of the DICOM entry.
            vr (str): The value representation of the DICOM entry.
            value (str): The value of the DICOM entry.
        """
        self.tag = tag
        self.name = name
        self.vr = vr
        self.value = value

    def is_empty(self):

        return len(self.value) == 0


def extract_dicom_dict(file, tags):
    """
    Extracts DICOM metadata and specified tags from a DICOM file.

    Args:
        file (str): The path to the DICOM file.
        tags (list): A list of tags to extract from the DICOM file.

    Returns:
        tuple: A tuple containing the extracted metadata dictionary, transfer syntax information,
               and pixel data (if present).

    Raises:
        FileNotFoundError: If the specified file does not exist.
    """
    if not os.path.exists(file):
        raise FileNotFoundError(f"File {file} not found.")

    output = dict()
    output["filepath"] = file

    dataset = pydicom.dcmread(file)
    dataset.PatientOrientation = ["L", "F"]
    dataset.ImageOrientationPatient = [-1.0, 0.0, 0.0, 0.0, 0.0, 1.0]

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
    if "PixelData" in dataset:
        pixel_data = dataset.PixelData
    elif "FloatPixelData" in dataset:
        pixel_data = dataset.FloatPixelData
    else:
        pixel_data = None

    return output, transfersyntax, pixel_data


def write_dicom(
    protocol, dicom_dict_list, seg, vol, opt, op, opt_file, op_file, file_path
):
    """
    Writes a DICOM file based on a specified protocol and input data.

    This function constructs a new DICOM file using metadata and pixel data from the input dictionary
    and according to the rules defined in the provided protocol.

    Args:
        protocol (object): An object that defines the rules for DICOM file creation, including
                           header tags, tags, sequence tags, and elements.
        dicom_dict_list (list): A list containing metadata dictionaries, transfer syntax information,
                                and pixel data.

        vol (dict): Dictionary containing volume data.
        opt (dict): Dictionary containing optical data.
        op (dict): Dictionary containing operational data.
        file_path (str): The path where the output DICOM file will be saved.
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

        source_image_sequence(dataset, dicom_dict_list)
        ophthalmic_image_type_code_sequence(dataset, dicom_dict_list)
        referenced_series_sequence(dataset, dicom_dict_list, seg, vol, opt, op)
        derivation_algorithm_sequence(dataset, dicom_dict_list)
        enface_volume_descriptor_sequence(dataset, dicom_dict_list)
        ophthalmic_frame_location_sequence(dataset, dicom_dict_list, opt_file, op_file)

    pydicom.filewriter.write_file(file_path, dataset, write_like_original=False)


def convert_dicom(
    inputenface, inputseg, inputvol, inputopt, inputop, output
):  # inputseg, inputoct, inputop,
    """
    Convert DICOM data using a specific conversion rule.

    This function takes multiple input DICOM files, applies the conversion rule specified in the variable
    'conversion_rule', and writes the converted DICOM data to the output file.

    Args:
        inputenface (str): Path to the input enface DICOM file.
        inputseg (str): Path to the input segmentation DICOM file.
        inputvol (str): Path to the input volume DICOM file.
        inputopt (str): Path to the input optical DICOM file.
        inputop (str): Path to the input operational DICOM file.
        output (str): Path to the output DICOM file directory.
    """
    conversion_rule = enface
    tags = (
        conversion_rule.header_tags()
        + conversion_rule.tags()
        + list(conversion_rule.sequence_tags().keys())
        + [
            "00082112",
            "0066002F",
            "00221612",
            "00221620",
            "00660036",
            "00660031",
            "00081115",
            "00221615",
            "00082218",
        ]
    )
    enf = extract_dicom_dict(inputenface, tags)
    seg = extract_dicom_dict(inputseg, ["0020000D", "0020000E", "00080016", "00080018"])
    vol = extract_dicom_dict(inputvol, ["0020000D", "0020000E", "00080016", "00080018"])
    opt = extract_dicom_dict(inputopt, ["0020000D", "0020000E", "00080016", "00080018"])
    op = extract_dicom_dict(inputop, ["0020000D", "0020000E", "00080016", "00080018"])

    filename = inputenface.split("/")[-1]

    write_dicom(
        conversion_rule,
        enf,
        seg,
        vol,
        opt,
        op,
        inputopt,
        inputop,
        f"{output}/converted_{filename}",
    )
