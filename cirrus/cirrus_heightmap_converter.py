import os
import pydicom
import numpy as np
from PIL import Image
from cirrus.cirrus_heightmap_converter_functional_groups import (
    shared_functional_group_sequence,
    per_frame_functional_groups_sequence,
    dimension_index_sequence,
    dimension_organization_sequence,
    segment_sequence,
    referenced_series_sequence,
)


class ZeissSegmentationConverter:
    """
    A class to convert Zeiss OCT segmentation DICOM files into a heightmap format.

    The class reads segmentation data from a DICOM file, finds transitions in pixel values
    (from 0 to 255 and from 255 to 0), and builds a heightmap representation based on these transitions.
    The result is a 3D array that captures the boundaries of segmentation layers in the original OCT data.

    Attributes:
        segmentation_file (str): The path to the DICOM segmentation file.
        pixel_array (numpy.ndarray): The pixel data array read from the DICOM file.
        change_indices_dict (dict): A dictionary that stores the indices where transitions
                                    from 0 to 255 and 255 to 0 occur for each pixel line.
        final_array (numpy.ndarray): A 3D array storing the heightmap representation of segmentation data.

    Methods:
        read_segmentation_file(): Reads the pixel data from the DICOM file.
        find_change_indices(): Identifies the indices where pixel values transition
                               from 0 to 255 and from 255 to 0 in each B-scan.
        build_final_array(): Builds the final heightmap array based on the identified transition indices.
        zeiss_segmentation_to_heightmap(): Executes the full process to convert the segmentation
                                           file into a heightmap and returns the result.
    """

    def __init__(self, segmentation_file):
        """
        Initializes the ZeissSegmentationConverter with the provided segmentation DICOM file.

        Args:
            segmentation_file (str): Path to the Zeiss OCT segmentation DICOM file.
        """
        self.segmentation_file = segmentation_file
        self.pixel_array = None
        self.change_indices_dict = {}
        self.final_array = None

    def read_segmentation_file(self):
        """
        Reads the pixel data from the segmentation DICOM file and stores it in the pixel_array attribute.

        Returns:
            None
        """
        a = pydicom.dcmread(self.segmentation_file)
        self.pixel_array = a.pixel_array

    def find_change_indices(self):
        """
        Identifies the indices where pixel values change from 0 to 255 and from 255 to 0 in each B-scan.

        The method scans through each pixel row in the B-scans, detects transitions in pixel values
        from 0 to 255 and from 255 to 0, and stores these indices in the change_indices_dict.

        Returns:
            None
        """
        self.change_indices_dict = {
            a: {"0": {}, "1": {}} for a in range(self.pixel_array.shape[0])
        }

        for a in range(self.pixel_array.shape[0]):
            for i in range(self.pixel_array.shape[2]):
                pixel_values = self.pixel_array[a, :, i]

                # Get indices where transitions occur
                change_indices_0_to_255 = (
                    np.where((pixel_values[:-1] <= 1) & (pixel_values[1:] >= 254))[0]
                    + 1
                )
                change_indices_255_to_0 = (
                    np.where((pixel_values[:-1] >= 254) & (pixel_values[1:] <= 1))[0]
                    + 1
                )

                if change_indices_0_to_255.size > 0:
                    self.change_indices_dict[a]["0"][i] = change_indices_0_to_255
                if change_indices_255_to_0.size > 0:
                    self.change_indices_dict[a]["1"][i] = change_indices_255_to_0

    def build_final_array(self):
        """
        Constructs the final heightmap array based on the detected pixel value transitions.

        The heightmap stores the first detected transition from 0 to 255 in one layer and
        the first transition from 255 to 0 in another layer for each pixel line in each B-scan.

        Returns:
            None
        """
        # Initialize the transformed data array
        self.final_array = np.zeros(
            (2, self.pixel_array.shape[0], self.pixel_array.shape[2]), dtype=float
        )

        # Populate the final array using the change_indices_dict
        for a in range(self.pixel_array.shape[0]):
            for i in range(self.pixel_array.shape[2]):
                if i in self.change_indices_dict[a]["0"]:
                    self.final_array[0, a, i] = float(
                        self.change_indices_dict[a]["0"][i][0]
                    )
                if i in self.change_indices_dict[a]["1"]:
                    self.final_array[1, a, i] = float(
                        self.change_indices_dict[a]["1"][i][0]
                    )

    def zeiss_segmentation_to_heightmap(self):
        """
        Executes the full pipeline to read the segmentation file, detect changes in pixel values,
        and build the heightmap representation.

        Returns:
            numpy.ndarray: The final heightmap array with shape (2, num_slices, num_columns),
                           where the first layer contains the 0-to-255 transitions and the second layer contains the 255-to-0 transitions.
        """
        self.read_segmentation_file()
        self.find_change_indices()
        self.build_final_array()
        return self.final_array


def get_heightmap_float_pixel_data(seg_file):
    """
    Convert the segmentation data from a Zeiss OCT DICOM file to a heightmap and return the pixel data in byte format.

    This function uses the ZeissSegmentationConverter class to process the segmentation file
    and extract heightmap data, then converts this data to byte format for further use.

    Args:
        seg_file (str): Path to the Zeiss OCT segmentation DICOM file.

    Returns:
        bytes: The heightmap pixel data in byte format.
    """
    converter = ZeissSegmentationConverter(segmentation_file=seg_file)
    pixel_array = converter.zeiss_segmentation_to_heightmap()
    floatpixeldata = pixel_array.tobytes()
    return floatpixeldata


KEEP = 0
BLANK = 1
HARMONIZE = 2
DESIGNATE = 3


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
        """
        Initialize the ConversionRule class with its name, header elements, elements, and sequences.

        Parameters:
            name (str): The name of the conversion rule.
            header_elements (list): List of Element instances representing header elements.
            elements (list): List of Element instances representing individual elements.
            sequences (list): List of Sequence instances representing sequences of elements.
        """
        self.name = name
        self.header_elements = header_elements
        self.elements = elements
        self.sequences = sequences

    def header_tags(self):
        """
        Extracts unique tags from header elements.

        Returns:
            list: A list of unique tags from the header elements.
        """
        headertags = set()
        for header_element in self.header_elements:
            headertags.add(header_element.tag)
        return list(headertags)

    def tags(self):
        """
        Extracts unique tags from individual elements.

        Returns:
            list: A list of unique tags from the individual elements.
        """
        tags = set()
        for element in self.elements:
            tags.add(element.tag)

        return list(tags)

    def sequence_tags(self):
        """
        Generates a dictionary of sequence tags and associated element tags.

        Returns:
            dict: A dictionary where keys are sequence tags and values are lists of lists of element tags.
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
    Represents a header element in the system.

    Attributes:
        name (str): The name of the header element.
        tag (str): The tag associated with the header element.
        vr (str): The value representation of the header element.
    """

    def __init__(self, name, tag, vr):
        """
        Initializes a new instance of the HeaderElement class.

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
    Represents an element in the system.

    Attributes:
        name (str): The name of the element.
        tag (str): The tag associated with the element.
        vr (str): The value representation of the element.
        decision (int): The decision value of the element.
        harmonized_value (int): The harmonized value of the element.
        designated_value (int): The designated value of the element.
    """

    def __init__(
        self, name, tag, vr, decision=0, harmonized_value=0, designated_value=0
    ):
        """
        Initializes a new instance of the Element class.

        Args:
            name (str): The name of the element.
            tag (str): The tag associated with the element.
            vr (str): The value representation of the element.
            decision (int, optional): The decision value of the element. Defaults to 0.
            harmonized_value (int, optional): The harmonized value of the element. Defaults to 0.
            designated_value (int, optional): The designated value of the element. Defaults to 0.
        """
        self.name = name
        self.tag = tag
        self.vr = vr
        self.decision = decision
        self.harmonized_value = harmonized_value
        self.designated_value = designated_value


class Sequence:
    """
    Represents a sequence in the system.

    Attributes:
        name (str): The name of the sequence.
        tag (str): The tag associated with the sequence.
        vr (str): The value representation of the sequence.
        element_lists (list): A list of element lists in the sequence.
        sequences (list): A list of sequences contained within this sequence.
    """

    def __init__(self, name, tag, vr, *element_lists, sequences=None):
        """
        Initializes a new instance of the Sequence class.

        Args:
            name (str): The name of the sequence.
            tag (str): The tag associated with the sequence.
            vr (str): The value representation of the sequence.
            *element_lists: Variable length argument list of element lists.
            sequences (list, optional): A list of sequences contained within this sequence. Defaults to None.
        """
        self.name = name
        self.tag = tag
        self.vr = vr
        self.element_lists = element_lists if element_lists else []
        self.sequences = sequences if sequences is not None else []


class MappingRule:
    """
    Represents a mapping rule in the system.

    Attributes:
        name (str): The name of the mapping rule.
        mappedvalues (dict): The values mapped by this rule.
    """

    def __init__(self, name, mappedvalues):
        """
        Initializes a new instance of the MappingRule class.

        Args:
            name (str): The name of the mapping rule.
            mappedvalues (dict): The values mapped by this rule.
        """
        self.name = name
        self.mappedvalues = mappedvalues


class Map:
    """
    Represents a map in the system.

    Attributes:
        name (str): The name of the map.
        tag (str): The tag associated with the map.
        file (str): The file associated with the map.
        mappedname (str): The mapped name in the map.
        mappedtags (list): The tags mapped in the map.
    """

    def __init__(self, name, tag, file, mappedname, mappedtags):
        """
        Initializes a new instance of the Map class.

        Args:
            name (str): The name of the map.
            tag (str): The tag associated with the map.
            file (str): The file associated with the map.
            mappedname (str): The mapped name in the map.
            mappedtags (list): The tags mapped in the map.
        """
        self.name = name
        self.tag = tag
        self.file = file
        self.mappedname = mappedname
        self.mappedtags = mappedtags


maestro_octa = MappingRule(
    "maestro_octs",
    [
        Map("Rows", "00280010", "OCT", "NumberOfFrames", ["00280008"]),
        Map("Columns", "00280011", "OCT", "Columns", ["00280011"]),
        Map("SamplesPerPixel", "00280002", "OCT", "SamplesPerPixel", ["00280002"]),
        Map(
            "PhotometricInterpretation",
            "00280004",
            "OCT",
            "PhotometricInterpretation",
            ["00280004"],
        ),
    ],
)


def mapping(tag, mappingrule):
    """
    Finds a map instance within the mapping rule that matches the given tag.

    Args:
        tag (str): The tag to search for.
        mappingrule (MappingRule): The mapping rule containing mapped values.

    Returns:
        Map: The map instance with the matching tag, or None if no match is found.
    """
    for map_instance in mappingrule.mappedvalues:
        if map_instance.tag == tag:
            return map_instance
    return None


def process_map_instance(oct_dic, seg_dic, map_instance):
    """
    Processes a map instance to retrieve the associated value from the given dictionaries.

    Args:
        oct_dic (dict): The dictionary containing OCT data.
        seg_dic (dict): The dictionary containing SEG data.
        map_instance (Map): The map instance to process.

    Returns:
        The value associated with the map instance based on its file and mapped tags.
    """
    if map_instance.file == "OCT":
        value = oct_dic[0][map_instance.mappedtags[0]].value[0]

    elif map_instance.file == "SEG":
        if len(map_instance.mappedtags) == 1:
            value = seg_dic[0][map_instance.mappedtags[0]].value[0]
        elif len(map_instance.mappedtags) == 2:
            value = len(seg_dic[0]["00620002"].value)

    return value


heightmap = ConversionRule(
    "Heightmap",
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
        Element("FrameOfReferenceUID", "00200052", "UI"),
        Element("PositionReferenceIndicator", "00201040", "LO"),
        Element("Manufacturer", "00080070", "LO"),
        Element("ManufacturerModelName", "00081090", "LO"),
        Element("DeviceSerialNumber", "00181000", "LO"),
        Element("SoftwareVersions", "00181020", "LO"),
        Element("InstanceNumber", "00200013", "IS"),
        Element("InstanceNumber", "00200013", "IS"),
        Element("ContentDate", "00080023", "DA"),
        Element("ContentTime", "00080033", "TM"),
        Element("NumberOfFrames", "00280008", "IS", HARMONIZE, 2),
        Element(
            "SamplePerPixel", "00280002", "US", DESIGNATE, 0, ["00280002", maestro_octa]
        ),
        Element("Rows", "00280010", "US", DESIGNATE, 0, ["00280010", maestro_octa]),
        Element(
            "PhotometricInterpretation",
            "00280004",
            "CS",
            DESIGNATE,
            0,
            ["00280004", maestro_octa],
        ),
        Element("Columns", "00280011", "US", DESIGNATE, 0, ["00280011", maestro_octa]),
        Element("BitsAllocated", "00280100", "US", HARMONIZE, 32),
        # Element("FloatPixelPaddingValue", "00280122", "FL"),
        # Element("FloatPixelPaddingRangeLimit", "00280124", "FL",),
        Element("ImageType", "00080008", "CS", HARMONIZE, "DERIVED PRIMARY"),
        Element("ContentLabel", "00700080", "CS", HARMONIZE, "OCT_SEGMENTATION"),
        Element("ContentDescription", "00700081", "LO"),
        Element("SegmentationType", "00620001", "CS", HARMONIZE, "HEIGHTMAP"),
        Element(
            "SOPClassUID", "00080016", "UI", HARMONIZE, "1.2.840.10008.5.1.4.xxxxx.1"
        ),
        Element("SOPInstanceUID", "00080018", "UI"),
        Element("SpecificCharacterSet", "00080005", "CS"),
        Element("ProtocolName", "00181030", "LO"),
    ],
    [
        Sequence("SegmentSequence", "00620002", "SQ"),
        Sequence("SurfaceSequence", "00660002", "SQ"),
    ],
)


oct_b = ConversionRule(
    "OCT B",
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
        Element("AccessionNumber", "00080050", "SH"),
        Element("Modality", "00080060", "CS"),
        Element("SeriesInstanceUID", "0020000E", "UI"),
        Element("SeriesNumber", "00200011", "IS"),
        Element("FrameOfReferenceUID", "00200052", "UI"),
        Element("PositionReferenceIndicator", "00201040", "LO"),
        Element("SynchronizationFrameOfReferenceUID", "00200200", "UI"),
        Element("SynchronizationTrigger", "0018106A", "CS"),
        Element("AcquisitionTimeSynchronized", "00181800", "CS"),
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
        Element("InstanceNumber", "00200013", "IS"),
        Element("ContentDate", "00080023", "DA"),
        Element("ContentTime", "00080033", "TM"),
        Element("NumberOfFrames", "00280008", "IS"),
        Element("ImageType", "00080008", "CS"),
        Element("SamplesPerPixel", "00280002", "US"),
        Element("AcquisitionDateTime", "0008002A", "DT"),
        Element("AcquisitionDuration", "00189073", "US"),
        Element("AcquisitionNumber", "00200012", "US"),
        Element("PhotometricInterpretation", "00280004", "CS"),
        Element("PixelRepresentation", "00280103", "US"),
        Element("BitsAllocated", "00280100", "US"),
        Element("BitsStored", "00280101", "US"),
        Element("HighBit", "00280102", "US"),
        Element("PresentationLUTShape", "20500020", "CS"),
        Element("LossyImageCompression", "00282110", "CS"),
        Element("LossyImageCompressionRatio", "00282112", "DS"),
        Element("LossyImageCompressionMethod", "00282114", "CS"),
        Element("BurnedInAnnotation", "00280301", "CS"),
        Element("ConcatenationFrameOffsetNumber", "00209228", "UL"),
        Element("InConcatenationNumber", "00209162", "US"),
        Element("InConcatenationTotalNumber", "00209163", "US"),
        Element("AxialLengthOfTheEye", "00220030", "FL"),
        Element("HorizontalFieldofView", "0022000C", "FL"),
        Element("RefractiveStateSequence", "0022001B", "SQ"),
        Element("EmmetropicMagnification", "0022000A", "FL"),
        Element("IntraOcularPressure", "0022000B", "FL"),
        Element("PupilDilated", "0022000D", "CS", HARMONIZE, "YES"),
        Element("MadriaticAgentSequence", "00220058", "SQ"),
        Element("DegreeOfDilation", "0022000E", "FL"),
        Element("DetectorType", "00187004", "CS"),
        Element("IlluminationWaveLength", "00220055", "FL"),
        Element("IlluminationPower", "00220056", "FL"),
        Element("IlluminationBandwidth", "00220057", "FL"),
        Element("DepthSpatia Resolution", "00220035", "FL"),
        Element("MaximumDepthDistortion", "00220036", "FL"),
        Element("AlongScanSpatialResolution", "00220037", "FL"),
        Element("MaximumAlongScanDistortion", "00220038", "FL"),
        Element("AcrossScanSpatialResolution", "00220048", "FL"),
        Element("MaximumAcrossScanDistortion", "00220049", "FL"),
        Element("ImageLaterality", "00200062", "CS"),
        Element("SOPClassUID", "00080016", "UI"),
        Element("SOPInstanceUID", "00080018", "UI"),
        Element("SpecificCharacterSet", "00080005", "CS"),
        Element("ProtocolName", "00181030", "LO"),
    ],
    [
        Sequence("LightPathFilterTypeStackCodeSequence", "00220017", "SQ"),
        Sequence("MydriaticAgentSequence", "00220058", "SQ"),
        Sequence("RefractiveStateSequence", "0022001B", "SQ"),
        Sequence("AcquisitionContextSequence", "00400555", "SQ"),
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
        """
        Initializes a new instance of the DicomEntry class.

        Args:
            tag (str): The DICOM tag associated with the entry.
            name (str): The name of the DICOM tag.
            vr (str): The value representation of the DICOM tag.
            value (Any): The value associated with the DICOM tag.
        """
        self.tag = tag
        self.name = name
        self.vr = vr
        self.value = value

    def is_empty(self):
        """
        Checks if the value of the DICOM entry is empty.

        Returns:
            bool: True if the value is empty, False otherwise.
        """
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

    dataset.ImageType = ["DERIVED", "PRIMARY"]

    header_elements = {
        "00020000": {
            "vr": "UL",
            "Value": [dataset.file_meta.FileMetaInformationGroupLength],
        },
        "00020001": {
            "vr": "OB",
            "Value": [dataset.file_meta.FileMetaInformationVersion],
        },
        "00020002": {"vr": "UI", "Value": ["1.2.840.10008.5.1.4.xxxxx.1"]},
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

    pixeldata = None  # dataset.FloatPixelData

    return output, transfersyntax, pixeldata


def write_dicom(protocol, seg_dic, oct_dic, op_dic, seg_file, oct_file, file_path):
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
    headertags = protocol.header_tags()
    tags = protocol.tags()
    sequencetags = protocol.sequence_tags()

    file_meta = pydicom.Dataset()

    for headertag in headertags:
        value = seg_dic[0][headertag].value
        element_name = pydicom.datadict.keyword_for_tag(seg_dic[0][headertag].tag)
        setattr(file_meta, element_name, value)

    dataset = pydicom.Dataset()
    dataset.file_meta = file_meta
    dataset.ImageLaterality = oct_dic[0]["00200062"].value

    for tag in tags:
        for element in protocol.elements:
            if element.tag == tag:
                desired_element = element

        if desired_element.decision == BLANK:
            value = []

        elif desired_element.decision == HARMONIZE:
            value = desired_element.harmonized_value

        elif desired_element.decision == DESIGNATE:

            map_instance = mapping(
                desired_element.designated_value[0], desired_element.designated_value[1]
            )
            value = process_map_instance(oct_dic, seg_dic, map_instance)

        elif tag in seg_dic[0]:
            value = seg_dic[0][tag].value

        else:
            value = []

        element_name = (
            pydicom.datadict.keyword_for_tag(seg_dic[0][tag].tag)
            if tag in seg_dic[0]
            else pydicom.datadict.keyword_for_tag(tag)
        )
        setattr(dataset, element_name, value)

    dataset.is_little_endian = seg_dic[1][0]
    dataset.is_implicit_VR = seg_dic[1][1]
    dataset.FloatPixelData = get_heightmap_float_pixel_data(seg_file)

    shared_functional_group_sequence(dataset, seg_dic, oct_dic, op_dic)
    per_frame_functional_groups_sequence(dataset)
    dimension_index_sequence(dataset, seg_dic, oct_dic)
    dimension_organization_sequence(dataset, seg_dic, oct_dic)
    segment_sequence(dataset, seg_dic, oct_dic)
    referenced_series_sequence(dataset, seg_dic, oct_dic, op_dic)
    pydicom.filewriter.write_file(file_path, dataset, write_like_original=False)


def convert_dicom(inputseg, inputoct, inputop, output):
    """
    Convert DICOM data using a specific conversion rule.

    This function takes input DICOM files, applies the conversion rule specified in the variable
    'conversion_rule', and writes the converted DICOM data to the output file.

    Args:
        inputseg (str): Path to the input SEG DICOM file.
        inputoct (str): Path to the input OCT DICOM file.
        inputop (str): Path to the input OP DICOM file.
        output (str): Path to the output DICOM file.
    """
    conversion_rule = heightmap
    tags = (
        conversion_rule.header_tags()
        + conversion_rule.tags()
        + list(conversion_rule.sequence_tags().keys())
        + ["52009230", "52009229", "00620002", "00081115", "00209221", "00209222"]
    )

    conversion_rule1 = oct_b
    tags1 = (
        conversion_rule1.header_tags()
        + conversion_rule1.tags()
        + list(conversion_rule1.sequence_tags().keys())
        + ["52009230", "52009229", "00620002", "00081115", "00209221", "00209222"]
    )
    x = extract_dicom_dict(inputseg, tags)

    y = extract_dicom_dict(inputoct, tags1)

    z = extract_dicom_dict(inputop, ["0020000E", "00080016", "00080018"])

    filename = inputseg.split("/")[-1]

    write_dicom(
        conversion_rule, x, y, z, inputseg, inputoct, f"{output}/converted_{filename}"
    )
