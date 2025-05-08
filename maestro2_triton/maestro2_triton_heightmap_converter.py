import os
import pydicom
import numpy as np
from PIL import Image
from maestro2_triton.maestro2_triton_heightmap_converter_functional_groups import (
    shared_functional_group_sequence,
    per_frame_functional_groups_sequence,
    dimension_index_sequence,
    dimension_organization_sequence,
    segment_sequence,
    referenced_series_sequence,
)


class OCTASeg:
    """
    A class to handle OCTA segmentation data from DICOM files.

    Attributes:
    structdcmfn : str
        Filepath for the structure DICOM file.
    octadcmfn : str
        Filepath for the OCTA DICOM file.
    structdcm : pydicom.dataset.FileDataset
        Loaded structure DICOM file.
    octadcm : pydicom.dataset.FileDataset
        Loaded OCTA DICOM file.
    missing : float
        Missing value indicator.
    allz : np.ndarray
        Layer heightmaps.
    namez : list
        Layer names.
    zscale : float
        Scaling factor for z-dimension.
    structvol : np.ndarray
        Volume data from the structure DICOM file.
    """

    def __init__(self, structdcm, octadcm):
        """
        Initialize the OCTASeg class with structure and OCTA DICOM file paths.

        Parameters:
        structdcm : str
            Filepath for the structure DICOM file.
        octadcm : str
            Filepath for the OCTA DICOM file.
        """
        self.structdcmfn = structdcm  # DCM Filepath for Structure
        self.octadcmfn = octadcm  # DCM Filepath for OCTA
        self.structdcm = None  # DCM Filepath for Structure
        self.octadcm = None  # DCM Filepath for OCTA
        self.missing = None  # float missing value
        self.allz = None  # layer heightmaps
        self.namez = None  # layer names
        self.loadStruct()
        self.loadOCTA()

    def loadStruct(self):
        """
        Load and process the structure DICOM file.
        """
        dcm = pydicom.dcmread(self.structdcmfn)
        zscale = dcm[0x52009229][0][0x00289110][0][0x00280030].value[0]
        self.structdcm = dcm
        self.zscale = zscale
        self.structvol = dcm.pixel_array

    def loadOCTA(self):
        """
        Load and process the OCTA DICOM file.
        """
        dcm = pydicom.dcmread(self.octadcmfn)
        self.namez = []
        self.allz = []
        for layer in dcm[0x00660002]:
            layern = layer[0x00660004].value.split(": ")[-1].replace("/", "_")
            a = np.frombuffer(layer[0x00660011][0][0x00660016].value, dtype=np.float32)
            a = a.reshape(layer[0x00660011][0][0x00660015].value, 3)
            xi = np.sort(np.unique(a[:, 0]))
            yi = np.sort(np.unique(a[:, 2]))
            z = np.zeros((xi.shape[0], yi.shape[0]), dtype=np.float32)
            for i in range(a.shape[0]):
                xii = np.where(xi == a[i, 0])[0]
                yii = np.where(yi == a[i, 2])[0]
                z[xii, yii] = a[i, 1]
            self.allz.append(z)
            self.namez.append(layern)
        self.allz = np.array(self.allz)
        self.allz = np.rot90(self.allz, k=1, axes=(1, 2))
        self.allz /= self.zscale
        self.octadcm = dcm

    def plotVol(self, outdir):
        """
        Generate and save volume plot images.

        Parameters:
        outdir : str
            Output directory to save the generated images.
        """
        if self.missing is not None:
            self.allz[self.allz == self.missing] = 0
        for i in range(self.structvol.shape[0]):
            outfn = "%s/oct-%03d.png" % (outdir, i)
            bscan = np.stack(
                (self.structvol[i], self.structvol[i], self.structvol[i]), axis=2
            )
            for j in range(self.allz.shape[0]):
                for zi, z in enumerate(self.allz[j, i, :]):
                    if z < 0:
                        continue
                    z = round(z)
                    bscan[z, zi, 0] = (j + 1) * 10
                    bscan[z, zi, 1] = 200
                    bscan[z, zi, 2] = 200
            Image.fromarray(bscan).save(outfn)


def get_heightmap_float_pixel_array(octfile, segfile):
    """
    Get heightmap float pixel data from OCT and segmentation files.

    Parameters:
    octfile : str
        Filepath for the OCT DICOM file.
    segfile : str
        Filepath for the segmentation DICOM file.

    Returns:
    bytes
        Float pixel data as bytes.
    """
    topcon_octa_instance = OCTASeg(structdcm=octfile, octadcm=segfile)
    pixel_array = topcon_octa_instance.allz
    return pixel_array


def get_heightmap_float_pixel_data(octfile, segfile):
    """
    Get heightmap float pixel data from OCT and segmentation files.

    Parameters:
    octfile : str
        Filepath for the OCT DICOM file.
    segfile : str
        Filepath for the segmentation DICOM file.

    Returns:
    bytes
        Float pixel data as bytes.
    """
    topcon_octa_instance = OCTASeg(structdcm=octfile, octadcm=segfile)
    pixel_array = topcon_octa_instance.allz
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
        Map(
            "NumerOfFrames", "00280008", "SEG", "NumberOfFrames", ["00620002", "#item"]
        ),
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
        Element(
            "NumberOfFrames", "00280008", "IS", DESIGNATE, 0, ["00280008", maestro_octa]
        ),
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
        Element("ContentLabel", "00700080", "CS"),
        Element("ContentDescription", "00700081", "LO"),
        Element("SegmentationType", "00620001", "CS", HARMONIZE, "HEIGHTMAP"),
        Element(
            "SOPClassUID", "00080016", "UI", HARMONIZE, "1.2.840.10008.5.1.4.xxxxx.1"
        ),
        Element("SOPInstanceUID", "00080018", "UI"),
        Element("SpecificCharacterSet", "00080005", "CS"),
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
    dataset.FloatPixelData = get_heightmap_float_pixel_data(oct_file, seg_file)

    shared_functional_group_sequence(dataset, seg_dic, oct_dic, op_dic)
    per_frame_functional_groups_sequence(dataset, seg_dic, oct_dic)
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
