import os
import pydicom


# Class representing a rule for classifying DICOM entries
class ClassifyingRule:
    """
    Represents a classification rule for processing DICOM entries.

    This class defines a rule used for classifying DICOM entries based on specific conditions.
    It contains attributes such as the rule's name and a list of conditions that must be met
    for the rule to apply.

    Attributes:
        name (str): The name of the classification rule.
        conditions (list): List of lambda functions representing the conditions.

    Methods:
        apply(dicom_entry): Checks if the DICOM entry meets all conditions.
    """

    def __init__(self, name, conditions):
        self.name = name
        self.conditions = conditions

    def apply(self, dicom_entry):
        # Apply all conditions to the DICOM entry
        for condition in self.conditions:
            if not condition(dicom_entry):
                return False
        return True


# List of classification rules
rules = [
    # Define various classification rules with specific conditions
    ClassifyingRule(
        "maestro2_octa_segmentation",
        conditions=[
            lambda entry: entry.device == "3DOCT-1Maestro2"
            and str(entry.filename).endswith("4.1.dcm")
            and str(entry.sopclassuid) == "1.2.840.10008.5.1.4.1.1.66.5"
        ],
    ),
    ClassifyingRule(
        "triton_octa_segmentation",
        conditions=[
            lambda entry: entry.device == "Triton plus"
            and str(entry.filename).endswith("4.1.dcm")
            and str(entry.sopclassuid) == "1.2.840.10008.5.1.4.1.1.66.5"
        ],
    ),
    ClassifyingRule(
        "raw_data_storage",
        conditions=[
            lambda entry: entry.sopclassuid.startswith("1.2.840.10008.5.1.4.1.1.66")
        ],
    ),
    ClassifyingRule(
        "missing_critical_information",
        conditions=[lambda entry: entry.error != "no"],
    ),
    ClassifyingRule(
        "optomed_mac_or_disk_centered_cfp",
        conditions=[
            lambda entry: "Aurora" == entry.device
            and entry.sopclassuid.startswith("1.2.840.10008.5.1.4.1.1.77.1.5.1")
        ],
    ),
    # eidon
    ClassifyingRule(
        "eidon_uwf_central_ir",
        conditions=[
            lambda entry: "0-infrared" in entry.filename.lower()
            and "Eidon" in entry.device
            and entry.sopclassuid.startswith("1.2.840.10008.5.1.4.1.1.77.1.5.1")
        ],
    ),
    ClassifyingRule(
        "eidon_uwf_central_faf",
        conditions=[
            lambda entry: "0-af-" in entry.filename.lower()
            and "Eidon" in entry.device
            and entry.sopclassuid.startswith("1.2.840.10008.5.1.4.1.1.77.1.5.1")
        ],
    ),
    ClassifyingRule(
        "eidon_uwf_central_cfp",
        conditions=[
            lambda entry: "0-visible" in entry.filename.lower()
            and "Eidon" in entry.device
            and entry.sopclassuid.startswith("1.2.840.10008.5.1.4.1.1.77.1.5.1")
        ],
    ),
    ClassifyingRule(
        "eidon_uwf_nasal_cfp",
        conditions=[
            lambda entry: "3-visible" in entry.filename.lower()
            and "Eidon" in entry.device
            and entry.sopclassuid.startswith("1.2.840.10008.5.1.4.1.1.77.1.5.1")
        ],
    ),
    ClassifyingRule(
        "eidon_uwf_temporal_cfp",
        conditions=[
            lambda entry: "4-visible" in entry.filename.lower()
            and "Eidon" in entry.device
            and entry.sopclassuid.startswith("1.2.840.10008.5.1.4.1.1.77.1.5.1")
        ],
    ),
    ClassifyingRule(
        "eidon_mosaic_cfp",
        conditions=[
            lambda entry: "11-visible" in entry.filename.lower()
            and "Eidon" in entry.device
            and entry.sopclassuid.startswith("1.2.840.10008.5.1.4.1.1.77.1.5.1")
        ],
    ),
    # maestro
    ClassifyingRule(
        "maestro2_retinal_photography",
        conditions=[
            lambda entry: entry.device == "3DOCT-1Maestro2"
            and entry.sopclassuid.startswith("1.2.840.10008.5.1.4.1.1.77.1.5.1")
            and entry.filename.lower().endswith("2.1.dcm")
        ],
    ),
    ClassifyingRule(
        "triton_retinal_photography",
        conditions=[
            lambda entry: entry.device == "Triton plus"
            and entry.sopclassuid.startswith("1.2.840.10008.5.1.4.1.1.77.1.5.1")
            and entry.filename.lower().endswith("2.1.dcm")
        ],
    ),
    ClassifyingRule(
        "maestro2_3d_macula_oct_oct",
        conditions=[
            lambda entry: entry.device == "3DOCT-1Maestro2"
            and 0.03 < entry.slicethickness < 0.05
            and entry.filename.lower().endswith(".dcm")
        ],
    ),
    ClassifyingRule(
        "maestro2_3d_wide_oct_oct",
        conditions=[
            lambda entry: entry.device == "3DOCT-1Maestro2"
            and 0.06 < entry.slicethickness < 0.08
            and entry.filename.lower().endswith(".dcm")
        ],
    ),
    ClassifyingRule(
        "maestro2_mac_6x6_octa_oct",
        conditions=[
            lambda entry: entry.device == "3DOCT-1Maestro2"
            and 0.0 < entry.slicethickness < 0.02
            and entry.filename.lower().endswith(".dcm")
        ],
    ),
    ClassifyingRule(
        "triton_3d_radial_oct_oct",
        conditions=[
            lambda entry: entry.device == "Triton plus"
            and str(entry.slicethickness).startswith("0.03")
        ],
    ),
    ClassifyingRule(
        "triton_macula_6x6_octa_oct",
        conditions=[
            lambda entry: entry.device == "Triton plus"
            and str(entry.slicethickness).startswith("0.01")
        ],
    ),
    ClassifyingRule(
        "triton_macula_12x12_octa_oct",
        conditions=[
            lambda entry: entry.device == "Triton plus"
            and str(entry.slicethickness).startswith("0.02")
        ],
    ),
    # #spectralis
    # 496, 768, 27
    ClassifyingRule(
        "spectralis_onh_rc_hr_oct",
        conditions=[
            lambda entry: entry.device == "Spectralis"
            and entry.sopclassuid == "1.2.840.10008.5.1.4.1.1.77.1.5.4"
            and entry.seriesdescription == "IR"
            and str(entry.rows) == "496"
            and str(entry.columns) == "768"
            and 25 < entry.framenumber < 29
        ],
    ),
    # 496, 768, 61
    ClassifyingRule(
        "spectralis_ppol_mac_hr_oct_small",
        conditions=[
            lambda entry: entry.device == "Spectralis"
            and entry.sopclassuid == "1.2.840.10008.5.1.4.1.1.77.1.5.4"
            and entry.seriesdescription == "Volume IR"
            and str(entry.rows) == "496"
            and str(entry.columns) == "768"
            and 59 < entry.framenumber < 63
        ],
    ),
    # 496, 1536, 61
    ClassifyingRule(
        "spectralis_ppol_mac_hr_oct",
        conditions=[
            lambda entry: entry.device == "Spectralis"
            and entry.sopclassuid == "1.2.840.10008.5.1.4.1.1.77.1.5.4"
            and str(entry.rows) == "496"
            and str(entry.columns) == "1536"
            and 59 < entry.framenumber < 63
        ],
    ),  # 496, 512, 512
    ClassifyingRule(
        "spectralis_mac_20x20_hs_octa_oct",
        conditions=[
            lambda entry: entry.device == "Spectralis"
            and entry.seriesdescription == "Volume IR"
            and str(entry.rows) == "496"
            and str(entry.columns) == "512"
            and 510 < entry.framenumber < 514
        ],
    ),
    # 496, 384, 284
    ClassifyingRule(
        "spectralis_retired_octa_oct",
        conditions=[
            lambda entry: entry.device == "Spectralis"
            and entry.sopclassuid == "1.2.840.10008.5.1.4.1.1.77.1.5.4"
            and str(entry.rows) == "496"
            and str(entry.columns) == "384"
            and 382 < entry.framenumber < 386
        ],
    ),
    # 768, 768
    ClassifyingRule(
        "spectralis_ppol_mac_hr_retinal_photography_small",
        conditions=[
            lambda entry: entry.device == "Spectralis"
            and entry.sopclassuid.startswith("1.2.840.10008.5.1.4.1.1.77.1.5.1")
            and entry.seriesdescription == "Volume IR"
            and str(entry.rows) == "768"
            and str(entry.columns) == "768"
            and str(entry.gaze) == "Primary gaze"
            and str(entry.privatetag) == "N/A"
        ],
    ),
    # 1536, 1536
    ClassifyingRule(
        "spectralis_ppol_mac_hr_retinal_photography",
        conditions=[
            lambda entry: entry.device == "Spectralis"
            and entry.sopclassuid.startswith("1.2.840.10008.5.1.4.1.1.77.1.5.1")
            and entry.seriesdescription == "Volume IR"
            and str(entry.rows) == "1536"
            and str(entry.columns) == "1536"
            and str(entry.gaze) == "Primary gaze"
            and str(entry.privatetag) == "N/A"
        ],
    ),
    # 1536 1536
    ClassifyingRule(
        "spectralis_onh_rc_hr_retinal_photography",
        conditions=[
            lambda entry: entry.device == "Spectralis"
            and entry.sopclassuid.startswith("1.2.840.10008.5.1.4.1.1.77.1.5.1")
            and entry.seriesdescription == "IR"
            and str(entry.rows) == "1536"
            and str(entry.columns) == "1536"
        ],
    ),
    # 768, 768
    ClassifyingRule(
        "spectralis_mac_20x20_hs_octa_retinal_photography",
        conditions=[
            lambda entry: entry.device == "Spectralis"
            and entry.sopclassuid.startswith("1.2.840.10008.5.1.4.1.1.77.1.5.1")
            and entry.seriesdescription == "Volume IR"
            and str(entry.rows) == "768"
            and str(entry.columns) == "768"
            and str(entry.privatetag) == "Super Slim"
        ],
    ),
    # 1536 1536
    ClassifyingRule(
        "spectralis_retired_octa_retinal_photography",
        conditions=[
            lambda entry: entry.device == "Spectralis"
            and entry.sopclassuid.startswith("1.2.840.10008.5.1.4.1.1.77.1.5.1")
            and str(entry.rows) == "512"
            and str(entry.columns) == "512"
        ],
    ),
    ClassifyingRule(
        "secondary_capture",
        conditions=[lambda entry: entry.sopclassuid == "1.2.840.10008.5.1.4.1.1.7"],
    ),
    ClassifyingRule(
        "pdf",
        conditions=[
            lambda entry: entry.sopclassuid.startswith("1.2.840.10008.5.1.4.1.1.104.1")
        ],
    ),
    ClassifyingRule(
        "maestro_octa_enface",
        conditions=[
            lambda entry: entry.device == "3DOCT-1Maestro2"
            and str(entry.sopclassuid) == "1.2.840.10008.5.1.4.1.1.77.1.5.7"
        ],
    ),
    ClassifyingRule(
        "triton_octa_enface",
        conditions=[
            lambda entry: entry.device == "Triton plus"
            and str(entry.sopclassuid) == "1.2.840.10008.5.1.4.1.1.77.1.5.7"
        ],
    ),
    ClassifyingRule(
        "maestro_octa_volume",
        conditions=[
            lambda entry: entry.device == "3DOCT-1Maestro2"
            and str(entry.sopclassuid) == "1.2.840.10008.5.1.4.1.1.77.1.5.8"
        ],
    ),
    ClassifyingRule(
        "triton_octa_volume",
        conditions=[
            lambda entry: entry.device == "Triton plus"
            and str(entry.sopclassuid) == "1.2.840.10008.5.1.4.1.1.77.1.5.8"
        ],
    ),
]


# Class representing a DICOM entry
class DicomEntry:
    """
    Represents a DICOM entry with various attributes.

    This class holds the detailed information of a DICOM file including filename, patient ID,
    SOP class UID, slice thickness, etc.

    Attributes:
        filename (str): The name of the DICOM file.
        filesize (float): The size of the DICOM file in MB.
        patientid (str): The patient ID.
        sopclassuid (str): The SOP class UID.
        sopinstanceuid (str): The SOP instance UID.
        laterality (str): The laterality information.
        rows (int): The number of rows in the image.
        columns (int): The number of columns in the image.
        device (str): The device used for capturing the image.
        framenumber (int): The frame number.
        referencedsopinstance (str): The referenced SOP instance.
        slicethickness (float): The slice thickness.
        privatetag (str): Private tag information.
        acquisitiondatetime (str): The acquisition date and time.
        performedprotocol (str): The performed protocol.
        seriesdescription (str): The series description.
        studyid (str): The study ID.
        gaze (str): The gaze information.
        seriesuid (str): The series UID.
        error (str): Error information.
        name (str): The name of the patient.
    """

    def __init__(
        self,
        filename,
        filesize,
        patientid,
        sopclassuid,
        sopinstanceuid,
        laterality,
        rows,
        columns,
        device,
        framenumber,
        referencedsopinstance,
        slicethickness,
        privatetag,
        acquisitiondatetime,
        performedprotocol,
        seriesdescription,
        studyid,
        gaze,
        seriesuid,
        error,
        name,
    ):
        self.filename = filename
        self.filesize = filesize
        self.patientid = patientid
        self.sopclassuid = sopclassuid
        self.sopinstanceuid = sopinstanceuid
        self.laterality = laterality
        self.rows = rows
        self.columns = columns
        self.device = device
        self.framenumber = framenumber
        self.referencedsopinstance = referencedsopinstance
        self.slicethickness = slicethickness
        self.privatetag = privatetag
        self.acquisitiondatetime = acquisitiondatetime
        self.performedprotocol = performedprotocol
        self.seriesdescription = seriesdescription
        self.studyid = studyid
        self.gaze = gaze
        self.seriesuid = seriesuid
        self.error = error
        self.name = name


# Class representing a summary of a DICOM entry
class DicomSummary:
    """
    Represents a summary of a DICOM entry.

    This class holds a summarized version of a DICOM file including filename, patient ID,
    laterality, description, acquisition date and time, and SOP instance UID.

    Attributes:
        filename (str): The name of the DICOM file.
        patientid (str): The patient ID.
        laterality (str): The laterality information.
        description (str): Description of the DICOM entry based on classification rules.
        acquisitiondatetime (str): The acquisition date and time.
        sopinstanceuid (str): The SOP instance UID.
    """

    def __init__(
        self,
        filename,
        patientid,
        laterality,
        description,
        acquisitiondatetime,
        sopinstanceuid,
    ):
        self.filename = filename
        self.patientid = patientid  # patient id
        self.laterality = laterality  # laterality
        self.description = description  # belongs to which one in AIREADI checklist
        self.acquitisiondatetime = acquisitiondatetime
        self.sopinstanceuid = sopinstanceuid


# Function to extract information from a DICOM file and create a DicomEntry object
def extract_dicom_entry(file):
    """
    Extract detailed information from a DICOM file and create a DicomEntry object.

    Args:
        file (str): The path to the DICOM file.

    Returns:
        DicomEntry: An object containing detailed information about the DICOM file.
    """
    if not os.path.exists(file):
        raise FileNotFoundError(f"File {file} not found.")

    dicom = pydicom.dcmread(file).to_json_dict()
    ds = pydicom.dcmread(file)
    filename = os.path.basename(file)
    bottom_file_name = os.path.basename(file)
    directory_one_level_up = os.path.dirname(file)
    second_to_bottom_file_name = os.path.basename(directory_one_level_up)

    filesize = os.path.getsize(file) / (1024 * 1024)
    folder_path = os.path.dirname(file)
    folder_files = os.listdir(folder_path)
    error = "no"

    if "0020000E" in dicom:
        seriesuid = dicom["0020000E"]["Value"][0]
    else:
        seriesuid = "noseriesuid"
        error = f"no seriesuid: {file}"

    if "00100020" in dicom and "Value" in dicom["00100020"]:
        patientid = dicom["00100020"]["Value"][0]
    else:
        patientid = "noid"
        error = f"no patientid: {file}"

    if "00100010" in dicom and "Value" in dicom["00100010"]:
        name = dicom["00100010"]["Value"][0]
    else:
        name = "no name"

    if "00080016" in dicom:
        sopclassuid = dicom["00080016"]["Value"][0]
    else:
        sopclassuid = "nosopclassuid"
        error = f"no sopclassuid: {file}"

    if "00080018" in dicom:
        sopinstanceuid = dicom["00080018"]["Value"][0]
    else:
        sopinstanceuid = "nosopinstanceuid"
        error = f"no sopinstanceuid: {file}"

    performedprotocol = "N/A"
    studyid = "N/A"
    slicethickness = 0
    rows = device = columns = framenumber = acquisitiondatetime = seriesdescription = (
        referencedsopinstance
    ) = gaze = "N/A"

    # Extract specific attributes based on SOP Class UID
    if "00200062" in dicom and "Value" in dicom["00200062"]:
        laterality = dicom["00200062"]["Value"][0]
    elif "00200060" in dicom and "Value" in dicom["00200060"]:
        laterality = dicom["00200060"]["Value"][0]
    else:
        laterality = "NA"
        error = f"no laterality: {file}"

    # Extract attributes for retinal photography
    if sopclassuid == "1.2.840.10008.5.1.4.1.1.77.1.5.1":
        rows = dicom["00280010"]["Value"][0]
        columns = dicom["00280011"]["Value"][0]
        acquisitiondatetime = dicom["0008002A"]["Value"][0]
        slicethickness = 0
        device = dicom["00081090"]["Value"][0]

        if "0008103E" in dicom:
            seriesdescription = dicom["0008103E"]["Value"][0]
        else:
            seriesdescription = "N/A"

        if "00511017" in dicom:
            privatetag = dicom["00511017"]["Value"][0]
        else:
            privatetag = "N/A"

        if "00220006" in dicom:
            gaze = dicom["00220006"]["Value"][0]["00080104"]["Value"][0]
        else:
            gaze = "N/A"

    # Extract attributes for OCT
    elif sopclassuid == "1.2.840.10008.5.1.4.1.1.77.1.5.4":
        rows = dicom["00280010"]["Value"][0]
        columns = dicom["00280011"]["Value"][0]
        framenumber = dicom["00280008"]["Value"][0]
        seriesdescription = dicom["0008103E"]["Value"][0]
        acquisitiondatetime = dicom["0008002A"]["Value"][0]
        device = dicom["00081090"]["Value"][0]
        privatetag = "N/A"

        if "52009229" in dicom:
            referencedsopinstance = dicom["52009229"]["Value"][0]["00081140"]["Value"][
                0
            ]["00081155"]["Value"][0]
        else:
            referencedsopinstance = "NA"
            error = f"no slices in oct: {file}"

        if (
            "52009229" in dicom
            and "00289110" in dicom["52009229"]["Value"][0]
            and "00180050" in dicom["52009229"]["Value"][0]["00289110"]["Value"][0]
        ):
            slicethickness = dicom["52009229"]["Value"][0]["00289110"]["Value"][0][
                "00180050"
            ]["Value"][0]
        else:
            slicethickness = ""

    # Extract attributes for Segmentation
    elif sopclassuid == "1.2.840.10008.5.1.4.1.1.66.5":
        seriesdescription = dicom["0008103E"]["Value"][0]
        device = dicom["00081090"]["Value"][0]
        privatetag = "N/A"
        slicethickness = 0

    # Extract attributes for Volume
    elif sopclassuid == "1.2.840.10008.5.1.4.1.1.77.1.5.8":
        seriesdescription = dicom["0008103E"]["Value"][0]
        device = dicom["00081090"]["Value"][0]
        privatetag = "N/A"
        slicethickness = 0

    # Extract attributes for En Face
    elif sopclassuid == "1.2.840.10008.5.1.4.1.1.77.1.5.7":
        device = dicom["00081090"]["Value"][0]
        privatetag = "N/A"
        slicethickness = 0

    elif sopclassuid == "1.2.840.10008.5.1.4.1.1.7":
        device = "Spectralis"
        privatetag = "N/A"
        slicethickness = 0

    else:  # unknown
        privatetag = "N/A"

    output = DicomEntry(
        filename,
        filesize,
        patientid,
        sopclassuid,
        sopinstanceuid,
        laterality,
        rows,
        columns,
        device,
        framenumber,
        referencedsopinstance,
        slicethickness,
        privatetag,
        acquisitiondatetime,
        performedprotocol,
        seriesdescription,
        studyid,
        gaze,
        seriesuid,
        error,
        name,
    )
    return output


## Domain, Modality, Protocol, Patient ID, Laterlity, sopinstanceuid, referencedsopinstance
def extract_dicom_summary(file):
    """
    Extract a summary from a DICOM file.

    This function extracts detailed information from a DICOM file using the extract_dicom_entry function,
    then summarizes this information into a DicomSummary object.

    Args:
        file (str): The path to the DICOM file.

    Returns:
        DicomSummary: An object containing summarized information about the DICOM file.
    """

    dicomentry = extract_dicom_entry(file)
    sopclassuid = dicomentry.sopclassuid

    filename = dicomentry.filename

    filesize = dicomentry.filesize

    acquisitiondatetime = dicomentry.acquisitiondatetime

    sopinstanceuid = dicomentry.sopinstanceuid
    device = dicomentry.device
    patientid = dicomentry.patientid
    laterality = dicomentry.laterality
    referencedsopinstance = dicomentry.referencedsopinstance
    performedprotocol = dicomentry.performedprotocol
    description = find_rule(file)

    output = DicomSummary(
        filename,
        patientid,
        laterality,
        description,
        acquisitiondatetime,
        sopinstanceuid,
    )
    # output = DicomSummaryDetail(domain, modality, patientid, laterality, description,sopinstanceuid,referencedsopinstance)
    return output


def get_summary(file):
    """
    Get a summary of a DICOM file.

    This function checks if the file is a valid DICOM file and then extracts its summary.

    Args:
        file (str): The path to the file.

    Returns:
        dict: A dictionary representation of the DICOM summary, or an error message if the file is not valid.
    """
    if file.endswith(".dcm") or file[-8:].isdigit():
        dicomsummary = extract_dicom_summary(file)
        obj_dict = vars(dicomsummary)
    else:
        obj_dict = "error"
    return obj_dict


def find_rule(file):
    """
    Find and apply classification rules to a DICOM file.

    This function checks if the file is a valid DICOM file and then applies classification rules
    to determine the appropriate classification.

    Args:
        file (str): The path to the file.

    Returns:
        str: The name of the classification rule that applies, or "no_rules_apply" if none apply.
    """
    if file.endswith(".dcm") or file[-8:].isdigit():
        dicomentry = extract_dicom_entry(file)
        matching_rules = [rule for rule in rules if rule.apply(dicomentry)]
        if matching_rules:
            for rule in matching_rules:
                return str(rule.name)
        else:
            return "no_rules_apply"


def is_dicom_file(file_path):
    """
    Find and apply classification rules to a DICOM file.

    This function checks if the file is a valid DICOM file and then applies classification rules
    to determine the appropriate classification.

    Args:
        file (str): The path to the file.

    Returns:
        str: The name of the classification rule that applies, or "no_rules_apply" if none apply.
    """
    try:
        pydicom.dcmread(file_path)
        return True
    except pydicom.errors.InvalidDicomError:
        return False
