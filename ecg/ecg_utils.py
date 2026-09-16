import logging
from typing import Any, Optional, Union, cast
from xml.dom.minidom import Attr, Document

from defusedxml import minidom
import xmltodict
import hashlib

utils_logger = logging.getLogger("ecg.utils")

ECG_LEADS_EXPECTED = 12
class UnsupportedXmlFileError(RuntimeError):
    """Raised when the ECG XML file format is unsupported."""


class MissingXmlElementError(RuntimeError):
    """Raised when a required XML element is missing."""


class MissingXmlAttributeError(RuntimeError):
    """Raised when a required XML attribute is missing."""


def get_node(xdoc: Document, tag_name: str) -> Document:
    """Return the first matching XML node or raise if none is found."""
    xelt = get_optional_node(xdoc, tag_name)
    if xelt is None:
        raise MissingXmlElementError(tag_name)
    return xelt


def get_optional_node(xdoc: Document, tag_name: str) -> Optional[Document]:
    """Return the first matching XML node, or None."""
    for xelt in xdoc.getElementsByTagName(tag_name):
        return cast(Document, xelt)
    return None


def get_attr_text(xdoc: Document, attr_name: str, default: Optional[str] = None) -> str:
    """Return text for a required attribute, or default if provided."""
    if attr_name in xdoc.attributes:
        return get_text(xdoc.attributes[attr_name])
    if default is None:
        raise MissingXmlAttributeError(attr_name)
    return default


def get_text(xdoc: Union[Document, Attr]) -> str:
    """Return concatenated text-node values from a DOM node."""
    text_chunks = []
    for node in xdoc.childNodes:
        if node.nodeType == node.TEXT_NODE:
            text_chunks.append(node.data)
    return "".join(text_chunks)


def assert_version(elt: Document):
    """Validate ECG document type/version against supported variants."""
    doc_info = get_node(elt, "documentinfo")
    doc_type = get_text(get_node(doc_info, "documenttype"))
    doc_ver = get_text(get_node(doc_info, "documentversion"))
    supported_doc_types = ["SierraECG", "PhilipsECG"]
    supported_doc_versions = ["1.03", "1.04", "1.04.01", "1.04.02"]
    if (doc_type not in supported_doc_types) or (doc_ver not in supported_doc_versions):
        raise UnsupportedXmlFileError(
            f"Files of type {doc_type} {doc_ver} are unsupported"
        )

    return doc_type, doc_ver


def make_dtstamp(meta_dict):
    """Converts date and time into an md5 hash.
        This may be useful as part of a file name to differentiate multiple ECGs from a single participant.
    Args:
        meta_dict (dict): dictionary containing date and time stamps
            Example: 'data_acq_date': '2023-11-28', 'data_acq_time': '08:44:48'
    Returns:
        dtstamp (string): combination of the time and date stamps
        dtstamp_md5 (string): md5 of the dtstamp
    """
    # Example: 'data_acq_date': '2023-11-28', 'data_acq_time': '08:44:48'
    dtstamp = (
        meta_dict["data_acq_date"] + "-" + meta_dict["data_acq_time"].replace(":", "")
    )
    dtstamp_md5 = hashlib.md5(dtstamp.encode("utf-8")).hexdigest()

    return dtstamp, dtstamp_md5


def parse_statements(statement_struct):
    """Converts the statement_struct to a list of dictionaries.

    ECGs can have one or more statements printed on the PDF output. The process steps of
    minidom.parse() and xmltodict.parse() are rendering these differently; in the first case,
    it's a simple dict, but in the 2nd case, it's a list of dicts. This function converts all
    cases to a list of dicts.

    Args:
        statement_struct: may be a dict or a list of dicts
    Returns:
        list of dicts

    Example of 1 statement only:
        Sinus rhythm...normal P axis, V-rate 50- 99
            <mdsignatureline>Unconfirmed Diagnosis</mdsignatureline>
            <severity code="NO" id="1">- NORMAL ECG -</severity>
            <statement editedflag="False">
                <statementcode>SR    </statementcode>
                <leftstatement>Sinus rhythm</leftstatement>
                <rightstatement>normal P axis, V-rate  50- 99</rightstatement>
            </statement>

    Example with 2 statements:
        Sinus rhythm...normal P axis, V-rate 50- 99
        Borderline right axis deviation...QRS axis ( 81, 90)
            <mdsignatureline>Unconfirmed Diagnosis</mdsignatureline>
            <severity code="ON" id="2">- OTHERWISE NORMAL ECG -</severity>
            <statement editedflag="False">
                <statementcode>SR    </statementcode>
                <leftstatement>Sinus rhythm</leftstatement>
                <rightstatement>normal P axis, V-rate  50- 99</rightstatement>
            </statement>
            <statement editedflag="False">
                <statementcode>AXR   </statementcode>
                <leftstatement>Borderline right axis deviation</leftstatement>
                <rightstatement>QRS axis ( 81, 90)</rightstatement>
            </statement>
    """

    statement_list = list()
    if isinstance(statement_struct, dict):
        # there is only one statement; keys leftstatement and rightstatement are all that's needed
        this_dict = dict()
        c_left = statement_struct["leftstatement"]
        c_right = statement_struct["rightstatement"]
        this_dict["left"] = c_left
        this_dict["right"] = c_right
        statement_list.append(this_dict)
    elif isinstance(statement_struct, list):
        # there are multiple statements, this is a list of dicts
        # 5 statements were seen in the pilot data; TBD on how many for the main study
        for d in statement_struct:
            this_dict = dict()
            c_left = d["leftstatement"]
            c_right = d["rightstatement"]
            this_dict["left"] = c_left
            this_dict["right"] = c_right
            statement_list.append(this_dict)
    else:
        utils_logger.error(
            "ERROR: unexpected structure type in the xml interpretation statements"
        )

    utils_logger.info(
        f"parse_statements input: {type(statement_struct)} converted to statement_list_of_dicts"
    )

    return statement_list


def get_text_if_exists(element):
    """Returns the value of #text if it exists in the xml, solving the
    problem where some files have it filled in and others don't.
    Args:
        element (dict): best described by 2 examples:
            example 1: {'@editedflag': 'False', '#text': '81'}
            example 2: {'@editedflag': 'False'}
    Returns:
        string
    """
    return_val = ""
    try:
        return_val = element["#text"]
    except KeyError as ke:
        utils_logger.info(f"KeyError: {ke} for element {element}")

    return return_val


def extract_participant_position(restingecg: dict) -> str:
    """Extract participant position from userdefine fields with robust key handling."""
    try:
        userdefine = restingecg["userdefines"]["userdefine"]
    except KeyError as ke:
        raise KeyError("Missing userdefines.userdefine in ECG XML") from ke

    userdefine_items = userdefine if isinstance(userdefine, list) else [userdefine]

    # Prefer explicit Position keys, then fall back to first available value.
    for item in userdefine_items:
        if isinstance(item, dict):
            name_val = str(item.get("name", "")).strip().lower()
            if name_val == "position" and "value" in item:
                return item["value"]

    for item in userdefine_items:
        if isinstance(item, dict) and "value" in item:
            return item["value"]

    raise KeyError("No participant position value found in userdefine fields")


def remove_ecg_waveform_text(my_dict: dict, verbosity: int = 0):
    """Remove parsed waveform text from ECG dictionaries for lightweight metadata handling."""

    if "restingecgdata" in my_dict.keys():
        if verbosity:
            utils_logger.info(
                "Main waveform length before: %s",
                len(my_dict["restingecgdata"]["waveforms"]["parsedwaveforms"].get("#text", "")),
            )
        my_dict["restingecgdata"]["waveforms"]["parsedwaveforms"]["#text"] = "waveform removed"
        
        repbeats = my_dict["restingecgdata"]["waveforms"]["repbeats"]["repbeat"]
        repbeat_list = repbeats if isinstance(repbeats, list) else [repbeats]
        
        if len(repbeat_list) != ECG_LEADS_EXPECTED:
            utils_logger.warning(f"Expected {ECG_LEADS_EXPECTED} leads, but found {len(repbeat_list)} in restingecgdata.")
            
        for beat in repbeat_list:
            if "waveform" in beat and "#text" in beat["waveform"]:
                beat["waveform"]["#text"] = "repbeat waveform removed"

    elif "waveforms" in my_dict.keys():
        if verbosity:
            utils_logger.info(
                "Main waveform length before: %s",
                len(my_dict["waveforms"]["parsedwaveforms"].get("#text", "")),
            )
        my_dict["waveforms"]["parsedwaveforms"]["#text"] = "waveform removed"
        
        repbeats = my_dict["waveforms"]["repbeats"]["repbeat"]
        repbeat_list = repbeats if isinstance(repbeats, list) else [repbeats]
        
        if len(repbeat_list) != ECG_LEADS_EXPECTED:
            utils_logger.warning(f"Expected {ECG_LEADS_EXPECTED} leads, but found {len(repbeat_list)} in waveforms.")
            
        for beat in repbeat_list:
            if "waveform" in beat and "#text" in beat["waveform"]:
                beat["waveform"]["#text"] = "repbeat waveform removed"

    elif "waveforms_parsedwaveforms_#text" in my_dict.keys():
        if verbosity:
            utils_logger.info(
                "Main waveform length before: %s",
                len(my_dict.get("waveforms_parsedwaveforms_#text", "")),
            )
        my_dict["waveforms_parsedwaveforms_#text"] = "waveform removed"
        
        repbeat_keys = [k for k in my_dict.keys() if "waveform" in k and k.endswith("#text") and "repbeat" in k]
        
        if len(repbeat_keys) != ECG_LEADS_EXPECTED:
            utils_logger.warning(f"Expected {ECG_LEADS_EXPECTED} leads, but found {len(repbeat_keys)} flattened repbeat waveform keys.")
            
        for k in repbeat_keys:
            my_dict[k] = "repbeat waveform removed"

    return my_dict


def flatten_nested_dict(
    input_dict: dict, separator: str = "_", prefix: str = ""
) -> dict[str, Any]:
    """Flatten nested dict/list structures into a single-level dictionary."""
    output_dict = {}
    for key, value in input_dict.items():
        if isinstance(value, dict) and value:
            deeper = flatten_nested_dict(value, separator, prefix + key + separator)
            output_dict.update({key2: val2 for key2, val2 in deeper.items()})
        elif isinstance(value, list) and value:
            for index, sublist in enumerate(value, start=1):
                if isinstance(sublist, dict) and sublist:
                    deeper = flatten_nested_dict(
                        sublist, separator, prefix + key + separator + str(index) + separator
                    )
                    output_dict.update({key2: val2 for key2, val2 in deeper.items()})
                else:
                    output_dict[prefix + key + separator + str(index)] = value
        else:
            output_dict[prefix + key] = value
    return output_dict


def extract_flattened_xml_metadata(
    xml_file_name: str, remove_waveforms: bool = True, verbosity: int = 0
) -> dict[str, Any]:
    """Extract flattened metadata directly from ECG XML (no waveform conversion)."""
    xdom = minidom.parse(xml_file_name)
    content = xdom.documentElement.toxml()
    ecg_full_dict = xmltodict.parse(content)

    if remove_waveforms:
        ecg_full_dict = remove_ecg_waveform_text(ecg_full_dict, verbosity=verbosity)

    ecg_dict = ecg_full_dict.get("restingecgdata", {})
    return flatten_nested_dict(ecg_dict)


def fetch_key_metadata(ecg_file, extended_meta=False):
    """Reads an ecg .xml file and returns selected text data. No waveforms are returned or processed.
    Args:
        ecg_file (string): complete path to the xml file, e.g. /path/to/ecg.xml
        extended_meta (boolean):
            True: return more data for the hea files
            False: return limited data for the manifest
    Returns:
        dict: structured output of text information from the *.xml; no waveforms are included
    """
    xdom = minidom.parse(ecg_file)
    root = get_node(xdom, "restingecgdata")
    assert_version(root)

    content = xdom.documentElement.toxml()  # a very long string!
    xml_dict = xmltodict.parse(content)

    restingecg = xml_dict["restingecgdata"]

    key_items = dict()

    # static items for manifest
    # key_items['domain'] = 'xml'
    # key_items['laterality'] = 'NA'  # could include for completeness, but not informative
    key_items["modality"] = "ECG"

    # device and software
    # machine_text e.g. PageWriter TC
    key_items["machine_text"] = get_text_if_exists(
        restingecg["dataacquisition"]["machine"]
    )

    # dataacquisition_machine_@detaildescription,Philips Medical Products:860306:A.07.07.07
    key_items["detailed_desc"] = restingecg["dataacquisition"]["machine"][
        "@detaildescription"
    ].replace(":", " ")

    # internalmeas_@measurementversion,10
    # internalmeas_@measurementversion,11
    # <internalmeasurements date="2023-09-29" time="07:50:23" measurementversion="10">
    # key_items['inter_measversion'] = get_text_if_exists(restingecg['internalmeasurements'])
    # print(key_items['inter_measversion'])
    key_items["inter_measversion"] = restingecg["internalmeasurements"][
        "@measurementversion"
    ]

    # interprts_interprt_@criteriaversion,0B
    # interprts_interprt_@criteriaversion,0C
    key_items["inter_criteraversion"] = restingecg["interpretations"]["interpretation"][
        "@criteriaversion"
    ]

    # patient_@criteriaversionforpatientdata,0B
    # patient_@criteriaversionforpatientdata,0C
    key_items["pt_criteraversion"] = restingecg["patient"][
        "@criteriaversionforpatientdata"
    ]

    # interprts_interprt_@criteriaversiondate,2008-04-24
    # interprts_interprt_@criteriaversiondate,2016-10-07
    # not currently fetching this one

    # for logging only
    key_items["internal_documentname"] = restingecg["documentinfo"]["documentname"]

    # variable items for file naming & manifest
    key_items["data_acq_date"] = restingecg["dataacquisition"]["@date"]
    key_items["data_acq_time"] = restingecg["dataacquisition"]["@time"]

    # variable items for manifest - this is 4 digit ID
    key_items["participant_id"] = restingecg["patient"]["generalpatientdata"]["name"]["firstname"]

    # QC item
    key_items["long_id"] = restingecg["patient"]["generalpatientdata"]["patientid"]

    # year of birth is omitted to avoid conflicts with OMOP data
    # dob = restingecg['patient']['generalpatientdata']['age']['dateofbirth']
    # yob = int(dob.split('-')[0])
    # if yob < 1934:
    #     yob = 1934  # do not report age for participants over 90
    # key_items['participant_yob'] = yob

    key_items["position"] = extract_participant_position(restingecg)

    # KeyErrors with #text in some files; split into 2 parts to enable exception handling of missing #text
    # key_items['value_HR'] = restingecg['interpretations']['interpretation']['globalmeasurements']['heartrate']['#text']
    globmeas = restingecg["interpretations"]["interpretation"]["globalmeasurements"]
    key_items["value_HR"] = get_text_if_exists(globmeas["heartrate"])
    key_items["value_PR"] = get_text_if_exists(globmeas["print"])
    key_items["value_QRSD"] = get_text_if_exists(globmeas["qrsdur"])
    key_items["value_QT"] = get_text_if_exists(globmeas["qtint"])
    key_items["value_QTc"] = get_text_if_exists(globmeas["qtcb"])
    key_items["value_P"] = get_text_if_exists(globmeas["pfrontaxis"])
    key_items["value_QRS"] = get_text_if_exists(globmeas["qrsfrontaxis"])
    key_items["value_T"] = get_text_if_exists(globmeas["tfrontaxis"])

    # Report printed items from lower right box of PDF - filter settings
    reportBW = restingecg["reportinfo"]["reportbandwidth"]
    key_items["value_highpass_filter"] = reportBW["highpassfiltersetting"]
    key_items["value_lowpass_filter"] = reportBW["lowpassfiltersetting"]
    key_items["value_notch_filter"] = reportBW["notchfiltersetting"]
    key_items["value_notch_harmonic"] = reportBW["notchharmonicssetting"]
    key_items["value_artifact_filter_flag"] = reportBW["artifactfilterflag"]
    key_items["value_hysteresis_filter_flag"] = reportBW["hysteresisfilterflag"]
    signalchars = restingecg["dataacquisition"]["signalcharacteristics"]
    key_items["value_notchfiltered"] = signalchars["notchfiltered"]
    key_items["value_acsetting"] = signalchars["acsetting"]  # e.g. 60

    # TBD whether internal date match is critical
    # rep_date = restingecg['reportinfo']['@date']
    # rep_time = restingecg['reportinfo']['@time']
    # rep = f'{rep_date} {rep_time}'
    # key_items['report_timestamp'] = rep

    # imeas_date = restingecg['internalmeasurements']['@date']
    # imeas_time = restingecg['internalmeasurements']['@time']
    # imeas_timestamp = f'{imeas_date} {imeas_time}'
    # key_items['meas_timestamp'] = imeas_timestamp

    # interp_date = restingecg['interpretations']['interpretation']['@date']
    # interp_time = restingecg['interpretations']['interpretation']['@time']
    # interp_timestamp = f'{interp_date} {interp_time}'
    # key_items['interp_timestamp'] = interp_timestamp

    if extended_meta:  # additional items for hea comments

        # units appear to be fixed as mm/mv and mm/s
        key_items["overallgain"] = restingecg["reportinfo"]["reportgain"][
            "amplitudegain"
        ]["overallgain"]
        key_items["amplitude_units"] = restingecg["reportinfo"]["reportgain"][
            "amplitudegain"
        ]["@unit"]
        key_items["timegain"] = restingecg["reportinfo"]["reportgain"]["timegain"][
            "#text"
        ]
        key_items["time_units"] = restingecg["reportinfo"]["reportgain"]["timegain"][
            "@unit"
        ]

        # convert to mm/mV and mm/sec for clarity and to match PDF
        if key_items["amplitude_units"] == "mm/mv":
            key_items["amplitude_units"] = "mm/mV"
        if key_items["time_units"] == "mm/s":
            key_items["time_units"] = "mm/sec"

        statement_list = parse_statements(
            restingecg["interpretations"]["interpretation"]["statement"]
        )
        key_items["statement_list"] = statement_list

        # diagnostic comments; unconfirmed by MD review
        key_items["report_desc"] = restingecg["reportinfo"][
            "reportdescription"
        ]  # e.g. Standard 12 Lead Report
        key_items["interp_c1"] = restingecg["interpretations"]["interpretation"][
            "mdsignatureline"
        ]  # e.g. Unconfirmed Diagnosis
        key_items["interp_c2"] = restingecg["interpretations"]["interpretation"][
            "severity"
        ][
            "#text"
        ]  # e.g. - OTHERWISE NORMAL ECG -

    return key_items
