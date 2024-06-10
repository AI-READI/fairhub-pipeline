import logging

from defusedxml import minidom
import xmltodict
import hashlib

utils_logger = logging.getLogger("ecg.utils")


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

    # variable items for manifest
    key_items["participant_id"] = restingecg["patient"]["generalpatientdata"]["name"][
        "firstname"
    ]

    # year of birth is omitted to avoid conflicts with OMOP data
    # dob = restingecg['patient']['generalpatientdata']['age']['dateofbirth']
    # yob = int(dob.split('-')[0])
    # if yob < 1934:
    #     yob = 1934  # do not report age for participants over 90
    # key_items['participant_yob'] = yob

    key_items["position"] = restingecg["userdefines"]["userdefine"][0]["value"]

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
