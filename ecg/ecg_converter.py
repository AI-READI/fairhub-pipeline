import logging

import pandas as pd
import shutil

from sierraecg import read_file as read_ecg_file
from wfdb.io.convert.csv import csv_to_wfdb

from ecg import ecg_utils as ecg_utils

conv_logger = logging.getLogger("ecg.converter")


def audit_ecg_metadata(meta_dict):
    """Check gain and units; these are expected to be the same for
    all sites and the code has not been designed to handle changes.
    Args:
        meta_dict (dict): only 4 of the keys are checked
    Returns:
        integer; should be 0
    """
    error_count = 0

    expected_value_dict = {
        "amplitude_units": "mm/mV",
        "time_units": "mm/sec",
        "overallgain": "10.00",
        "timegain": "25.00",
    }

    for k, v in expected_value_dict.items():
        if meta_dict[k] != expected_value_dict[k]:
            error_count += 1
            conv_logger.error(
                f"Meta data {k} : {meta_dict[k]} not equal to expected value {expected_value_dict[k]}"
            )

    # ensure participantID is in the range 1001 to 9999
    pID = meta_dict["participant_id"]
    try:
        int_pID = int(pID)
        if (int_pID < 1001) or (int_pID > 9999):
            error_count += 1
            conv_logger.error(
                f"Meta data participant_id : {int_pID} is not in the range 1001 through 9999"
            )
    except ValueError:
        error_count += 1
        conv_logger.error(
            f"Meta data participant_id : {pID} could not be converted to int."
        )

    return error_count


def rescale_signals(df, divide_by=200):
    """Accepts a pandas DataFrame of 12 lead values and applies a conversion.

    The 2-step process of *.xml --> *.csv --> *.wfdb is handling the default ADC gain
    factor in an unexpected way; reversing this effect is accomplished by dividing out the
    factor of 200. Rescaling is evaluated by examining the trailing square pulse which should
    be 1mV tall and 0.2 seconds wide in the final signal traces.

    Args:
        df (pandas DataFrame): dataframe must contain only the columns for the lead values,
           e.g. ['I', 'II', 'III', 'aVR', 'aVL', 'aVF', 'V1', 'V2', 'V3', 'V4', 'V5', 'V6']
        divide_by (number): factor needed to convert the test pulse correctly
    Returns:
        pandas DataFrame
    """
    # The reference pulse is 1mV (2 grid) when scaling=True and factor=200

    for c in df.columns:
        df[c] = df[c] / divide_by
    return df


def assemble_hea_comments(f, key_meta, verbose=False):
    """Formats key meta data for the *.hea sidecar file
    Args:
        key_meta (dict): key meta data used for the *.hea annotation file
    Returns:
        list of comments to be appended to the comment section at the end of the *.hea file
    """
    comment_list = [
        # Static Fields
        "manufacturer: Philips",
        "device_model: PageWriter TC30",
        # f'domain: {key_meta["domain"]}',
        f'modality: {key_meta["modality"]}',
        "header_version: 1.0",
        "dataset_information: see docs.aireadi.org and fairhub.io",
        "dataset_usage_and_license: see docs.aireadi.org",
        # Fields from xml
        f'machine_text: {key_meta["machine_text"]}',
        f'machine_detail_description: {key_meta["detailed_desc"]}',  # Philips Medical Products:860306:A.07.07.07
        f'interpretation_criteriaversion: {key_meta["inter_criteraversion"]}',  # 0B or 0C
        f'patient_criteriaversion: {key_meta["pt_criteraversion"]}',  # 0B or 0C
        f'internalmeasurements_version: {key_meta["inter_measversion"]}',  # 10 or 11
        f'Time_axis: {key_meta["timegain"]} {key_meta["time_units"]}',  # 10.00 mm/mV
        f'Amplitude: {key_meta["overallgain"]} {key_meta["amplitude_units"]}',  # 25.00 mm/sec
        f"device_documentation_type_and_version: {f.doc_type} {f.doc_ver}",
        f'participant_id: {key_meta["participant_id"]}',
        # removing yob to avoid conflict w/ OMOP
        # f'participant_year_of_birth: {key_meta["participant_yob"]}',
        f'participant_position: {key_meta["position"]}',
        # Field names based on PDF export
        f'Rate: {key_meta["value_HR"]}',
        f'PR: {key_meta["value_PR"]}',
        f'QRSD: {key_meta["value_QRSD"]}',
        f'QT: {key_meta["value_QT"]}',
        f'QTc: {key_meta["value_QTc"]}',
        f'P: {key_meta["value_P"]}',
        f'QRS: {key_meta["value_QRS"]}',
        f'T: {key_meta["value_T"]}',
        # Filter information printed on PDF export
        f'high_pass_filter_setting: {key_meta["value_highpass_filter"]}',
        f'low_pass_filter_setting: {key_meta["value_lowpass_filter"]}',
        f'notch_filter_setting: {key_meta["value_notch_filter"]}',
        f'notch_harmonic_setting: {key_meta["value_notch_harmonic"]}',
        f'artifact_filter_flag: {key_meta["value_artifact_filter_flag"]}',
        f'hysteresis_filter_flag: {key_meta["value_hysteresis_filter_flag"]}',
        f'notch_filtered: {key_meta["value_notchfiltered"]}',
        f'ac_setting: {key_meta["value_acsetting"]}',
        # diagnostic comments (unconfirmed by MD review)
        f'report_description: {key_meta["report_desc"]}',  # e.g. Standard 12 Lead Report
        f'interpretation_comment_1: {key_meta["interp_c1"]}',  # e.g. Unconfirmed Diagnosis
        f'interpretation_comment_2: {key_meta["interp_c2"]}',  # e.g. - OTHERWISE NORMAL ECG -
    ]

    conv_logger.info(
        f'ECG contains {len(key_meta["statement_list"])} statements that will be added to the *.hea file.'
    )

    # statements such as 'Borderline right axis deviation'
    # {left}....{right} resembles the PDF but is hard to understand
    for idx, d in enumerate(key_meta["statement_list"]):
        # c_str = f'comment_{idx+1}: {d["left"]}.....{d["right"]}'
        # comment_list.append(c_str)
        c_key = f'comment_{idx+1}_key: {d["left"]}'
        c_val = f'comment_{idx+1}_val: {d["right"]}'
        comment_list.append(c_key)
        comment_list.append(c_val)

    return comment_list


def convert_ecg(ecg_xml_path, temp_csv_folder, output_wfdb_folder):
    """Reads the xml file and converts to wfdb format, consisting of
        *.dat - waveform data only in binary format
        *.hea - header data to assist in reading the binary and to provide other annotations
    Args:
        ecg_xml_path (string): full path the xml file to convert
        temp_csv_folder (string): full path to a folder that can hold the intermediate csv file
        output_wfdb_folder (string): full path to the folder where the final *.dat and *.hea files should be moved
    Returns:
        participant ID (string)
        full path to the output *.hea file (string)
    """
    conv_dict = dict()

    # read meta data
    key_meta = ecg_utils.fetch_key_metadata(ecg_xml_path, extended_meta=True)
    pID = key_meta["participant_id"]
    conv_dict["participantID"] = pID

    # audit meta data
    error_count = audit_ecg_metadata(key_meta)
    if error_count > 0:
        msg = f"Conversion audit FAILED with error_count {error_count}"
        conv_logger.error(f"Conversion audit FAILED with error_count {error_count}")
        conv_dict["conversion_success"] = False
        conv_dict["conversion_issues"] = [msg]
        # return pID, "ERROR: no conversion"
        return conv_dict

    # determine output file names
    # uniq = key_meta["internal_documentname"].split('.')[0]  # save this for the logfile
    dtstamp, dtstamp_md5 = ecg_utils.make_dtstamp(key_meta)
    conv_logger.debug(f"dtstamp for file disambiguation is {dtstamp}")

    ecg_fname_base = f"{pID}_ecg_{dtstamp_md5[-8:]}"
    temp_csv_file = temp_csv_folder + f"/{ecg_fname_base}.csv"

    # read the xml file and separate out the values for the leads
    f = read_ecg_file(ecg_xml_path)
    lead_dict = dict()
    for lead in f.leads:
        lead_dict[lead.label] = lead
        conv_logger.debug(
            f"{lead.label}:\tduration={lead.duration} sampling_freq={lead.sampling_freq} {lead.samples[0:6]}..."
        )
    lead_simple_dict = {k: lead_dict[k].samples for k in lead_dict.keys()}
    # read_ecg_file lead values are all integers from about -250 to + 250, although in the df they
    #  get saved as 200.000000 with 6 zeros

    # load signals into a df, rescale, convert to csv in preparation for csv_to_wfdb
    df = pd.DataFrame(lead_simple_dict)
    df = rescale_signals(
        df, divide_by=200
    )  # rescale xml lead signals to get correct test pulse
    df.to_csv(temp_csv_file, index=False)
    conv_logger.debug(f"ECG intermediate csv file has been written to {temp_csv_file}")

    # outut wfdb file
    # wfdb.io.convert.csv.csv_to_wfdb -- not our own routine
    # creates both *.dat and *.hea
    # ~/opt/anaconda3/envs/ai_readi_311/lib/python3.11/site-packages/wfdb/io/convert/csv.py # if we want to modify it
    comments_to_insert = assemble_hea_comments(f, key_meta)

    print("temp_csv_file", temp_csv_file)

    # reminder that csv_to_wfdb() writes to the current dir and is not configurable
    # note that the adc_gain and the required rescaling above are likely linked; future work will
    # try to disentangle this; for now, just check the gain settings so that this approach provides
    # consistent output
    csv_to_wfdb(
        temp_csv_file,
        fs=lead.sampling_freq,  # sampling freq is 500 for ECG data from the Philips device
        fmt="16",  # fmt must be a list or else a single string that will be applied to all channels
        # Accepted formats are: '80','212','16','24', and '32'
        adc_gain=200,  # Default is 200
        units="mV",
        comments=comments_to_insert,  # comments will be added to the *.hea file only
        verbose=False,
    )  # has no return value

    # move the wfdb output files from the current directory to the intended directory

    ecg_as_dat = ecg_fname_base + ".dat"
    ecg_as_hea = ecg_fname_base + ".hea"

    src_dat = f"./{ecg_as_dat}"
    src_hea = f"./{ecg_as_hea}"

    dest_dat = f"{output_wfdb_folder}/{ecg_as_dat}"
    dest_hea = f"{output_wfdb_folder}/{ecg_as_hea}"

    conv_logger.info(f"ECG data file moved from {src_dat} to {dest_dat}")

    ret_dat = shutil.move(src_dat, dest_dat)
    ret_hea = shutil.move(src_hea, dest_hea)

    conv_logger.debug(f"Return values from shutil.move are {ret_dat} and {ret_hea}")

    conv_dict["output_hea_file"] = dest_hea
    conv_dict["output_dat_file"] = dest_dat
    conv_dict["output_files"] = [dest_hea, dest_dat]
    conv_dict["conversion_success"] = True
    conv_dict["conversion_issues"] = []

    return conv_dict  # pID, dest_hea
