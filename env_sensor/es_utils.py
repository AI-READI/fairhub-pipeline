import logging

import inspect  # may be temporary debug solution
from pathlib import Path
import glob
import os
import pkgutil  # to get access to the sensor_id asset file
import io  # for io.BytesIO to read the asset file

# plotting
# import matplotlib.pyplot as plt

import pandas as pd
import zipfile
from datetime import datetime, timedelta, date

utils_logger = logging.getLogger('es.utils')

# To provide a working start date and a sanity check date with 24 hr margin
CONST_STUDY_START = '2023-07-01'
CONST_DEFAULT_VISIT = '2023-07-02'
# Thresholds for QA, filtering
CONST_NUMCOLS_RAWCSV = 22  # 22 data fields in each row
CONST_MIN_DATA_LINES = 12 * 2  # 12 rows per min, so about 2 minutes

# myself = lambda: inspect.stack()[1][3]  # not permitted by Flake8


def all_same(items):
    """Returns true if all of the items in the list are identical

    Args: items (list), e.g. ['ver 1.2', 'ver 1.2', 'ver 2.7']
    Returns: True or False
    """
    return all(x == items[0] for x in items)


def get_csv_list(input_path):
    """Retrieves a sorted list of the *.csv files in a folder

    Args: input_path (string): Path to the folder that contains the *.csv files
    Returns: a list (could be empty) of csv files
    """
    csv_file_list = []  # default is the empty list

    if os.path.isdir(input_path):  # is a folder, so search inside for *.csv
        csv_file_list = sorted(glob.glob(input_path + "/*.csv"))
    elif os.path.isfile(input_path):  # single file that is a .csv
        if (input_path[-4:] == ".csv"):
            csv_file_list = [input_path]
        else:  # single file that is not *.csv
            utils_logger.info(f'Single file but not a csv: {input_path}')
    else:
        utils_logger.info(f'Neither a single file nor a folder: {input_path}')

    return csv_file_list


def get_elapsed_time(date_string1, date_string2):
    """Returns the elapsed time between 2 date strings

    Args:
        date_string1 (string): date, e.g. '2024-05-19 12:22:11'
        date_string2 (string): date
    Returns:
        datetime.timedelta between the two dates, e.g. datetime.timedelta(days=8, seconds=1331)
    """

    date_1 = datetime.strptime(date_string1, '%Y-%m-%d %H:%M:%S')
    date_2 = datetime.strptime(date_string2, '%Y-%m-%d %H:%M:%S')
    elapsed_time = date_2 - date_1

    return elapsed_time


def pipeline_get_pppp_nnn_from_foldername(s, sep='-'):
    """Extract elements pppp and nnn from folder name ENV-pppp-nnn

    Args:
        s (dict): provides 'input_path' folder name, e.g. 'ENV-9999-333'
        sep (string): Separator string, e.g. '-' or '_'
    Returns:
        s (dict) updated with:  # ToDo better info
            value of pppp: (participant ID, str)
            nnn:(esID, str)
            name of routine that set each of these
    """
    folder_name = s['t']['input_path']
    basefile = os.path.basename(folder_name)
    items = basefile.split(sep)

    pppp_ok = True  # True until error found
    nnn_ok = True

    if items[0] != "ENV":
        utils_logger.info(f"Folder name should start with ENV. {folder_name}")
        # not a problem but shouldn't happen; may indicate a larger issue
    if len(items) < 3:
        emsg = f"Insufficient components in folder name {folder_name}. Expected ENV-pppp-nnn."
        s = eh(s, emsg)
        s['t']['pppp_fname'] = "NPID"
        s['t']['nnn_fname'] = "999"
        pppp_ok = False
        nnn_ok = False
    else:
        s['t']['pppp_fname'] = items[1]
        s['t']['nnn_fname'] = items[2]  # [:3]  # first 3 chars only; permits other elements after nnn

    if (not isValid_pID(s['t']['pppp_fname'])):
        emsg = f"Invalid participant ID {s['t']['pppp_fname']}; using NPID placeholder."
        s = eh(s, emsg)
        s['t']['pppp_fname'] = 'NPID'
        pppp_ok = False

    if (not isValid_esID(s['t']['nnn_fname'])):
        emsg = f"Invalid sensor ID {s['t']['nnn_fname']}; using 999 placeholder."
        s = eh(s, emsg)
        s['t']['nnn_fname'] = '999'
        nnn_ok = False

    # use the function name on the stack to record which function set the value
    # 1,3 -- convert_env_sensor
    # 0,3 -- pipeline_* (correct one)
    s['qa']['pppp_fname_well_formatted']['ok'] = pppp_ok
    s['qa']['pppp_fname_well_formatted']['set_by'] = inspect.stack()[0][3]
    s['qa']['nnn_fname_well_formatted']['ok'] = nnn_ok
    s['qa']['nnn_fname_well_formatted']['set_by'] = inspect.stack()[0][3]

    return s


def pipeline_get_csv_list(s):
    """Get a sorted list of the contents of the folder
    Args:
        s (dict): structure with the input path
    Returns:
        s (dict): updated with the file list
    """
    file_list = sorted(get_csv_list(s['t']['input_path']))
    if len(file_list) < 1:
        err_msg = f'No *.csv files found for input_path {s["t"]["input_path"]}'
        s = eh(s, err_msg)

    s['t']['file_list'] = file_list
    s['t']['num_orig_files'] = len(file_list)
    return s


def split_header_list(header_list):
    """Separates semi-colons and keywords from the value and returns dict

    File parser read_single_csv identifies header lines by the semi-colon
    that starts the line; all such lines are lumped together into a list of
    header elements. This function will separate the parameters into separate
    lists and report any unexpected keywords.

    Args:
        header_list (list): list of lines that started with ; or #
        Example of expected elements shown below
            ; Version: 1.2.4
            ; SEN55 ABCDEF1234567
    Returns:
        hdict (dict): keys, values parsed from header elements
    """
    hdict = dict()
    hdict['fw_version_list'] = list()  # empty list as default
    hdict['sen55_list'] = list()

    for h in header_list:
        _, p, v = h.strip().split(' ')

        if p == 'Version:':
            hdict['fw_version_list'].append(v)
        elif p == 'SEN55':
            hdict['sen55_list'].append(v)
        else:  # unknown or unexpected element, possibly a corrupted byte
            msg = f'Header line unexpected: {h}'
            utils_logger.warning(msg)

    return hdict


def pipeline_get_merged_file_contents(s):
    """Performs the read_files() task and saves the info in s

    Args:
        s (dict): structure used throughout the pipeline
    Returns:
        s (dict) updated
    """
    filter_level = s['r']['filter_level']

    header_list, column_dict, data_list, \
        err_dict, nd, nf = read_files(s['t']['file_list'], filter_level=filter_level)
    hdict = split_header_list(header_list)
    s['t']['num_orig_data_lines'] = nd
    s['t']['num_final_files'] = nf

    msg = f'hdict for debug:\n\n**** hdict\n {hdict}'
    utils_logger.debug(msg)

    # expecting fw_version_list, sen55_list in hdict keys
    for k, v in hdict.items():
        s['t'][k] = v

    # qa_header will check that the column headers from the files all match
    s['t']['column_dict'] = column_dict
    s['t']['col_name_list'] = list(column_dict.values())

    m = f'col_name_list has {len(s["t"]["col_name_list"])} items:  {s["t"]["col_name_list"]}'
    utils_logger.debug(m)

    s['t']['data_list'] = data_list
    s['t']['err_dict_from_read_files'] = err_dict

    return s


def pipeline_get_outfile_name(s):
    """Assemble the outfile name
        CAUTION: does not currently check that output_folder exists
    """
    s['t']['outfile_posixpath'] = Path(f'{s["t"]["output_folder"]}/{s["t"]["pppp_fname"]}_ENV.csv')
    return s


def pipeline_get_p_visit(s, visit_dict):
    """Use the pppp to get the visit info, otherwise get default info and flag the issue.
    """
    if s['t']['pppp_fname'] in visit_dict.keys():
        p_visit = visit_dict[s['t']['pppp_fname']]

        # pppp is presumed correct since it exists in visit_dict
        s['r']['pppp'] = s['t']['pppp_fname']
        s['r']['location'] = p_visit['location']
        s['qa']['pppp_in_visit_dict']['ok'] = True
        s['qa']['pppp_in_visit_dict']['set_by'] = inspect.stack()[0][3]

    else:
        p_visit = visit_dict['0000']
        err_msg = f'Participant ID {s["t"]["pppp_fname"]} is not in visit table; using default to enable checks.'
        s = eh(s, err_msg)
        s['qa']['pppp_in_visit_dict']['ok'] = False
        s['qa']['pppp_in_visit_dict']['set_by'] = inspect.stack()[0][3]

    s['t']['p_visit'] = p_visit  # save all items in the visit dict

    return s


def find_short_run(data_list, min_gap_delta, max_lines=240):
    """Finds short runs separated by large time gaps that could be chopped out

    Scans data_list for a large time gap, return boolean to indicate if a chop
    could be made and the number of lines to remove. Rules for chopping:
        - remove data only if there is < 20 minutes of consistent data followed by
            a large time gap, indicating visit demo prior to placement at home
        - only return the first set of lines meeting these rules; function can
            be run again as needed

    Args:
        data_list (list of data rows): data to evaluate
        min_gap_delta (timedelta): minimum gap to consider a break in the data
        max_lines (int) : 12 lines per minute, 20 mins would be 12*20 = 240
    Returns:
        found_chop (boolean): chop point meeting the rules was found
        trim_nlines (int): number of lines to chop
    """
    found_chop = False
    if (len(data_list) < 2):
        return False, 0  # found_chop cannot be found, trim_nlines is 0
    tstr0 = data_list[0].split(',')[0]  # string
    timestamp0 = get_datetime_from_timestr(tstr0)  # datetime

    done_finding_gaps = False
    n = 0
    last_timestamp = timestamp0
    len_data_list = len(data_list)

    while (not done_finding_gaps) and (n < (len_data_list - 1)):
        n += 1
        this_tstr = data_list[n].split(',')[0]
        this_timestamp = get_datetime_from_timestr(this_tstr)

        t_delta = this_timestamp - last_timestamp  # can use time string

        if (t_delta) > min_gap_delta:
            done_finding_gaps = True
            found_chop = True
            m = f'...reached max_t_delta at line {n}'
            utils_logger.debug(m)
        elif (n > max_lines):
            done_finding_gaps = True

        last_timestamp = this_timestamp

    return found_chop, n


def pipeline_filter_visit_dates(s):
    """Filter data to contain only timestamps between visit_date and return_date

    Filter data that is inside the observation window (patient receives device to returns device)
    may contain a short amout of data from the study visit demo and a short amount of data from a
    function check of the returned device. These are filtered here.
    ASSUMES: Data outside the observation window, i.e. from before the valid visit date and
    after the return date has already been filtered out by pipeline_qa_csv_fname_to_p_visit.

    Args:
        s (dict): structure
        gap_min (int): minutes of gap (minimum) to consider separation of visit data
    Returns:
        s (dict): updated
    """
    if ('data_list' not in list(s['t'].keys())) or (len(s['t']['data_list']) < 1):
        # no filter needed... there is no data
        return s
    filter_level = s['r']['filter_level']
    if (filter_level < 2):  # this routine only does filtering level 2 and up
        # lvl 2 gets num_rows updated; do this here
        s['r']['num_rows'] = len(s['t']['data_list'])
        return s

    # create tdelta constants
    max_removal_span = timedelta(minutes=59, seconds=59)  # 1 hour
    min_gap_for_rm = timedelta(seconds=30)  # 30 secs
    max_rm_lines = 12 * 30  # 30 minutes

    # F2 - find and remove demo at participant visit
    #  only remove files within 1 hour of first saved data
    #  only remove files shorter than 30 minutes
    done_finding_gaps = False
    first_tstr = s['t']['data_list'][0].split(',')[0]  # string; 1st timestr in collected data

    while (not done_finding_gaps):
        # only look for another chop if we aren't already 1 hr past first data
        new_first_tstr_would_be = s['t']['data_list'][0].split(',')[0]
        tdelta_from_orig = get_elapsed_time(first_tstr, new_first_tstr_would_be)
        n_datalines = len(s['t']['data_list'])  # if very few left, not point in looking for more to remove
        if (tdelta_from_orig < max_removal_span) and (n_datalines > 0):
            # haven't already removed an hour, OK to keep looking as long as there a lines to consider
            found_chop, nline = find_short_run(s['t']['data_list'],
                                               min_gap_delta=min_gap_for_rm)
            # evaluate the findings
            msg = f'found_chop; nline is {nline}; evaluating impact...'
            utils_logger.debug(msg)
            if (found_chop):
                # new_first_tstr_would_be = s['t']['data_list'][nline].split(',')[0]
                # tdelta_from_orig = get_elapsed_time(first_tstr, new_first_tstr_would_be)

                if (nline < max_rm_lines):  # only if chop is < max number of removal lines
                    msg1 = 'meets requirement of not removing too much; begin trimming'
                    msg2 = f' data_list before: {len(s['t']['data_list'])}'
                    msg3 = f'   first row is {s['t']['data_list'][0]}'
                    utils_logger.debug(msg1)
                    utils_logger.debug(msg2)
                    utils_logger.debug(msg3)

                    new_data_list = s['t']['data_list'][nline:]
                    s['t']['data_list'] = new_data_list

                    msg1 = f' data_list after: {len(s['t']['data_list'])}'
                    msg2 = f'   first row is {s['t']['data_list'][0]}'
                    utils_logger.debug(msg1 + msg2)
                else:  # removal would be too large
                    done_finding_gaps = True  # only finding ones that are too much removal
            else:  # did not find a chop
                done_finding_gaps = True
        else:  # met or exceed max time span chop
            done_finding_gaps = True

    # update number of observations
    if 'r' not in list(s.keys()):
        s['r']['num_rows'] = 0  # ToDo: what was this trying to accomplish?

    msg = f' replacing former row count of {s['r']['num_rows']} with new count of {len(s['t']['data_list'])}'
    utils_logger.debug(msg)

    s['r']['num_rows'] = len(s['t']['data_list'])

    return s


def pipeline_qa_p_visit(s):
    """Check nnn, es_data_ok, visit_date, return_date are valid

    If values are not filled in, the file conversion will eventually fail but
    the following defaults are used to permit further checks to be made so as
    to get a more complete list of all of the issues:
        Actual first participant visit_date is 7/15/2023
        Bounds of study set to 7/1/2023
        Replacement visit_date set to 7/2/2023
    Args:
        s (struct)
    Returns:
        s (struct): updated with qa findings T/F and set_by this routine

    """

    if (not s['t']['p_visit']['esID'] == s['t']['nnn_fname']):
        s = eh(s, f'EnvSensor ID in folder name {s["t"]["nnn_fname"]}'
               + f' does not match esID in visit table {s["t"]["p_visit"]["esID"]}.')

    if (not s["t"]['p_visit']['es_data_ok'].lower() in ['yes', 'ok']):
        s = eh(s, f'Sensor data marked invalid ({s["t"]["p_visit"]["es_data_ok"]}) in visit table.')
        s['qa']['es_data_ok']['ok'] = False
    else:
        s['qa']['es_data_ok']['ok'] = True

    # date checks
    visit_dt = get_datetime_from_visit_date(s["t"]['p_visit']['visit_date'])
    return_dt = get_datetime_from_visit_date(s["t"]['p_visit']['return_date'])

    # unknown, TBD, etc. will be replaced to allow QA to proceed, but no export will occur
    if (not type(visit_dt) is datetime):
        s = eh(s, f'Visit date {visit_dt} not usable. Default to {CONST_DEFAULT_VISIT} to continue QA.')
        s["t"]['p_visit']['visit_date'] = CONST_DEFAULT_VISIT
        visit_dt = get_datetime_from_visit_date(CONST_DEFAULT_VISIT)
    if (not type(return_dt) is datetime):
        s = eh(s, f'Return date {return_dt} not usable. Default to today {str(date.today())} to continue QA.')
        s["t"]['p_visit']['return_date'] = str(date.today())
        # offset_hours=0 to avoid being flagged as future date in the tests below
        return_dt = get_datetime_from_visit_date(s["t"]['p_visit']['return_date'], offset_hours=0)  # avoids future warning

    if (return_dt < visit_dt):
        s = eh(s, f'Return {return_dt} is before visit {visit_dt} in visit table.')
        s['qa']['visit_date_before_return_date']['ok'] = False
    else:
        s['qa']['visit_date_before_return_date']['ok'] = True

    if (return_dt > datetime.now()):
        s = eh(s, f'Visit table has sensor return {return_dt} in the future.')
        s['qa']['return_date_not_in_future']['ok'] = False
    else:
        s['qa']['return_date_not_in_future']['ok'] = True

    if (visit_dt < get_datetime_from_visit_date(CONST_STUDY_START)):
        s = eh(s, f'Visit table has visit date {visit_dt} before study start.')
        s['qa']['visit_date_in_study_range']['ok'] = False
    else:
        s['qa']['visit_date_in_study_range']['ok'] = True

    # data and reported dates may be correct, but env sensor start cannot be inferred
    # if appt and visit dates do not match
    if (s["t"]['p_visit']['av_dates_match'] is False):
        s['qa']['appt_and_visit_dates_match']['ok'] = False
    else:
        s['qa']['appt_and_visit_dates_match']['ok'] = True

    s['qa']['es_data_ok']['set_by'] = inspect.stack()[0][3]
    s['qa']['visit_date_in_study_range']['set_by'] = inspect.stack()[0][3]
    s['qa']['return_date_not_in_future']['set_by'] = inspect.stack()[0][3]
    s['qa']['visit_date_before_return_date']['set_by'] = inspect.stack()[0][3]
    s['qa']['appt_and_visit_dates_match']['set_by'] = inspect.stack()[0][3]

    return s


def pipeline_get_sen55_from_nnn(s, esID_dict):
    """Get SEN55 16-char ID from static sensor dict,
        note whether it is found and which function set the flag
    Args:
        s (struct)
        esID_dict (dict): contains relationship of esID to SEN55
    Returns:
        s (struct): updated
    """

    if (s['t']['nnn_fname'] in esID_dict.keys()):
        sen55 = esID_dict[s['t']['nnn_fname']]['sen55']
        s['r']['SEN55'] = sen55
        s['qa']['nnn_in_sensor_dict']['ok'] = True
        s['qa']['nnn_in_sensor_dict']['set_by'] = inspect.stack()[0][3]

    else:
        s = eh(s, f'esID {s["t"]["nnn_fname"]} not found in sensor dict; no SEN55 retrieved.')
        s['r']['SEN55'] = 'NO_SEN55_IN_DICT'
        s['qa']['nnn_in_sensor_dict']['ok'] = False
        s['qa']['nnn_in_sensor_dict']['set_by'] = inspect.stack()[0][3]

    return s


def eh(s, emsg):
    """Error helper - combines msg creation, logging, and incrementing errorCount.
       Args:
            s (struct): see es_converter.convert_env_sensor for definition
            emsg (string): error message
       Returns:
            s - updated
    """
    utils_logger.error(emsg)
    s["t"]['conversion_issues'].append(emsg)
    s["t"]['errorCount'] += 1
    return s


def read_files(file_list, filter_level=1):
    """Reads each file in the file_list and gathers
        header, column name, and data
    information for further checking and exporting.

    Args:
        file_list (list of strings): Paths to the *.csv files to read
        filter_level (int): (Optional) Controls amount of filtering.
            0 - no filtering
            1 - remove files with fewer data lines than CONST_MIN_DATA_LINES
            ... additional levels of filtering are not performed here
    Returns:
        header_list_all (list): List of strings, one for each file header
        column_dict_all (dict): One dict for each file ...
        data_list_all (list): List of strings, one for each row of the file data
        err_dict (dict): Errors and warnings encountered
    """
    # reminders for when switch to passing in s
    # filter_level = s['r']['filter_level']
    # file_list = s['t']['file_list']

    header_list_all = list()
    column_dict_all = dict()
    data_list_all = list()
    err_dict = dict()
    num_orig_data_lines = 0

    msg = f"read_files:: file_list length is {len(file_list)}"
    utils_logger.info(msg)
    date_prev = None
    num_final_files = 0

    for idx, fname in enumerate(file_list):

        keep_file_data = True  # True until we find a reason not to keep it
        fname_short = os.path.basename(fname)
        sen55_id, header_list, column_dict, \
            data_list, f_err_dict = read_single_csv(fname, return_errs=True)

        nlines = len(data_list)
        # add all readable lines to the count of original lines prior to any filtering
        num_orig_data_lines += nlines

        # F0 - files with no data are always removed
        if (nlines == 0):  # oops... this was 2, should have been == 0
            msg = f'file {idx} is too short; remove. fname is {fname}'
            utils_logger.info(msg)
            f_err_dict['short file'] = f'{nlines} lines'
            keep_file_data = False

        # F1 - files shorter than the CONST_MIN_DATA_LINES are removed (~ 2 mins)
        if (filter_level > 0):
            if (nlines < CONST_MIN_DATA_LINES):
                msg = f"data_list < 2 for {fname}; may be an empty file"
                utils_logger.info(msg)
                keep_file_data = False

        # check date span if we're keeping the file
        if (keep_file_data):
            date_first = data_list[0].split(',')[0]
            date_last = data_list[-1].split(',')[0]
            msg = f'file #{idx} has {nlines} rows {date_first} to {date_last} fname is {fname}'
            utils_logger.info(msg)

            if date_prev is None:
                date_prev = date_last
            else:
                # explores time gap between files
                time_gap = get_elapsed_time(date_prev, date_first)  # need to do as timestamp
                # reminders on how to pull the time_gap apart
                # print(f'  {idx} time_gap is {type(time_gap)} {time_gap}')
                # print(f'  {idx} or in days {time_gap.days}')
                # print(f'  {idx} plus seconds {time_gap.seconds}')

                msg = f"file {idx} time_gap is {type(time_gap)} {time_gap}"
                utils_logger.info(msg)

        err_dict[fname_short] = f_err_dict

        # need to append them if we want to keep them...
        if (keep_file_data):
            num_final_files += 1
            header_list_all.extend(header_list)
            data_list_all.extend(data_list)
            column_dict_all[fname_short] = column_dict

    return header_list_all, column_dict_all, data_list_all, err_dict, num_orig_data_lines, num_final_files


def isValidTimestring(time_string):
    """T/F check whether string is YYYY-MM-DD HH:mm:ss"""
    dto = get_datetime_from_timestr(time_string)
    if (dto is False):
        return False
    else:  # is a datetime object, but not planning to use it; just need yes/no
        return True


def parse_data_row(myline, last_timestr, ngood_rows, fname, err_dict):
    """Helper function to report issues in a single row of timestamped data

    Checks for several possible issues in a single row, taking into account the
    timestring in the prior row of data:
        - corrupted timestamp
        - incorrect number of fields
        - new timestamp is older than previous timestamp (indicate of corrupted bytes)

    Args:
        myline (string): one row of data
        last_timestr (string): the last known good timestring
        ngood_rows (integer): how many good rows so far; used to pinpoint any error
        fname (string): file containing this row; used to pinpoint any error
        err_dict (dict): contains accrued issues to be reported at end of processing
    Returns:
        return_dict (dict): includes T/F that this row is ok to keep, and what the
            updated last_timestr should be
    """
    CONST_TDELTA_0 = timedelta(seconds=0)
    return_dict = {'ok_to_append': False, 'last_timestr': last_timestr}  # defaults

    fields = [str(x) for x in myline.split(',')]
    tstr = fields[0]
    if (not isValidTimestring(tstr)):
        return return_dict

    if (last_timestr != 'TBD'):
        t_delta = get_elapsed_time(last_timestr, tstr)
        emsg = f"TimestampRetro {last_timestr} then {tstr} in {fname} at line {ngood_rows + 1}"
        assert (t_delta > CONST_TDELTA_0), emsg
        # if t_delta < 0:  # going backwards in time!
        #     # break out of the for loop and throw away from this line to the end of the file
        #     err_msg = f'TimestampRetro {tstr} in {fname} at line {ngood_rows + 1}, omitting bad line and remainder of file.'
        #     utils_logger.info(err_msg)
        #     err_dict['TimestampRetro'] = f'{tstr}  at line {ngood_rows + 1}'
        #     break  # skip the data_list.append()
    if (len(fields) != CONST_NUMCOLS_RAWCSV):
        err_msg = f'IncorrectFieldCount in {fname} at line {ngood_rows + 1}, omitting bad line.'
        utils_logger.info(err_msg)
        err_dict['IncorrectFieldCount'] = f'line {ngood_rows + 1}'  # ToDo: decide if this needs to be returned or removed
    else:  # count of fields is ok, line wasn't extremely long, line not corrupted
        # data_list.append(myline)
        return_dict['ok_to_append'] = True
        # note the timestamp; any subsequent line in this file that is earlier signals an error
        last_timestr = tstr  # update this after reading and adjusting
        return_dict['last_timestr'] = tstr
    return return_dict


def read_single_csv(fname, return_errs=True):
    """Function to wrap reading the csv file and handle errors

    Examples of errors include
       - files with differing amounts of self-documenting information
       - files with mid-row byte corruption
       - files with corrupted timestamp in first field
       Return values can be reassembled into a pd.DataFrame

    Args:
        fname (string): Path to the *.csv to read

    Returns:
        sen55_id (string): 16 char SEN-55 ID from the environmental sensor
        header_list (list): List of lines in self-documenting header starting with ;
        column_string (string): Column names as a single comma-separated string
        data_list (list): CSV data as a list of strings, each string has comma-separated values
        err_dict (dict): dict of error type, line where found
    """
    sen55_id = 'unknown'
    count_header_rows = 0
    ngood_rows = 0

    header_list = list()
    column_dict = dict()  # may not want this to be dict; error in later calls
    column_string = 'no_cols_yet'
    data_list = list()
    err_dict = dict()

    fname_short = os.path.basename(fname)
    utils_logger.debug(f'debug info for {fname}')
    # print(f'debug info for {fname}')
    last_timestr = 'TBD'

    with open(fname, 'r') as f:
        try:
            for line in f:
                myline = line.strip()
                ngood_rows += 1
                if len(myline) > 160:  # was 155, then found some 159
                    print(f'Line length is {len(myline)}')  # typically 153 or less
                    err_msg = f'extreme line length {len(myline)} in {fname} at line {ngood_rows + 1}, omitting bad line.'
                    utils_logger.info(err_msg)
                    err_dict['ExtremeLineLength'] = f'line {ngood_rows + 1} has {len(myline)} characters'
                    # Examples: 32994, 13248 -- appeared to be corrupted characters
                else:
                    if myline[0] in [';', '#']:  # first lines should start with ;
                        header_list.append(line)
                        count_header_rows += 1
                        if myline[0] == ";":  # raw file
                            if 'SEN55' in myline:
                                sen55_id = myline.split(' ')[-1]
                    elif myline[:2] == "ts":  # after header, col names and first col is timestamp
                        column_dict[fname_short] = myline
                        column_string = myline.strip("'")
                        cols = [str(x) for x in myline.strip("'").split(',')]
                        msg = f'cols {cols}'
                        utils_logger.info(msg)
                    else:  # after header and col names, should get the data
                        # create a subroutine to parse data row to handle all the corner case errors
                        return_dict = parse_data_row(myline, last_timestr, ngood_rows, fname, err_dict)

                        if return_dict['ok_to_append']:
                            data_list.append(myline)
                        last_timestr = return_dict['last_timestr']

        # option to group them as (UnicodeDecodeError, nameofotherError)
        except UnicodeDecodeError as e:
            # Exception has occurred: UnicodeDecodeError
            # 'utf-8' codec can't decode byte 0xf1 in position 98320: invalid continuation byte
            err_msg = f'UnicodeDecodeError reading {fname} at line {ngood_rows + 1}, omitting bad lines.' + \
                      f' Keep these rows: {range(count_header_rows, ngood_rows, 1)}'
            utils_logger.info(err_msg)
            err_dict['UnicodeDecodeError'] = f'line {ngood_rows + 1}'
            utils_logger.info(f'Reason: {e.reason}')
        # custom exceptions
        except AssertionError as e:
            err_msg = f'CUSTOM_EXCEPTION {e}; remaining lines in file will be discarded.'
            utils_logger.warning(err_msg)

    return sen55_id, header_list, column_string, data_list, err_dict


def pipeline_qa_match_sen55(s):
    """Check that sen55 in the raw csv match expected sen55 from sensor_dict
    """
    if len(s['t']['sen55_list']) > 0:
        uniq_sen55 = s['t']['sen55_list'][0]
    else:
        uniq_sen55 = 'missing'

    if (s['r']['SEN55'] == uniq_sen55):
        s['qa']['csv_sen55_matches_nnn_from_sensor_dict']['ok'] = True
    else:
        s = eh(s, f'SEN55 value {uniq_sen55} does not match expected value {s["r"]["SEN55"]}')
        s['qa']['csv_sen55_matches_nnn_from_sensor_dict']['ok'] = True
    s['qa']['csv_sen55_matches_nnn_from_sensor_dict']['set_by'] = inspect.stack()[0][3]
    return s


def pipeline_qa_hdr_list(s, field, chdr_field, qa_field):
    """Check that all FW versions in the raw files were the same; record the FW version.
        Example: for field 'fw_version_list', check
            'fw_version_list': ['1.2.4', '1.2.4', '1.2.4', ...]
    """
    rf_set = sorted(list(set(s['t'][field])))  # raw csv field
    if (len(rf_set) == 1):
        s['r'][chdr_field] = rf_set[0]
        s['qa'][qa_field]['ok'] = True
    else:
        emsg = f'Expecting exactly one value in {field}; found {rf_set}.'
        s = eh(s, emsg)
        s['qa'][qa_field]['ok'] = False

    s['qa'][qa_field]['set_by'] = inspect.stack()[0][3]

    return s


def qa_header(header_list, column_dict):
    """Confirms column list is consistent, SEN55 ID is consistent.

    Args:
        header_list (list of strings): example shown below
            header_list = ['; Version: 1.2.4\n', '; SEN55 056C8FDFDB965372\n',
                            '; Version: 1.2.4\n', '; SEN55 056C8FDFDB965372\n',
                            '; Version: 1.2.4\n', '; SEN55 056C8FDFDB965372\n']
        column_dict (dict): where k = fname and v = column names in that file
            column_dict = {'20230829000101.csv': 'ts,val0,val1',
                            '20230829010101.csv': 'ts,val0,val1'}
    Returns:
        header_dict (dict), col_names (string)
    """
    # confirm header values for SEN55 ID and FW version are consistent
    #  mismatch indicates a device error or
    #                     a folder with a mix of files from multiple devices
    h_dict = dict()
    header_dict = dict()
    #    first get the values
    for h in header_list:
        # separate into keys ['Version:', 'SEN55']
        #              values['1.2.4',    '056C8FDFDB965372]
        h_list = h[1:].strip().split(" ")  # FW 1.2.4 has k, v
        if len(h_list) != 2:
            err_msg = f"ERROR - header elements !=2 {h}"
            utils_logger.error(err_msg)
        else:
            h_key, h_val = h_list[:2]
            if h_key not in h_dict.keys():
                h_dict[h_key] = list()  # currently only keeping a list of vals
            h_dict[h_key].append(h_val)
    #    then check that they are all the same
    for k, v in h_dict.items():
        if all_same(v):
            utils_logger.debug("{k} is consistently {v[0]}")
            header_dict[k] = v[0]
        else:
            # num_qa_concerns += 1
            err_msg = f"ERROR - header information {k} has a mismatch {v}"
            utils_logger.error(err_msg)

    col_hdr_list = list(column_dict.values())
    column_names = "col_names_tbd"

    if all_same(col_hdr_list):
        msg = "All *.csv column headers match; data should align."
        utils_logger.info(msg)
        column_names = col_hdr_list[0]
    else:
        msg = "col headers not the same for all *.csv files; data may not align"
        utils_logger.error(msg)
        utils_logger.info(column_dict)
    return header_dict, column_names


# Tools for working with ENV folder names, csv files, and headers; originally es_utils_pilot.py


def get_filename_stem(fname):
    """Given /some/path/20230904112231.csv, returns 20230904112231 as a string"""
    basefile = os.path.basename(fname).split('.')[0]
    return basefile


def get_datetime_from_fname(fname):
    """Given the stem of a date-stamped csv as a string, return a datetime object
        Example usage:
        fdate_obj = get_datetime_from_fname('20230904112231')
    """
    datetime_object = datetime.strptime(fname, '%Y%m%d%H%M%S')  # 4 digit Year
    return datetime_object


def get_datetime_from_timestr(t_str):
    """Given the string timestamp in a *.csv file, return a datetime object
        Example usage:
        fdate_obj = datetime_from_timestampstr('2023-11-18 00:19:23')
    """
    try:
        datetime_object = datetime.strptime(t_str, '%Y-%m-%d %H:%M:%S')  # 4 digit Year
    except Exception as e:
        print(f'bad timestamp {e}')
        return False

    return datetime_object


def isValid_pID(pid):
    """Valid participant IDs are 4 digit numbers between 1000 and 9999 saved as strings.
        Args:
            pid (string): valid if between 1001 and 9999
        Returns: True / False
    """
    if pid == 'NPID':
        return False

    if (type(pid) is str) & (int(pid) > 1000) & (int(pid) <= 9999):
        return True
    else:
        return False


def isValid_esID(esid):
    """Valid sensor IDs are numbers from 001 through about 300 saved as strings."""
    retval = False
    if (type(esid) is str):
        if (len(esid) == 3):
            retval = True
    return retval


def get_csv_info(fname):
    """Given a csv file, return a minimal set of info as a dict().

    Intended to capture the readable information in a raw EnvSensor file and
    gracefully handle any unreadable characters or early line terminations.

    Args:
        fname (string): full path to csv file to read
    Returns:
        dict() with these key, values pairs:
            sen55_id : the value found in the csv file, or 'no_sen55_id' if not found
            num_hdr_lines : the number of lines prefixed with ; or #
            num_data_lines : the number of lines of data (not header or col names)
            num_readable_lines : number of lines read before encountering any read errors
                num_readable_lines = num_hdr_lines + num_data_lines + 1 column header row
            first_timestamp: timestamp of the first data row
            last_timestamp: timestamp of the last data row
                if there are no data rows, the *timestamp values will be 'no_timestamp'
                if there is only 1 data row, both timestamps will be the same
            isFullyReadable : True if all lines of the file were read without error
            orderedTimestamps : True if timestamps in all rows are an increase of 1 to 10 seconds
                from previous row
    """

    info_dict = {
        'sen55_id': 'no_sen55_id',

        'num_hdr_lines': 0,
        'columns': [],
        'num_data_lines': 0,
        'num_readable_lines': 0,

        'first_timestamp': 'no_timestamp',
        'last_timestamp': 'no_timestamp',

        'isFullyReadable': True
    }

    #  first_timestamp = 'no_timestamp'
    #  last_timestamp = 'no_timestamp'
    ngood_rows = 0
    num_hdr_lines = 0
    num_data_lines = 0
    nbad_time_increase = 0
    nbad_rows = 0  # wrong number of fields

    most_recent_timestamp = None

    with open(fname, 'r') as f:
        try:
            for line in f:
                myline = line.strip()
                ngood_rows += 1
                if myline[0] in [';', '#']:  # first lines should start with ;
                    num_hdr_lines += 1
                    if myline[0] == ";":  # raw file
                        if 'SEN55' in myline:
                            info_dict['sen55_id'] = myline.split(' ')[-1]
                            utils_logger.info(f'  Found SEN55: {info_dict["sen55_id"]}')
                elif myline[:2] == "ts":  # list of column names
                    cols = [str(x) for x in myline.strip("'").split(',')]
                    num_cols = len(cols)  # expecting 22 at this time
                    if (num_cols != CONST_NUMCOLS_RAWCSV):
                        msg = f'File {f} has {num_cols} in header instead of {CONST_NUMCOLS_RAWCSV}'
                        utils_logger.error(msg)
                        # short data lines are handled elsewhere; unclear if short header is an issue
                else:  # after header and col names, should get the data
                    num_data_lines += 1

                    # check the number of fields in the line
                    all_fields = myline.split(',')
                    ts = all_fields[0]

                    if (len(all_fields) != num_cols):  # if bad row, do not use timestamp
                        nbad_rows += 1
                    utils_logger.error(f'{len(all_fields)} field names vs {len(cols)} in {fname}')
                    # try using the timestamp as long as the row has 2 or more fields
                    if (len(all_fields) > 1):
                        # record first and last timestamp
                        if (info_dict['first_timestamp'] == 'no_timestamp'):
                            info_dict['first_timestamp'] = ts
                            info_dict['last_timestamp'] = ts
                        else:
                            info_dict['last_timestamp'] = ts  # TBD should we record this if the line was bad?

                        # calculate row to row time delta
                        if most_recent_timestamp is None:  # handles the very first line; no tdelta possible
                            most_recent_timestamp = ts
                        else:
                            tprev_as_dt = get_datetime_from_timestr(most_recent_timestamp)
                            tnow_as_dt = get_datetime_from_timestr(ts)
                            tdelta = tnow_as_dt - tprev_as_dt

                            if ((tdelta > timedelta(seconds=1)) and (tdelta < timedelta(seconds=15))):
                                pass  # pass if time is more than 1 second or less than 15 seconds
                            else:
                                utils_logger.info(f'{fname} prev {most_recent_timestamp} now {ts} --> bad delta {tdelta} at {ngood_rows}')
                                nbad_time_increase += 1

                            most_recent_timestamp = ts  # save current timestep for use as prev on next go around

        except UnicodeDecodeError:
            # Exception has occurred: UnicodeDecodeError
            # 'utf-8' codec can't decode byte 0xf1 in position 98320: invalid continuation byte
            info_dict['isFullyReadable'] = False

    # already set: 'first_timestamp', 'last_timestamp'
    info_dict['num_readable_lines'] = ngood_rows
    info_dict['num_hdr_lines'] = num_hdr_lines
    info_dict['columns'] = cols
    info_dict['num_data_lines'] = num_data_lines
    info_dict['num_bad_tdelta'] = nbad_time_increase
    info_dict['num_bad_rows'] = nbad_rows
    return info_dict


# Tools for working with the sensor ID dictionary


def build_es_dict(build_csv=None):
    """Read es table and create dict to find 16-digit sen55 from 3-digit unit ID.

    Args:
        csv_file (string): full path to a csv file containing the sensor build info
            if None, the included asset file is used.
    Returns:
        sensor_dict (dict): 3-char esID as first key
                                value is dict of original esID (integer), sen55, and site
            {'011': {'esID': 11, 'sen55': 'C796DB182B49FC0C', 'site': 'site_01'},
                '012': {'esID': 12, 'sen55': 'E78769BAFA9BE9E7', 'site': 'site_01'} ... }
    """
    # read the es_id as an integer and convert to 3 char later so that all esID values
    # are handled the same way
    if (build_csv is None):
        asset_fname = 'es_sensor_id.csv'
        asset_data = pkgutil.get_data(__name__, asset_fname)  # creates one big binary string
        df = pd.read_csv(io.BytesIO(asset_data),  # separate the binary string into rows
                         skiprows=10,  # header section
                         encoding='utf8', sep=',',
                         dtype={'esID': int, 'sen55': str})
        utils_logger.info(f'Using default sensor_id asset file {asset_fname}')
    else:
        # note that this presumes there are no rows to skip
        df = pd.read_csv(build_csv, dtype={'esID': int, 'sen55': str})
        utils_logger.info(f'Read sensor ids from {build_csv}')
    utils_logger.info(f'columns: {df.columns}')

    def id_3char(es_id):
        if len(str(es_id)) < 3:
            es_id = '0' + str(es_id)
        return str(es_id)

    df['es_id_3char'] = df.apply(lambda r: id_3char(r['esID']), axis=1)
    sensor_dict = df.set_index('es_id_3char').T.to_dict()

    return sensor_dict


def get_expected_sen55_from_esID(es_id_3char, sensor_dict):
    """Given 3-char es_ID, return 16-char sen55 from sensor_dict

    Args:
        es_id_3char (string): 3 character sensor ID e.g. 052
        sensor_dict(dict): dictionary that can find the 16 digit SEN55 ID from the esID
    Returns:
        sen55 16-char ID
    """
    if es_id_3char in sensor_dict.keys():
        expected_sen55 = sensor_dict[es_id_3char]['sen55']
        return expected_sen55
    else:
        utils_logger.error(f'es_id {es_id_3char} not found in EnvSensor ID table')
        msg_str = f'ERROR: es_id {es_id_3char} not found in EnvSensor ID table'
        return msg_str


# Tools for working with the visit dictionary


def build_visit_dict(visit_csv):
    """Opens a file with visit information and builds a dictionary.

    THIS VERSION works with the new short-field name csv from REDCap
    Args:
        visit_csv (string): full path to a csv of the visit information
            or None, which will create a minimal visit dict with key 0000 only
    Returns:
        visit_dict (dict): dictionary of key items
    """
    # Create a minimal dict with unused participant ID 0000 only.
    # this permits other checks to provide feedback on the data content even though
    # processing cannot yet be finalized
    default_visit_entry = {'site': 'no_site',
                           'visit_appt_time': '2023-07-02 14:00',
                           'appt_date': '2023-07-02',
                           'visit_date': '2023-07-02',
                           'av_dates_match': True,
                           'esID': '000', 'return_date': 'TBD',
                           'location': 'TBD', 'es_data_ok': 'yes'}
    visit_dict = dict()
    utils_logger.info(f'Using visit_csv {visit_csv}')

    if (visit_csv is None):
        visit_dict = {'0000': default_visit_entry}
    else:
        try:
            df = pd.read_csv(visit_csv, dtype={"studyid": str,  # participant ID pppp
                                               "dvenvsn": str})  # env sensor ID nnn
            utils_logger.info(f'Visit csv orig has columns: {df.columns}')

            df = df.rename(columns={'studyid': 'pid',
                                    'siteid': 'site',
                                    'visdat': 'visit_appt_time',  # includes time
                                    'pacmpdat': 'visit_date',
                                    'dvenvendat': 'return_date',
                                    'dvenvdwnd': 'es_data_ok',
                                    'dvenvsn': 'esID',
                                    'dvenvlocn': 'location'})

            df = df.drop(columns=['dvenvenyn', 'dvenvyn',
                                  'dvenvstcrcid',
                                  'dvamwstcrcid', 'dvamwendwnd',
                                  'dvamwenhand', 'dvamwendhand'])

            # replace empty values with 'TBD' to avoid having to test for nan later
            tbd_cols = ['return_date',
                        'es_data_ok',
                        'location']
            df[tbd_cols] = df[tbd_cols].fillna('TBD')

            # convert appointment date and time to just date; flag if appt and visit match
            df['appt_date'] = df.apply(lambda row: row['visit_appt_time'].split(' ')[0], axis=1)
            df['av_dates_match'] = df.apply(lambda row: True if row['appt_date'] == row['visit_date'] else False, axis=1)

            # ensure esID is a 3-char string
            def nnn_to_3char(x):
                x_str = str(x)
                if len(x_str) < 3:
                    x_str = '0' + x_str
                # print(f'esID value is now .{x_str}.')
                return x_str
            df['esID'] = df.apply(lambda x: nnn_to_3char(x['esID']), axis=1)

            # new file uses 1.0 and 0.0 instead of 'yes' 'ok' and 'no'
            df['es_data_ok'] = df.apply(lambda x: 'yes' if x['es_data_ok'] == 1.0 else 'no', axis=1)

            visit_dict = df.set_index('pid').T.to_dict()

            # replace characters in the location text that need to be reserved for other files
            for v in visit_dict.values():
                orig_loc = v['location']
                new_loc = orig_loc.replace(':', ' ')  # : is used in the self-doc header
                new_loc = new_loc.replace('\t', ' ')  # \t is used in the manifest.tsv
                # new_loc = new_loc.replace('L','l')  # just for testing
                v['location'] = new_loc

        except Exception as e:  # most likely FileNotFoundError
            str1 = f'{e}'
            str2 = 'relying on default_dict with key 0000 only'
            utils_logger.error(f'Unable to build visit_dict from {visit_csv} due to {str1}; {str2}')

    # in all cases, add default. if visit_csv failed, it will be the only entry
    visit_dict['0000'] = default_visit_entry  # use for pid not in dict

    return visit_dict


def get_visit_esID(pid, visit_dict):
    """Retrieves the visit information using the participantID as the key

    Args:
        pid (string): 4 digit participant identifier
        visit_dict (dict): dict with pid as keys and the sensor ID as a value
    Returns:
        sensor_id (3-char string)
    """
    if pid in visit_dict.keys():
        pdict = visit_dict[pid]
        return str(pdict['esID'])
    else:
        return f'No such ID {pid} in visit_dict'


def get_datetime_from_visit_date(date_string, offset_hours=13):
    """Gets visit day and adds hours

    Intended for use with the visit_date and return_date which do not have hh:mm:ss
        1 p.m. was selected as a reasonable average for both dates and to ensure
        an integer number of days

    Args:
        date_string (string): date e.g. '2024-05-21'
        offset_hours (integer): number of hours to add to the date_string
    Returns:
        adjusted date as a date_string, e.g. '2024-05-21 13:00:00'
    """

    if date_string == 'TBD':
        return 'TBD'
    else:
        try:
            # no hour in date_string, so make it 1 p.m. for all cases
            datetime_object = datetime.strptime(date_string, '%Y-%m-%d') + \
                timedelta(hours=offset_hours)
            return datetime_object
        except Exception as e:
            emsg = f'Error {e}; unable to get datetime from visit date info.'
            utils_logger.error(emsg)
            return 'TBD'


def isInTimeWindow(date_to_check, window_start, window_end):
    """Given a date to check and the start and end of the window, return an
    indication of whether the date is in the time window

    Args:
        date_to_check (datetime object): the date to consider
        window_start (datetime object):
        window_end (datetime object):
    Returns:
        bool: whether or not the date is in the window
    """
    if (date_to_check > window_start) and (date_to_check < window_end):
        return True
    else:
        return False

# Tools for auditing the EnvSensor folder data


def read_single_csv_first_data(fname):
    """Read csv file and return first data line

    Reads one csv file with header lines, 1 column name row, then data;
    returns the first line of actual data.

    Args:
        fname (string): path the file to read
    Returns:
        first_timestamp_line (string): first full timestamped data line in the file
        # ToDo: what happens if there isn't a first line of data?
    """
    ngood_rows = 0  # total number of good rows in the file
    found_first_ts_line = -1
    # fopen_ok = False

    with open(fname, 'r') as f:
        # fopen_ok = True
        try:
            for line in f:
                myline = line.strip()
                ngood_rows += 1
                if (myline[0] not in [';', '#', 't']):  # header and col names
                    # raw data uses ;
                    # sensor table and final data use #
                    found_first_ts_line = ngood_rows
                    first_timestamp_line = line
                    break
            # print(f'Found first timestamp_line: {line} in {f} at {found_first_ts_line}')
        except UnicodeDecodeError as e:
            # Exception has occurred: UnicodeDecodeError
            # 'utf-8' codec can't decode byte 0xf1 in position 98320: invalid continuation byte
            utils_logger.error(f'Reason: {e.reason}')
            print(f'DEBUG: UnicodeDecodeError in {f} at ngood_rows {ngood_rows}')

    if (found_first_ts_line < 3):
        # this can happen with a short file that contains no data
        emsg = f'DEBUG: Problem getting to timestamp in {f}'
        utils_logger.error(emsg)
        print(emsg)

    return first_timestamp_line


def pipeline_qa_csv_fname_to_p_visit(s):
    """Check that each csv filename is between visit_date and return_date

    Checks that each filename is within the observation window; this will remove any files
    leftover from a prior participant or a function test performed prior to a new participant
    visit. Observation window time start and end are calculated from date alone as follows:
        - Env sensor timestamps are UTC. On a give date, say YYYY-11-06, starred times are the key limits
                UTC              central          pacific
                3 p.m.      -->  *9 a.m.    -->    7 a.m.     earliest participant visit data
                2 a.m. +1d  -->   8 p.m.    -->   *6 p.m.     latest participant return data (manual drop off)
            9 a.m. central time is YYYY-11-06 15:00Z (+15h) ; earliest possible participant visit data start
           10 p.m. pacific time is YYYY-11-07 02:00Z (+26h) ; latest possible returned device check in
        - Dates are adjusted for these factors to better tailor the filtering of the observation window
        - If files are removed, information will be preserved in the log file, but no error
        flags will be set.
    Args:
        s (dict): contains the list of csv files and the participant visit and return dates
    Returns:
        s (dict): updated list of csv files with any out-of-window files removed
    """
    count_out_of_window_files = 0
    # adjust hours conservatively to leave lots of time
    visit_datetime = get_datetime_from_visit_date(s["t"]['p_visit']['visit_date'], offset_hours=15)
    return_datetime = get_datetime_from_visit_date(s["t"]['p_visit']['return_date'], offset_hours=26)
    exclude_list = list()

    for idx, cfile in enumerate(s['t']['file_list']):
        fname = cfile.split('/')[-1].split('.')[0]
        fname_as_datetime = get_datetime_from_fname(fname)
        if isInTimeWindow(fname_as_datetime, visit_datetime, return_datetime):
            pass
        else:
            msg = f'File in csv_list is outside observation window: {cfile}'
            utils_logger.debug(msg)
            count_out_of_window_files += 1
            exclude_list.append(cfile)

    final_list = [x for x in s['t']['file_list'] if x not in exclude_list]
    s['t']['file_list'] = sorted(final_list)

    # record number of files tossed out
    if (count_out_of_window_files > 0):
        wndw = f'{s["t"]['p_visit']['visit_date']} - {s["t"]['p_visit']['return_date']}'
        wndw = f'{visit_datetime} - {return_datetime}'
        exc_short = [x.split('/')[-1] for x in exclude_list]
        estr = ','.join(exc_short)
        s['t']['conversion_issues'].append(f'INFO: {len(exclude_list)} files were outside the observation window {wndw} and removed. {estr}')
    return s


def pipeline_qa_csv_drop_short_files_from_list(s):
    """Opens each file and confirms that it is readable and has at least 1 row of data.
    Files that do not meet these criteria are removed from the list and reported
    in the processing history as information. First few lines are expected to contain

    ; Version: 1.2.4                        # expect 17 chars
    ; SEN55 77.. serial number              # expect 25 chars
    ts, ... the list of column names        # expect 109 chars
    2024-08-08 00:00:00, ... values         # expect > 0 chars

    Args:
        s (dict): structure containing list of files to check
    Returns:
        s (dict): updated csv list and add to 'qa' section reflecting the result of this check
    """
    drop_list = list()
    for f in s['t']['file_list']:
        # failure only if < 4 lines
        try:
            with open(f, 'r') as this_f:
                for n in range(4):
                    r = this_f.readline()  # can print during debugging
                    msg = f'  line {n}: {len(r)} chars, {r}'
                    utils_logger.debug(msg)
            if (len(r) < 21):  # expect closer to 140, but 21 will cover the timestamp
                msg = f'File is < 4 lines, drop it. {f}'
                utils_logger.debug(msg)
                drop_list.append(f)
        except UnicodeDecodeError as e:
            emsg = f'File contains an invalid byte in the first 4 lines and should be dropped...{f}'
            emsg += f' e is {e.reason}'
            utils_logger.warning(emsg)
            drop_list.append(f)

        except Exception as e:
            emsg = f'File contains a read error in the first 4 lines and should be dropped...{f}'
            emsg += f' e is {e}'
            drop_list.append(f)
            utils_logger.warning(emsg)

    s['t']['file_drops'] = drop_list
    keep_list = [x for x in s['t']['file_list'] if x not in drop_list]
    msg = f'drop_short_files: orig list was {len(s['t']['file_list'])} and drop_list is {len(drop_list)}'
    utils_logger.debug(msg)

    s['t']['file_list'] = keep_list
    msg = f'...final list is {len(s['t']['file_list'])}'
    utils_logger.debug(msg)

    return s


def pipeline_qa_csv_namedelta_to_timestamp1(s):
    """Opens each file and compares the filename (which is basically a timestamp) with
    the timestamp of the first row of data. This timedelta should be within the specified
    tolerance (default is 10 seconds).
    If any file has a timedelta out of range, the qa flag is set to False, and the final data
    will not be exported. All contents of the folder will need to be reviewed.
    Assumes all files < 4 lines have already been removed, there should exist at least 1 data row.

    Args:
        s (dict): structure containing list of files to check
    Returns:
        s (dict): updated to 'qa' section reflecting the result of this check
    """
    s['qa']['csv_fname_to_first_timestamps_checked']['ok'] = True  # until find one that's false

    for f in s['t']['file_list']:
        first_timestring = read_single_csv_first_data(f).split(',')[0]  # returns first line
        # check if the timestamp is valid; if not, handle the error here and return False
        if (isValidTimestring(first_timestring)):
            # dto = get_datetime_from_timestr(first_timestring)
            fname_tdelta_ok = audit_csv_namedelta(f, first_timestring)
        else:
            fname_tdelta_ok = False  # if invalid, then can't check, so fails

        if (not fname_tdelta_ok):
            eh(s, f'First timestamp {first_timestring} does not make sense with filename {f}')
            s['qa']['csv_fname_to_first_timestamps_checked']['ok'] = False

    s['qa']['csv_fname_to_first_timestamps_checked']['set_by'] = inspect.stack()[0][3]

    return s


def audit_csv_namedelta(name_str, first_timestamp_str, tol=10):
    """Checks first timestamp relative to filename and ensures < tolerance seconds
    Assumes: first timestamp is valid
    Anything longer may be an indication of an incorrectly renamed file.

    Args:
        name_str (string): name of the file including the timestamp
        first_timestamp_str (string): first timestamp of the file as a string
        tol (integer): timedelta tolerance in seconds
    Returns:
        boolean - True if ok, else False
    """
    stem = get_filename_stem(name_str)
    filename_as_dt = get_datetime_from_fname(stem)

    t1_as_dt = get_datetime_from_timestr(first_timestamp_str)
    time_gap = t1_as_dt - filename_as_dt  # must be positive number less than tolerance
    dt_tolerance = timedelta(seconds=tol)

    if ((time_gap < dt_tolerance) and (-1 * dt_tolerance < time_gap)):
        # print(f'OK fname {fstem} to first timestamp {t1} is delta {delta_time}')
        return True
    else:
        return False
