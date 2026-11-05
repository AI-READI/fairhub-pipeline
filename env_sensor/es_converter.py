"""Conversion helpers for environmental sensor CSV files."""

import logging
import pkgutil

try:
    from env_sensor import es_utils
except ImportError:
    from . import es_utils

conv_logger = logging.getLogger("es.converter")
conv_logger.addHandler(logging.NullHandler())


def es_export_simple(fname, col_names, data_list, wora="w", verbose=False):
    """Write out env sensor data as a csv without the extended header info."""
    if verbose:
        conv_logger.info(f"export_simple:: writing to {fname}")
    with open(fname, wora) as f:
        f.write(f"{col_names}\n")
        for line in data_list:
            f.write(f"{line}\n")


def es_export(fname, col_names, data_list, header_dict, verbose=False):
    """Write out env sensor data as an enhanced csv with self-documenting header info."""
    if verbose:
        conv_logger.info(f"export:: writing to {fname}")

    with open(fname, "w") as f:
        f.write("# Environmental sensor self-documenting header\n")
        for k, v in header_dict.items():
            f.write(f"# {k} {v}\n")
    es_export_simple(fname, col_names, data_list, wora="a")


def customize_selfdoc_header(chdr, header_template_file=None):
    """Reads the self-documenting header template and inserts custom meta data.

    Args:
        chdr (dict): dictionary of custom header values to be inserted
        header_template_file (string): optional path to a template file
    Returns:
        list of self-documenting header strings (lines)
    """

    # Read the header template
    if header_template_file is None:
        hdr_template_asset = "es_doc_header.txt"
        asset_data = pkgutil.get_data(__name__, hdr_template_asset)
        hdr_template_lines = asset_data.decode("utf-8").splitlines()
        conv_logger.info(
            "Header template asset "
            + f"{hdr_template_asset} loaded with {len(hdr_template_lines)} lines."
        )
    else:
        with open(header_template_file, "r") as f:
            hdr_template_lines = [line.rstrip("\n") for line in f]
        conv_logger.info(
            "Header template file "
            + f"{header_template_file} loaded with {len(hdr_template_lines)} lines."
        )

    # Customize the header lines
    hdr_line_list = list()
    observation_extent = chdr["num_rows"] / (12.0 * 60 * 24)  # convert to days

    # Fill in the custom values for the header
    ch = {
        "# meta_sensor_id": ": " + str(chdr["SEN55"]),
        "# meta_participant_id": ": " + str(chdr["pppp"]),
        "# meta_sensor_location": ": " + chdr["location"],
        "# meta_number_of_observations": ": " + str(chdr["num_rows"]),
        "# meta_extent_of_observation_in_days": ": "
        + str(round(observation_extent, 1)),
        "# environmental_sensor_firmware_version": ": " + chdr["fw_version"],
    }
    
    if "hw_ver" in chdr:
        ch["# environmental_sensor_hardware_version"] = ": " + str(chdr["hw_ver"])

    for line in hdr_template_lines:
        myline = line.strip().split(":")
        field = myline[0]

        if field in ch.keys():
            pline = field + ch[field]
            hdr_line_list.append(pline)
        else:
            hdr_line_list.append(line.strip())

    return hdr_line_list  # return the customized header


def pipeline_es_export(s, header_template_file=None):
    """Writes out env sensor data as an enhanced csv with self-documenting header info.

    Args:
        fname (string): path and filename of where to write or append the data
        col_names (string): csv row providing the columns names
        hdr_custom_values (dict): header element keys, values used to customize the header section
    Returns:
        s (dict): status, error counts, etc.
    """
    if "p_visit" in s["t"] and "hw_ver" in s["t"]["p_visit"]:
        s["r"]["hw_ver"] = str(s["t"]["p_visit"]["hw_ver"])

    hdr_line_list = customize_selfdoc_header(
        s["r"], header_template_file=header_template_file
    )

    qa_complete = True  # until proven false...
    qa_conv_issues = "QA failed checks include: "
    for k in s["qa"].keys():
        if s["qa"][k]["ok"] is not True:
            qa_conv_issues += f" {k} not ok. "
            qa_complete = False
    if qa_complete is False:
        s["t"]["conversion_issues"].append(qa_conv_issues)
        s["t"]["errorCount"] += 1

    if qa_complete and (s["t"]["errorCount"] == 0):
        try:
            with open(s["t"]["outfile_posixpath"], "w") as f:
                # Write the self-documenting header.
                for k in hdr_line_list:
                    f.write(f"{k}\n")

                # Write the column names after the header.
                f.write(f"{s['r']['column_names']}\n")

                # write rows of data
                for line in s["t"]["data_list"]:
                    f.write(f"{line}\n")
            s["output_file"] = str(s["t"]["outfile_posixpath"])
            s["conversion_success"] = True
        except Exception as e:
            err_msg = (
                f'Problem {e} writing the output file {s["t"]["outfile_posixpath"]}'
            )
            conv_logger.error(err_msg)
            conv_logger.exception("pipeline_es_export encountered an exception")
            s["conversion_success"] = False
            s["output_file"] = None

    else:
        msg = f'Skipping final export due to {s["t"]["errorCount"]} errors for {s["t"]["input_path"]}'
        conv_logger.error(msg)
        s["conversion_success"] = False
        s["output_file"] = None

    return s


def create_struct_s(input_path, output_folder, filter_level):
    """Create initial structure for the conversion pipeline
    s has 4 top level keys:
        't' - temporary data only
        'qa' - notes from doing QA
        'r' - key results to report
        'conversion_success' - T/F whether conversion was completed
        'output_file' - path to output file if conversion was successful
    """
    s = {
        "t": {  # temporary values
            "input_path": input_path,
            "output_folder": output_folder,
            "errorCount": 0,
            "conversion_issues": [],  # includes errors and warnings
            # Remaining items are filled in with correct values as pipeline progresses
            # 'pppp_fname': # value from input_path, a.k.a. pID
            # 'nnn_fname': # value from input_path
            # 'p_visit': None,  # selected items of pID's info from visit_dict
            # 'outfile_posixpath': 'TBD',  # assemble from validated info above
            "file_list": [],  # raw csv files
            "file_drops": [],  # list of dropped raw csv files
            # simplify filter dev and debug by keeping some stats
            "num_orig_files": 0,
            "num_final_files": 0,  # subtract every time we filter one out
            "num_orig_data_lines": 0,
            # 'num_final_data_lines': 0,
            # filled in as part of merging all csv files:
            # 'fw_version_list': [],  # list of all FW versions in the headers of raw csv files
            # 'sen55_list': [],  # list of all SEN55 IDs found in the headers of raw csv files
            # 'column_dict':  # is this still used
            # 'data_list': [],  # consumes a lot of memory - delete when no longer needed
            # 'err_dict_from_read_files':  # is this still used
            # updated as values are fetched
        },
        "r": {  # key return values, formerly hdr_custom_values
            "fw_version": "unknown",
            "sen55": "NO_SEN55_IN_DICT",  # ...get_sen55_from_nnn
            "column_names": "unknown",
            "location": "unknown",
            "num_rows": 0,  # number of final data rows
            "filter_level": filter_level,
            "pppp": "NPID",
        },
        "qa": {  # definitive QA checklist and results
            # ENV-pppp-nnn foldername validation
            "pppp_fname_well_formatted": {"ok": "TBD", "set_by": "unknown"},
            "nnn_fname_well_formatted": {"ok": "TBD", "set_by": "unknown"},
            "pppp_in_visit_dict": {"ok": "TBD", "set_by": "unknown"},
            "nnn_in_sensor_dict": {"ok": "TBD", "set_by": "unknown"},
            # participant visit data valid
            "visit_date_in_study_range": {"ok": "TBD", "set_by": "unknown"},
            "visit_date_before_return_date": {"ok": "TBD", "set_by": "unknown"},
            "return_date_not_in_future": {"ok": "TBD", "set_by": "unknown"},
            "es_data_ok": {"ok": "TBD", "set_by": "unknown"},
            "appt_and_visit_dates_match": {"ok": "TBD", "set_by": "unknown"},
            # csv header consistency
            "csv_sen55_all_same": {"ok": "TBD", "set_by": "unknown"},
            "csv_sen55_matches_nnn_from_sensor_dict": {
                "ok": "TBD",
                "set_by": "unknown",
            },
            "csv_fw_versions_all_same": {"ok": "TBD", "set_by": "unknown"},
            "csv_col_names_all_same": {"ok": "TBD", "set_by": "unknown"},
            # csv contents valid
            "csv_fname_to_first_timestamps_checked": {"ok": "TBD", "set_by": "unknown"},
            # unclear what the next one meant, remove for now
            # 'csv_readable_lines_added_to_data_list': {'ok': 'TBD', 'set_by': 'unknown'},
            # 'csv_data_values_in_range': {'ok': 'TBD', 'set_by': 'unknown'},
            # 'data_from_visit_demo_removed': {'ok': 'TBD', 'set_by': 'unknown'},
            # 'data_from_return_device_check_removed': {'ok': 'TBD', 'set_by': 'unknown'},
            # 'qa_complete_and_successful': {'ok': 'TBD', 'set_by': 'unknown'},
        },
        # final output fields will stay at the top level
        "participantID": "0000",  # start with default
        "output_file": None,  # str will be set if export (conversion) is a success
        "conversion_success": False,  # will be set by export() if errorCount == 0
        "conversion_issues": [],  # start with default
    }
    return s


def convert_env_sensor(
    input_path,
    output_folder,
    visit_file,
    filter_level=2,
    build_file=None,
    doc_header_file=None,
    extended_conv_dict=False,
):
    """Reads all files in input folder, checks and combines them and exports to the output_folder.
    Args:
        input_path (string): full path to folder containing input files
        output_folder (string): full path to output folder
        visit_file (string): Full path to a csv file with visit data
            if None, a single default entry is used to allow other audits to proceed
        filter_level (int): Controls the amount of data filtering.
            0 - filter out corrupted lines
            1 - reserved
            2 - max filter - removes data prior to visit_date and after return_date
        build_file (string): Full path to a csv file with sensor ID mapped to SEN55
            Default is to use the es_sensor_id.csv included with the ES code files
        doc_header_file (string): Full path to a self-documenting header template file
            Default is to use es_doc_header.txt included with the ES code files
    Returns:
        dict containing status, issues, and the full path to the output_file
    """
    # Default struct values enable QA to proceed where possible
    s = create_struct_s(input_path, output_folder, filter_level)

    # visit_dict = es_utils.build_visit_dict(visit_csv=visit_file)
    visit_dict = es_utils.build_visit_dict(visit_csv=visit_file)
    esID_dict = es_utils.build_es_dict(build_csv=build_file)

    # fetch and qa the pppp and nnn extracted from input folder name
    s = es_utils.pipeline_get_pppp_nnn_from_foldername(s)

    s = es_utils.pipeline_get_p_visit(s, visit_dict)
    s = es_utils.pipeline_qa_p_visit(s)
    # checked: nnn, es_data_ok, visit_date, return_date
    # add to s: p_visit [site, visit_date, return_date, appt_date,
    #                    esID (same as nnn),
    #                    location, es_data_ok]

    s = es_utils.pipeline_get_sen55_from_nnn(s, esID_dict)
    # checked: nnn in dict, else use 'NO_SEN55_IN_DICT'
    # add to s: SEN55, qa check for SEN55

    s = es_utils.pipeline_get_outfile_name(s)
    s = es_utils.pipeline_get_csv_list(s)
    s = es_utils.pipeline_qa_csv_fname_to_p_visit(s)  # removes files outside the window
    # add to s: outfile_posixpath, file_list

    # short files (ones we will not include) need to have been removed from the list prior
    # to calling the qa_csv_namedelta_to_timestamp1
    s = es_utils.pipeline_qa_csv_drop_short_files_from_list(
        s
    )  # drops all files < 4 lines

    s = es_utils.pipeline_qa_csv_namedelta_to_timestamp1(s)
    # checked: first timestamp in each file

    s = es_utils.pipeline_get_merged_file_contents(s)
    # add to s: column_dict, data_list, col_name_list,
    # err_dict_from_read_files, fw_version_list, sen55_list

    total_html_files = 0
    total_apple_files = 0
    total_public_files = 0

    file_errors = s["t"].get("err_dict_from_read_files", {})
    for fname, f_err in file_errors.items():
        if f_err.get('has_html'):
            total_html_files += 1
        if f_err.get('has_apple'):
            total_apple_files += 1
        if f_err.get('has_public'):
            total_public_files += 1

    s["html_count"] = total_html_files
    s["apple_count"] = total_apple_files
    s["public_count"] = total_public_files

    # check that the raw csv files all had the same FW, SEN55 and column names
    s = es_utils.pipeline_qa_hdr_list(
        s, "fw_version_list", "fw_version", "csv_fw_versions_all_same"
    )  # check FW versions in the raw csv files
    s = es_utils.pipeline_qa_hdr_list(
        s, "sen55_list", "sen55", "csv_sen55_all_same"
    )  # check SEN55 IDs
    s = es_utils.pipeline_qa_hdr_list(
        s, "col_name_list", "column_names", "csv_col_names_all_same"
    )

    s = es_utils.pipeline_qa_match_sen55(s)
    # checked: csv sen55 matches r.SEN55

    # s = es_utils.pipeline_qa_data(s)
    # checked: nothing at the moment; reminder for possible future implementation

    # filter the rows of data to remove initial visit, return device check
    # data will be in s['t']['data_list']
    s = es_utils.pipeline_filter_visit_dates(s)

    if (0):  # major issue .. many lines dropped; need to remove this or fix it another way
        # Check the values for the column "screen". Any lines with a value other than 0 or 1
        # should be flagged as corrupt and removed.
        if s["t"].get("data_list") and s["t"].get("col_name_list"):
            try:
                screen_idx = s["t"]["col_name_list"][0].index("screen")
                filtered_data = []
                corrupt_count = 0
                for line in s["t"]["data_list"]:
                    parts = line.split(",")
                    if len(parts) > screen_idx and parts[screen_idx].strip() in ("0", "1", "0.0", "1.0"):
                        filtered_data.append(line)
                    else:
                        corrupt_count += 1
                
                if corrupt_count > 0:
                    s["t"]["conversion_issues"].append(
                        f"WARNING: Removed {corrupt_count} corrupt data lines where 'screen' != 0 or 1"
                    )
                s["t"]["data_list"] = filtered_data
                s["r"]["num_rows"] = len(filtered_data)
            except ValueError:
                s["t"]["conversion_issues"].append("WARNING: 'screen' column not found for filtering")

    # QA for measured values is intentionally deferred until the range checks are implemented.
    # The data values are still treated as strings at this stage.

    s = pipeline_es_export(s, header_template_file=doc_header_file)

    s["participantID"] = s["r"]["pppp"]
    s["conversion_issues"] = s["t"]["conversion_issues"]

    s["nfiles_start"] = s["t"].get("num_orig_files", 0)
    s["nfiles_dropoutsidewindow"] = s.get("nfiles_dropoutsidewindow", 0)
    final_files = len(s["t"].get("file_list", []))
    s["nfiles_dropother"] = max(0, s["nfiles_start"] - s["nfiles_dropoutsidewindow"] - final_files)

    # cleanup
    if not extended_conv_dict:
        s["t"]["fw_version_list"] = []  # list of all FW versions found in the headers of raw csv files
        s["t"]["sen55_list"] = []  # list of all SEN55 IDs found in the headers of raw csv files
        s["t"]["column_dict"] = []  # is this still used
        s["t"]["data_list"] = []  # consumes a lot of memory - delete if not needed
        s.pop("t", None)  # completely remove 't'
        s.pop("qa", None)
        s.pop("r", None)

    return s
