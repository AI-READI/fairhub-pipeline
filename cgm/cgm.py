# import argparse
import re
import sys
import os
import pandas as pd
import json
import copy
import csv
import pytz
from datetime import datetime

import cgm.cgm_qc as QC


def convert_to_utc(df, column_name, timezone):
    # Map common timezone abbreviations to pytz timezone
    timezone_mappings = {
        "est": "America/New_York",  # Eastern Standard Time
        "cst": "America/Chicago",  # Central Standard Time
        "mst": "America/Denver",  # Mountain Standard Time
        "pst": "America/Los_Angeles",  # Pacific Standard Time
        "akst": "America/Anchorage",  # Alaska Standard Time
        "hast": "America/Adak",  # Hawaii-Aleutian Standard Time
        # Add more mappings as needed
    }

    # Use the mapping if available, otherwise assume it's a valid pytz timezone
    timezone = timezone_mappings.get(timezone.lower(), timezone)

    local_tz = pytz.timezone(timezone)

    def localize_and_convert(dt_str):
        try:
            # Parse the datetime string into a datetime object
            dt = datetime.strptime(
                dt_str, "%Y-%m-%dT%H:%M:%S"
            )  # Adjusted to match the ISO 8601 format

        except ValueError:
            # Handle the exception if datetime format is incorrect
            print(f"Error parsing datetime string: {dt_str}")
            return None

        # Localize and convert to UTC
        local_dt = local_tz.localize(dt, is_dst=True)
        return local_dt.astimezone(pytz.utc)

    df[column_name] = df[column_name].apply(localize_and_convert)


def convert(
    input_path,
    output_path,
    effective_time_frame,
    event_type,
    source_device_id,
    blood_glucose,
    transmitter_time,
    transmitter_id,
    uuid,
    timezone,
    optional=None,
):
    # ADD PARSER ARGUMENTS#
    # parser = argparse.ArgumentParser()
    # parser.add_argument(
    #     "input",
    #     help="The input dataset that will be converted. User input must be an existing .csv or .xlsx file that exists inside of the current directory",
    # )
    # parser.add_argument(
    #     "output",
    #     help="The name of the output file that will be created. Must have a .json suffix.",
    # )
    # parser.add_argument(
    #     "required",
    #     help="All of the required properties that need to be converted. Required arguments are 'effective_time_frame', 'event_type', 'source_device', 'blood_glucose', 'transmitter_time', 'transmitter_id', 'uuid', and 'timezone'. In this argument, each property must have an equals sign next to it and an integer on the other side of it that represents in what order the property will be placed in each data point in the data series. After each property there must be a comma which is what separates all of the properties. No integer can be repeated. For 'uuid', instead of an integer, the user can put in any valid input. For 'timezone', the user must put in one of the following (case-insensitive): 'pst' (for Pacific Standard Time), 'mst' (for Mountain Standard Time), 'cst' (for Central Standard Time), and 'est' (for Eastern Standard Time). An example of a valid argument string looks like this: 'effective_time_frame=1,event_type=2,source_device=3,blood_glucose=4,transmitter_time=5,transmitter_id=6,uuid=123e4567-e89b-12d3-a456-426655440000,timezone=cst'",
    # )
    # parser.add_argument(
    #     "-o",
    #     "--optional",
    #     help="All of the optional properties to add to each data point. In this argument, each property must have an equals sign next to it and an integer on the other side of it that represents in what order the property will be placed in each data point in the data series. After each poperty there must be a comma which is what separates all of the properties. No integer can be repeated (this takes in account the integers specified in the 'required' argument string). An example of a valid argument string would look like this: 'foo=7,bar=8",
    #     required=False,
    # )
    # args = parser.parse_args()

    # VERIFY THAT input_path AND output_path HAVE VALID NAMES AND EXIST IN EXISTING DIRECTORY#
    if not re.match("[\w\s\(\)_\-,\.\*\/]*\.xlsx", input_path) and not re.match(
        "[\w\s\(\)_\-,\.\*\/]*\.csv", input_path
    ):
        print("Input is either not a file or an invalid file type")
        sys.exit()

    if not re.match("[\w\s\(\)_\-,\.\*\/]*\.json", output_path):
        print("Output is either not a file or an invalid file type (not JSON format)")
        sys.exit()

    # check whether the specified input path exists or not
    inputExists = os.path.exists(input_path)
    if not inputExists:
        print("Input does not exist. Please input a valid file path")
        sys.exit()

    # check whether the specified output path exists and if it does not then create a directory
    outputExists = os.path.exists(output_path.rstrip(".json"))
    if not outputExists:
        print("Output does not exist. Creating directory to place output files")
        os.makedirs(output_path.rstrip(".json"))

    # VERIFY THAT input_path AND output_path ARE CORRECTLY FORMATTED#
    # places all properties and their values in args.required in to dictionary
    props_dict = {
        "effective_time_frame": effective_time_frame,
        "event_type": event_type,
        "source_device_id": source_device_id,
        "blood_glucose": blood_glucose,
        "transmitter_time": transmitter_time,
        "transmitter_id": transmitter_id,
        "uuid": uuid,
        "timezone": timezone,
    }
    # for item in props:
    #     item_sep = item.split("=")
    #     if item_sep[0] != "uuid" and item_sep[0] != "timezone":
    #         props_dict[item_sep[0]] = int(item_sep[1])
    #     else:
    #         props_dict[item_sep[0]] = item_sep[1]

    # place all properties and their values in args.optional in to a dictionary
    # opts_dict = {}
    # opts_dict = optional
    # if args.optional:
    #     opts = args.optional.split(sep=",")
    #     for item in opts:
    #         item_sep = item.split("=")
    #         if item_sep[0] != "creation_date_time":
    #             opts_dict[item_sep[0]] = int(item_sep[1])
    #         else:
    #             opts_dict[item_sep[0]] = item_sep[1]

    if (
        ("effective_time_frame" not in props_dict)
        or ("event_type" not in props_dict)
        or ("source_device_id" not in props_dict)
        or ("blood_glucose" not in props_dict)
        or ("transmitter_time" not in props_dict)
        or ("transmitter_id" not in props_dict)
        or ("uuid" not in props_dict)
        or ("timezone" not in props_dict)
    ):
        print(
            "All required values not present in third command line argument. Required arguments are 'effective_time_frame', 'event_type', 'source_device', 'blood_glucose', 'transmitter_time', 'transmitter_id', 'uuid' and 'timezone'"
        )
        sys.exit()

    if (
        props_dict["timezone"] != "pst"
        and props_dict["timezone"] != "mst"
        and props_dict["timezone"] != "cst"
        and props_dict["timezone"] != "est"
    ):
        print("'timezone' must be equal to 'pst', 'mst', 'cst', or 'est'")
        sys.exit()

    rev_dict = {}
    for key, value in props_dict.items():
        rev_dict.setdefault(value, set()).add(key)
    duplicates = [key for key, values in rev_dict.items() if len(values) > 1]
    if duplicates:
        print("There can be no duplicate integer values assigned to properties")
        sys.exit()

    pd.set_option("display.max_columns", None)
    df = None

    if re.match("[\w\s\(\)_\-,\.\*\/]*\.xlsx", input_path):
        df = pd.read_excel(input_path, engine="openpyxl")
    elif re.match("[\w\s\(\)_\-,\.\*\/]*\.csv", input_path):
        df = pd.read_csv(input_path)
        with open(input_path, encoding="utf-8") as csv_file_handler:
            csv_reader = csv.DictReader(csv_file_handler)
    else:
        sys.exit()

    # ALTER COLUMN HEADERS AND DROP UNNECESSARY COLUMNS IN DATAFRAME FROM .XLSX FILE#
    # if re.match("[\w\s\(\)_\-,\.\*]*\.xlsx", input_path):
    # 	df = pd.read_excel(input_path, engine='openpyxl')
    patient = df["Patient Info"][2]

    # Dynamically detect and remove metadata rows
    # Find the first row where "Timestamp (YYYY-MM-DDThh:mm:ss)" is not empty
    first_data_index = df[df["Timestamp (YYYY-MM-DDThh:mm:ss)"].notna()].index[0]
    # Keep only the rows from the first data index onward
    df = df.loc[first_data_index:].reset_index(drop=True)

    # drop columns that have no data
    df.drop(
        [
            "Index",
            "Event Subtype",
            "Patient Info",
            "Device Info",
            "Insulin Value (u)",
            "Carb Value (grams)",
            "Duration (hh:mm:ss)",
            "Glucose Rate of Change (mg/dL/min)",
        ],
        axis=1,
        inplace=True,
    )

    # change original excel columns to Open mHealth/IEEE equivalent
    df.rename(
        columns={
            "Timestamp (YYYY-MM-DDThh:mm:ss)": "effective_time_frame",
            "Event Type": "event_type",
            "Source Device ID": "source_device_id",
            "Glucose Value (mg/dL)": "blood_glucose",
            "Transmitter Time (Long Integer)": "transmitter_time",
            "Transmitter ID": "transmitter_id",
        },
        inplace=True,
    )

    # <Convert>##
    print("WARNING: You've set the timezone to:", props_dict["timezone"])
    convert_to_utc(df, "effective_time_frame", props_dict["timezone"])

    # CREATION_DATE_TIME INPUT VERIFICATION AND INITIALIZATION#
    # Get the current date and time
    current_datetime = datetime.utcnow()

    # Format the current date and time in the desired format
    formatted_datetime = current_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")

    # Assign the formatted datetime string to the variable
    datetime_str = formatted_datetime

    # JSON STRING CREATED FROM DATAFRAME VALUES#
    df_data = (
        '{"header":{"uuid":'
        + f'"{props_dict["uuid"]}"'
        + f',"creation_date_time":"{datetime_str}",'
        + f'"patient_id":"{patient}"'
        + ',"schema_id":{"namespace":"omh","name":"blood-glucose","version":3.0},"modality":"sensed","acquistion_rate":{"number_of_times":1, "time_window":{"value":5, "unit":"min"}},"external_datasheets":{"datasheet_type":"source_device","datasheet_reference":"iri-of-cgm-device"}'
        + f',"timezone": "{props_dict["timezone"]}"'
        + '}, "body": {"cgm":'
        + df.to_json(orient="records", date_format="iso", date_unit="s")
        + "}}"
    )

    # JSON STRING TO DICT, PROPERTY NESTING, AND JSON OUTPUT FILE#
    parsed = json.loads(df_data)
    # create a deep copy because we will convert this to csv later
    parsed_copy = copy.deepcopy(parsed)
    # convert data point properties to nested properties if needed
    # parsed_copy values not assigned nested properties because we will be converting it to csv later which does not have nested properties
    for data in range(len(parsed["body"]["cgm"])):
        # copies exist so value can be embedded in nested property
        # copies exist so value can be embedded in nested property
        copy_effective_time_frame = parsed["body"]["cgm"][data]["effective_time_frame"]
        parsed["body"]["cgm"][data]["effective_time_frame"] = {
            "time_interval": {
                "start_date_time": copy_effective_time_frame,
                "end_date_time": copy_effective_time_frame,
            }
        }
        copy_blood_glucose = parsed["body"]["cgm"][data]["blood_glucose"]
        if copy_blood_glucose != "Low" and copy_blood_glucose != "High":
            parsed["body"]["cgm"][data]["blood_glucose"] = {
                "unit": "mg/dL",
                "value": int(copy_blood_glucose),
            }
        else:
            parsed["body"]["cgm"][data]["blood_glucose"] = {
                "unit": "mg/dL",
                "value": copy_blood_glucose,
            }
        copy_transmitter_time = parsed["body"]["cgm"][data]["transmitter_time"]
        parsed["body"]["cgm"][data]["transmitter_time"] = {
            "unit": "long integer",
            "value": int(copy_transmitter_time),
        }

    # create output JSON file
    args_output_list = output_path.split("/")
    json_file = args_output_list[-1]
    with open(
        output_path.rstrip(".json") + "/" + json_file, "w", encoding="utf-8"
    ) as json_file_handler:
        json_file_handler.write(json.dumps(parsed, indent=4))

    # DATAFRAME TO CSV AND DICT TO CSV CONVERSION, AND COMPARISON#
    # convert dataframe version of .xlsx and .csv file to csv and create output file
    df.to_csv(
        output_path.rstrip(".json") + "/" + json_file.rstrip(".json") + "_df_to_csv.csv"
    )
    # convert json output file to csv and create output file
    # change all float values to int
    for data in range(len(parsed_copy["body"]["cgm"])):
        parsed_copy["body"]["cgm"][data]["transmitter_time"] = int(
            parsed_copy["body"]["cgm"][data]["transmitter_time"]
        )
        if (
            parsed_copy["body"]["cgm"][data]["blood_glucose"] != "High"
            and parsed_copy["body"]["cgm"][data]["blood_glucose"] != "Low"
        ):
            parsed_copy["body"]["cgm"][data]["blood_glucose"] = int(
                parsed_copy["body"]["cgm"][data]["blood_glucose"]
            )
    fieldnames = parsed_copy["body"]["cgm"][0].keys()

    with open(
        output_path.rstrip(".json")
        + "/"
        + json_file.rstrip(".json")
        + "_standard_json_to_csv.csv",
        "w",
    ) as json_convert:
        writer = csv.DictWriter(json_convert, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(parsed_copy["body"]["cgm"])

    # PERFORM QUALITY CONTROL TESTS#
    with open(
        output_path.rstrip(".json") + "/" + "QC_results.txt", "w", encoding="utf-8"
    ) as f:
        f.write("##QC ON THE INPUT DATAFRAME VALUES##")
        f.write("\n")
        # print out min, max, and hist values of dataframe
        f.write("effective_time_frame START_TIME: ")
        f.write(QC.df_min(df["effective_time_frame"]))
        f.write("\n")
        f.write("effective_time_frame END_TIME: ")
        f.write(QC.df_max(df["effective_time_frame"]))
        f.write("\n")
        df.drop(
            [
                "effective_time_frame",
                "event_type",
                "source_device_id",
                "transmitter_id",
            ],
            axis=1,
            inplace=True,
        )
        for column in df.columns:
            f.write(column)
            f.write(" MIN: ")
            f.write(QC.df_min(df[column]))
            f.write("\n")
            f.write(column)
            f.write(" MAX: ")
            f.write(QC.df_max(df[column]))
            f.write("\n")
            QC.df_hist(
                df[column],
                output_path.rstrip(".json")
                + "/"
                + json_file.rstrip(".json")
                + "_input_"
                + column,
            )

        # place all values from relevant JSON dict values in to lists
        effective_time_frame_list = []
        blood_glucose_list = []
        transmitter_time_list = []

        for data in range(len(parsed["body"]["cgm"])):
            effective_time_frame_list.append(
                parsed["body"]["cgm"][data]["effective_time_frame"]["time_interval"][
                    "start_date_time"
                ]
            )
            blood_glucose_list.append(
                parsed["body"]["cgm"][data]["blood_glucose"]["value"]
            )
            transmitter_time_list.append(
                parsed["body"]["cgm"][data]["transmitter_time"]["value"]
            )

        f.write("\n\n")
        f.write("##QC ON THE STANDARD JSON DICT VALUES##\n")
        # print out min, max, and hist values of relevant json dict keys
        f.write("effective_time_frame START_TIME: ")
        f.write(QC.list_min(effective_time_frame_list))
        f.write("\n")
        f.write("effective_time_frame END_TIME: ")
        f.write(QC.list_max(effective_time_frame_list))
        f.write("\n")
        f.write("blood_glucose MIN: ")
        f.write(QC.list_min(blood_glucose_list))
        f.write("\n")
        f.write("blood_glucose MAX: ")
        f.write(QC.list_max(blood_glucose_list))
        f.write("\n")
        QC.list_hist(
            blood_glucose_list,
            "Blood Glucose",
            output_path.rstrip(".json") + "/" + json_file.rstrip(".json"),
        )
        f.write("transmitter_time MIN: ")
        f.write(QC.list_min(transmitter_time_list))
        f.write("\n")
        f.write("transmitter_time MAX: ")
        f.write(QC.list_max(transmitter_time_list))
        QC.list_hist(
            transmitter_time_list,
            "Transmitter Time",
            output_path.rstrip(".json") + "/" + json_file.rstrip(".json"),
        )
