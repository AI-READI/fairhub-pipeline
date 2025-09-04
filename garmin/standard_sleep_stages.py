import pandas as pd
from datetime import datetime
import json
import glob
import os
from pathlib import Path
from traceback import format_exc


def merge_json_files(file_paths, outdir, ptname):
    # Initialize variables to store the combåined header and bodies
    combined_header = None
    combined_body_sleep = []

    for file_path in file_paths:
        # Load the current JSON file
        with open(file_path, "r") as file:
            data = json.load(file)

            # If the combined_header is not set, take the header from the first file
            if combined_header is None:
                combined_header = data["header"]

            # Extend the combined_body_sleep list with the sleep list from the current file
            combined_body_sleep.extend(data["body"]["sleep"])

    # Sort the combined_body_sleep list by date_time
    combined_body_sleep.sort(
        key=lambda x: x["sleep_stage_time_frame"]["time_interval"]["start_date_time"]
    )

    # Create the combined dictionary
    combined_data = {"header": combined_header, "body": {"sleep": combined_body_sleep}}

    # Write the combined dictionary to a new JSON file
    with open(outdir + "/" + ptname + "_sleep" + ".json", "w") as combined_file:
        json.dump(combined_data, combined_file, indent=4)


def standardize_sleep_stages(
    root_dir, patient_id, output_folder, final_output, timezone="pst"
):
    pt = patient_id

    pt_hr_count = 0

    pt_heartrate_files = []

    try:
        if os.path.isdir(root_dir):
            for entry in os.listdir(root_dir):
                hr_file = root_dir + "/" + entry + "/sleep_data.csv"

                for filename in glob.glob(hr_file):
                    pt_heartrate_files.append(filename)

        # monitor_files = root_dir + pt + "/Garmin/Sleep/"
        # for entry in os.listdir(monitor_files):
        #     full_path = os.path.join(monitor_files, entry)
        #     if os.path.isdir(full_path):
        #         hr_file = (
        #             root_dir + pt + "/Garmin/Sleep/" + entry + "/sleep_data.csv"
        #         )
        #         for filename in glob.glob(hr_file):
        #             pt_heartrate_files.append(filename)

        # Print out the list of found CSV files
        for file_path in pt_heartrate_files:

            # Get patient ID
            # patient_ID = file_path.split(root_dir)[1].split("/")[0]
            patient_ID = patient_id

            # Get HR file name
            hr_file_name = file_path.split("/")[-2]
            # hr_file_name = file_path.split("Sleep/")[1].replace("/sleep_data.csv", "")

            # Load the CSV file
            sleep_df = pd.read_csv(file_path)

            if not sleep_df.empty:

                # Prepare the JSON structure again
                json_output = {
                    "header": {
                        "uuid": f"AIREADI-{patient_ID}",
                        "creation_date_time": datetime.now().strftime(
                            "%Y-%m-%dT%H:%M:%SZ"
                        ),
                        "user_id": f"AIREADI-{patient_ID}",
                        "schema_id": {
                            "namespace": "omh",
                            "name": "sleep-stages",
                            "version": 2.0,
                        },
                        "timezone": timezone,
                    },
                    "body": {"sleep": []},
                }

                # Iterate through the DataFrame to calculate durations of each stage
                for i in range(len(sleep_df) - 1):
                    stage = sleep_df["sleep_type"].iloc[i]
                    start_time = pd.to_datetime(sleep_df["adjusted_timestamp"].iloc[i])
                    end_time = pd.to_datetime(
                        sleep_df["adjusted_timestamp"].iloc[i + 1]
                    )

                    sleep_stage_entry = {
                        "sleep_stage_state": stage,
                        "sleep_stage_time_frame": {
                            "time_interval": {
                                "start_date_time": start_time.strftime(
                                    "%Y-%m-%dT%H:%M:%SZ"
                                ),
                                "end_date_time": end_time.strftime(
                                    "%Y-%m-%dT%H:%M:%SZ"
                                ),
                            }
                        },
                    }
                    json_output["body"]["sleep"].append(sleep_stage_entry)
                    pt_hr_count = pt_hr_count + 1

                # Sort the sleep entries based on start_date_time as datetime objects
                json_output["body"]["sleep"].sort(
                    key=lambda x: datetime.strptime(
                        x["sleep_stage_time_frame"]["time_interval"]["start_date_time"],
                        "%Y-%m-%dT%H:%M:%SZ",
                    )
                )

                formatted_json = json.dumps(json_output, indent=4, sort_keys=False)

                # To save the formatted JSON to a file
                # output_dir = Path(
                #     "respiratory_rate_jsons/" + patient_ID + "_respiratory_rate"
                # )

                output_dir = Path(output_folder)
                output_dir.mkdir(parents=True, exist_ok=True)

                output_filename = patient_ID + "_" + hr_file_name + ".json"
                output_file_path = output_dir / output_filename

                with open(output_file_path, "w") as f:
                    f.write(formatted_json)

        pt_directory = output_folder
        pt_directory_path = Path(pt_directory)

        out_directory = os.path.join(final_output, pt)
        out_directory_path = Path(out_directory)
        out_directory_path.mkdir(parents=True, exist_ok=True)

        file_paths = list(pt_directory_path.glob("*.json"))

        merge_json_files(file_paths, out_directory, pt)

    except Exception:
        print(format_exc())
