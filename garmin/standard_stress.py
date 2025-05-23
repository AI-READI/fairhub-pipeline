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
    combined_body_stress = []

    for file_path in file_paths:
        # Load the current JSON file
        with open(file_path, "r") as file:
            data = json.load(file)

            # If the combined_header is not set, take the header from the first file
            if combined_header is None:
                combined_header = data["header"]

            # Extend the combined_body_stress list with the stress list from the current file
            combined_body_stress.extend(data["body"]["stress"])

    # Sort the combined_body_stress list by date_time
    combined_body_stress.sort(key=lambda x: x["effective_time_frame"]["date_time"])

    # Create the combined dictionary
    combined_data = {
        "header": combined_header,
        "body": {"stress": combined_body_stress},
    }

    # Write the combined dictionary to a new JSON file
    with open(outdir + "/" + ptname + "_stress" + ".json", "w") as combined_file:
        json.dump(combined_data, combined_file, indent=4)


def standardize_stress(root_dir, patient_id, output_folder, final_output):
    pt = patient_id

    pt_hr_count = 0

    pt_heartrate_files = []

    try:
        if os.path.isdir(root_dir):
            for entry in os.listdir(root_dir):
                hr_file = root_dir + "/" + entry + "/stress_level_data*.csv"

                for filename in glob.glob(hr_file):
                    pt_heartrate_files.append(filename)

        # Print out the list of found CSV files
        for file_path in pt_heartrate_files:

            # Get patient ID
            # patient_ID = file_path.split(root_dir)[1].split("/")[0]
            patient_ID = patient_id

            # Get HR file name
            hr_file_name = file_path.split("stress_level_data")[1].replace(".csv", "")

            # Load the CSV file
            stress_df = pd.read_csv(file_path)

            # Prepare the JSON structure again
            json_output = {
                "header": {
                    "uuid": f"AIREADI-{patient_ID}",
                    "creation_date_time": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "user_id": f"AIREADI-{patient_ID}",
                    "schema_id": {"namespace": "", "name": "", "version": ""},
                },
                "body": {"stress": []},
            }

            for index, row in stress_df.iterrows():
                original_timestamp = pd.to_datetime(row["datetime"])
                formatted_timestamp = original_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")

                stress_entry = {
                    "stress": {
                        "value": row["stress_value (per minute)"],
                        "unit": "stress level",
                    },
                    "effective_time_frame": {"date_time": formatted_timestamp},
                }
                pt_hr_count = pt_hr_count + 1
                json_output["body"]["stress"].append(stress_entry)

            formatted_json = json.dumps(json_output, indent=4, sort_keys=False)

            # To save the formatted JSON to a file
            output_dir = Path(output_folder)
            output_dir.mkdir(parents=True, exist_ok=True)
            output_filename = patient_ID + "_" + hr_file_name + ".json"
            output_file_path = os.path.join(output_dir, output_filename)

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
