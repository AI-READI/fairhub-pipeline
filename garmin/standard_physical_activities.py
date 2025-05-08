import pandas as pd
from datetime import datetime
import json
import glob
import os
from pathlib import Path
from traceback import format_exc


def standardize_physical_activities(root_dir, patient_id, output_folder, final_output):
    pt = patient_id

    pt_hr_count = 0

    pt_heartrate_files = []

    try:
        if os.path.isdir(root_dir):
            for entry in os.listdir(root_dir):
                hr_file = root_dir + "/" + entry + "/active_calories_data*.csv"

                for filename in glob.glob(hr_file):
                    pt_heartrate_files.append(filename)

                hr_file2 = root_dir + "/" + entry + "/activity_type_data*.csv"

                for filename in glob.glob(hr_file2):
                    pt_heartrate_files.append(filename)

        # Merge activity files to a csv and sort for different activities
        dataframes = [pd.read_csv(file) for file in pt_heartrate_files]
        merged_df = pd.concat(dataframes, ignore_index=True)
        # Convert 'datetime' to datetime data type
        merged_df["datetime"] = pd.to_datetime(merged_df["datetime"])
        # Sort based on 'datetime'
        sorted_df = merged_df.sort_values(by="datetime")

        # Optionally, save to a new file
        merged_dir_path = os.path.join(output_folder, pt + "_physical_activities")
        os.makedirs(merged_dir_path, exist_ok=True)

        sorted_df.to_csv(
            os.path.join(merged_dir_path, "merged_sorted.csv"),
            index=False,
        )

        # Read the CSV file
        file_path = os.path.join(
            output_folder, pt + "_physical_activities", "merged_sorted.csv"
        )

        df = pd.read_csv(file_path)

        patient_ID = patient_id

        json_data = {
            "header": {
                "uuid": f"AIREADI-{patient_ID}",
                "creation_date_time": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                "user_id": f"AIREADI-{patient_ID}",
                "schema_id": {"namespace": "", "name": "", "version": ""},
            },
            "body": {"activity": []},
        }

        # Iterate over the dataframe to fill the "activity" list
        for index, row in df.iterrows():
            # For the first record, there's no previous record, so we use its own datetime
            start_date_time = (
                df.loc[index - 1, "datetime"] if index > 0 else row["datetime"]
            )

            # Set value to 0 if activity_type is "sedentary", else fetch from the "intensity" column
            if row["activity_type"] == "sedentary":
                value = 0
                act_type = row["activity_type"]
            elif row["activity_type"] in ["9", 9]:
                value = ""
                act_type = ""
            else:
                value = row["current_activity_type_intensity"]
                act_type = row["activity_type"]

            # convert to UCT
            start_date_time_object = datetime.strptime(
                start_date_time, "%Y-%m-%d %H:%M:%S"
            )
            start_date_time_utc_str = start_date_time_object.strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )

            end_date_time_object = datetime.strptime(
                row["datetime"], "%Y-%m-%d %H:%M:%S"
            )
            end_date_time_utc_str = end_date_time_object.strftime("%Y-%m-%dT%H:%M:%SZ")

            activity_record = {
                "activity_name": act_type,
                "base_movement_quantity": {"value": value, "unit": "steps"},
                "effective_time_frame": {
                    "time_interval": {
                        "start_date_time": start_date_time_utc_str,
                        "end_date_time": end_date_time_utc_str,
                    }
                },
            }
            pt_hr_count = pt_hr_count + 1

            json_data["body"]["activity"].append(activity_record)

        # Sort the sleep entries based on start_date_time as datetime objects
        json_data["body"]["activity"].sort(
            key=lambda x: datetime.strptime(
                x["effective_time_frame"]["time_interval"]["start_date_time"],
                "%Y-%m-%dT%H:%M:%SZ",
            )
        )

        formatted_json = json.dumps(json_data, indent=4, sort_keys=False)

        out_directory = os.path.join(final_output, pt)
        out_directory_path = Path(out_directory)
        out_directory_path.mkdir(parents=True, exist_ok=True)

        with open(
            os.path.join(final_output, pt, pt + "_activity.json"),
            "w",
        ) as combined_file:
            combined_file.write(formatted_json)

        # To save the formatted JSON to a file
        # out_directory = Path(
        #     "physical_activity/garmin_vivosmart5/" + pt.replace("FitnessTracker-", "")
        # )
        # out_directory.mkdir(parents=True, exist_ok=True)
        # with open(
        #     "physical_activity/garmin_vivosmart5/"
        #     + pt.replace("FitnessTracker-", "")
        #     + "/"
        #     + pt.replace("FitnessTracker-", "")
        #     + "_activity"
        #     + ".json",
        #     "w",
        # ) as combined_file:
        #     combined_file.write(formatted_json)

    except Exception:
        print(format_exc())
