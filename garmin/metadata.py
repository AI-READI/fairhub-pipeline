import os
import json
import csv
from datetime import datetime
import math
from collections import defaultdict


class GarminManifest:
    def __init__(self, processed_data_output_folder):
        self.processed_data_output_folder = processed_data_output_folder
        self.participants_data = defaultdict(dict)
        self.redcap_data = (
            {}
        )  # To store the wrist_worn_on and dominant_hand data from REDCap.tsv

    def read_redcap_file(self, file_path):
        """
        Reads the REDCap.tsv file and stores wrist_worn_on and dominant_hand for each participant.
        """
        # Mapping of numerical codes to textual descriptions
        value_mapping = {
            "1": "Right",
            "2": "Left",
            "3": "Not provided",
            "4": "Neither (ambidextrous)",
        }

        with open(file_path, mode="r") as file:
            reader = csv.DictReader(file)

            for row in reader:
                participant_id = row["studyid"]
                dominant_hand_code = row["dvamwendhand"]
                wrist_worn_on_code = row["dvamwenhand"]

                # Map numerical codes to textual descriptions
                dominant_hand = value_mapping.get(dominant_hand_code, "None")
                wrist_worn_on = value_mapping.get(wrist_worn_on_code, "None")

                self.redcap_data[participant_id] = {
                    "wrist_worn_on": wrist_worn_on,
                    "dominant_hand": dominant_hand,
                }

    def add_to_participant_data(
        self, participant_id, key_prefix, file_path, record_count, average_value
    ):
        """
        Adds data to the participant's dictionary with average_value rounded to 2 decimal places.
        """
        self.participants_data[participant_id][f"{key_prefix}_filepath"] = file_path
        self.participants_data[participant_id][
            f"{key_prefix}_record_count"
        ] = record_count
        self.participants_data[participant_id][
            f"average_{key_prefix}"
        ] = f"{average_value:.2f}"

    def calculate_sampling_extent(
        self,
        directory,
        key,
        subkey,
        value_key,
        date_key,
        key_prefix,
        is_nan_check=False,
    ):
        date_format = "%Y-%m-%dT%H:%M:%SZ"

        for root, dirs, files in sorted(os.walk(directory)):
            dirs.sort()
            for file in sorted(files):
                if file.endswith(".json"):
                    file_path = os.path.join(root, file)
                    try:
                        with open(file_path, "r") as json_file:
                            data = json.load(json_file)
                            if key in data and subkey in data[key]:
                                records = data[key][subkey]
                                if not records:
                                    continue
                                records = sorted(
                                    records,
                                    key=lambda x: datetime.strptime(
                                        x[date_key]["date_time"], date_format
                                    ),
                                )
                                total_value, num_records = 0, 0
                                for record in records:
                                    if (
                                        value_key in record
                                        and "value" in record[value_key]
                                    ):
                                        record_value = record[value_key]["value"]
                                        if is_nan_check and (
                                            record_value is None
                                            or math.isnan(record_value)
                                        ):
                                            continue
                                        total_value += record_value
                                        num_records += 1
                                average_value = (
                                    total_value / num_records if num_records > 0 else 0
                                )
                                participant_id = os.path.basename(root)

                                output_file_path = f"/wearable_activity_monitor/{key_prefix}/garmin_vivosmart5/{participant_id}/{file}"

                                self.add_to_participant_data(
                                    participant_id,
                                    key_prefix,
                                    output_file_path,
                                    num_records,
                                    average_value,
                                )
                    except Exception as e:
                        print(f"Error processing file {file_path}: {e}")

    def process_heart_rate(self, directory):
        self.calculate_sampling_extent(
            directory,
            "body",
            "heart_rate",
            "heart_rate",
            "effective_time_frame",
            "heart_rate",
        )

    def process_calories(self, directory):
        """
        Processes calorie-related JSON files, calculates total and average calories burned.
        """
        for root, dirs, files in sorted(os.walk(directory)):
            dirs.sort()
            for file in sorted(files):
                if file.endswith(".json"):
                    file_path = os.path.join(root, file)
                    participant_id = os.path.basename(root)
                    try:
                        with open(file_path, "r") as json_file:
                            data = json.load(json_file)
                            if "body" in data and "activity" in data["body"]:
                                calorie_data = data["body"]["activity"]
                                total_calories = 0
                                valid_records = 0
                                for record in calorie_data:
                                    if (
                                        "calories_value" in record
                                        and "value" in record["calories_value"]
                                    ):
                                        calorie_value = record["calories_value"][
                                            "value"
                                        ]
                                        if isinstance(
                                            calorie_value, (int, float)
                                        ):  # Ensure it's a valid number
                                            total_calories += calorie_value
                                            valid_records += 1
                                average_calories = (
                                    total_calories / valid_records
                                    if valid_records > 0
                                    else 0
                                )

                                output_file_path = f"/wearable_activity_monitor/physical_activity_calorie/garmin_vivosmart5/{participant_id}/{file}"

                                self.add_to_participant_data(
                                    participant_id,
                                    "active_calories",
                                    output_file_path,
                                    valid_records,
                                    average_calories,
                                )
                    except Exception as e:
                        print(f"Error processing file {file_path}: {e}")

    def process_sleep(self, directory):
        """
        Processes sleep JSON files, calculates total and average sleep duration.
        """
        # date_format = "%Y-%m-%dT%H:%M:%SZ"
        for root, dirs, files in sorted(os.walk(directory)):
            dirs.sort()
            for file in sorted(files):
                if file.endswith(".json"):
                    file_path = os.path.join(root, file)
                    participant_id = os.path.basename(root)
                    try:
                        with open(file_path, "r") as json_file:
                            data = json.load(json_file)
                            if "body" in data and "sleep" in data["body"]:
                                sleep_data = data["body"]["sleep"]
                                sleep_sorted = sorted(
                                    sleep_data,
                                    key=lambda x: x["sleep_stage_time_frame"][
                                        "time_interval"
                                    ]["start_date_time"],
                                )
                                total_sleep_duration = 0
                                for item in sleep_sorted:
                                    start_time = datetime.strptime(
                                        item["sleep_stage_time_frame"]["time_interval"][
                                            "start_date_time"
                                        ],
                                        "%Y-%m-%dT%H:%M:%SZ",
                                    )
                                    end_time = datetime.strptime(
                                        item["sleep_stage_time_frame"]["time_interval"][
                                            "end_date_time"
                                        ],
                                        "%Y-%m-%dT%H:%M:%SZ",
                                    )
                                    duration = (
                                        end_time - start_time
                                    ).total_seconds() / 3600
                                    total_sleep_duration += duration
                                average_sleep_duration = (
                                    total_sleep_duration / len(sleep_sorted)
                                    if sleep_sorted
                                    else 0
                                )

                                output_file_path = f"/wearable_activity_monitor/sleep/garmin_vivosmart5/{participant_id}/{file}"

                                self.add_to_participant_data(
                                    participant_id,
                                    "sleep",
                                    output_file_path,
                                    len(sleep_sorted),
                                    average_sleep_duration,
                                )
                    except Exception as e:
                        print(f"Error processing file {file_path}: {e}")

    def process_activity(self, directory):
        """
        Processes activity-related JSON files, calculates total steps and average steps per unique day.
        """
        date_format = "%Y-%m-%dT%H:%M:%SZ"
        for root, dirs, files in sorted(os.walk(directory)):
            dirs.sort()
            for file in sorted(files):
                if file.endswith(".json"):
                    file_path = os.path.join(root, file)
                    participant_id = os.path.basename(root)
                    try:
                        with open(file_path, "r") as json_file:
                            data = json.load(json_file)
                            if "body" in data and "activity" in data["body"]:
                                steps_data = data["body"]["activity"]
                                steps_sorted = sorted(
                                    steps_data,
                                    key=lambda x: x["effective_time_frame"][
                                        "time_interval"
                                    ]["start_date_time"],
                                )

                                total_steps = 0
                                unique_days = set()  # Set to track unique days

                                for record in steps_sorted:
                                    step_value = record["base_movement_quantity"][
                                        "value"
                                    ]
                                    # Parse the date of the record
                                    date_time_str = record["effective_time_frame"][
                                        "time_interval"
                                    ]["start_date_time"]
                                    date_obj = datetime.strptime(
                                        date_time_str, date_format
                                    ).date()  # Extract only the date

                                    # Add the date to the set of unique days
                                    unique_days.add(date_obj)

                                    # Check if the value is valid (integer or a string that can be converted to an integer)
                                    if (
                                        isinstance(step_value, str)
                                        and step_value.isdigit()
                                    ):
                                        step_value = int(step_value)
                                    elif not isinstance(step_value, int):
                                        step_value = 0  # Treat invalid values as 0

                                    total_steps += step_value

                                # Calculate the number of unique days
                                num_unique_days = len(unique_days)

                                # Calculate the average steps per unique day
                                average_steps_per_day = (
                                    total_steps / num_unique_days
                                    if num_unique_days > 0
                                    else 0
                                )

                                # Store the average and other details in the participant's data dictionary
                                output_file_path = f"/wearable_activity_monitor/physical_activity/garmin_vivosmart5/{participant_id}/{file}"

                                self.participants_data[participant_id][
                                    "physical_activity_filepath"
                                ] = output_file_path
                                self.participants_data[participant_id][
                                    "physical_activity_num_days"
                                ] = num_unique_days
                                self.participants_data[participant_id][
                                    "average_physical_activity"
                                ] = f"{average_steps_per_day:.2f}"

                    except Exception as e:
                        print(f"Error processing file {file_path}: {e}")

    def process_oxygen_saturation(self, directory):
        self.calculate_sampling_extent(
            directory,
            "body",
            "breathing",
            "oxygen_saturation",
            "effective_time_frame",
            "oxygen_saturation",
            True,
        )

    def process_respiratory_rate(self, directory):
        self.calculate_sampling_extent(
            directory,
            "body",
            "breathing",
            "respiratory_rate",
            "effective_time_frame",
            "respiratory_rate",
            True,
        )

    def process_stress(self, directory):
        self.calculate_sampling_extent(
            directory, "body", "stress", "stress", "effective_time_frame", "stress"
        )

    def calculate_sensor_sampling_duration(self, heart_rate_directory):
        """
        Calculates the number of unique days for heart rate data and adds this information to the dictionary.
        """
        date_format = "%Y-%m-%dT%H:%M:%SZ"
        for root, dirs, files in sorted(os.walk(heart_rate_directory)):
            dirs.sort()
            for file in sorted(files):
                if file.endswith(".json"):
                    file_path = os.path.join(root, file)
                    participant_id = os.path.basename(root)
                    try:
                        with open(file_path, "r") as json_file:
                            data = json.load(json_file)
                            if "body" in data and "heart_rate" in data["body"]:
                                records = data["body"]["heart_rate"]
                                unique_days = set()
                                for record in records:
                                    date_time = record["effective_time_frame"][
                                        "date_time"
                                    ]
                                    date_obj = datetime.strptime(
                                        date_time, date_format
                                    ).date()  # Add only the date part
                                    unique_days.add(date_obj)
                                self.participants_data[participant_id][
                                    "sensor_sampling_duration_days"
                                ] = len(unique_days)

                    except Exception as e:
                        print(
                            f"Error calculating sensor sampling duration for {file_path}: {e}"
                        )

    def write_tsv(self, output_file):
        data = self.participants_data

        # Define the required column order
        headers = [
            "participant_id",
            "wrist_worn_on",
            "dominant_hand",
            "heart_rate_filepath",
            "heart_rate_record_count",
            "average_heart_rate",
            "oxygen_saturation_filepath",
            "oxygen_saturation_record_count",
            "average_oxygen_saturation",
            "stress_filepath",
            "stress_record_count",
            "average_stress",
            "sleep_filepath",
            "sleep_record_count",
            "average_sleep",
            "respiratory_rate_filepath",
            "respiratory_rate_record_count",
            "average_respiratory_rate",
            "physical_activity_filepath",
            "physical_activity_num_days",
            "average_physical_activity",
            "active_calories_filepath",
            "active_calories_record_count",
            "average_active_calories",
            "sensor_sampling_duration_days",
            "manufacturer",
            "manufacturer_model_name",
        ]

        with open(output_file, mode="w", newline="") as file:
            writer = csv.writer(file, delimiter="\t")
            # Write header
            writer.writerow(headers)
            # Sort the participants by ID and write their data
            for participant_id in sorted(data.keys()):
                row = [
                    participant_id,
                    self.redcap_data.get(participant_id, {}).get(
                        "wrist_worn_on", "None"
                    ),
                    self.redcap_data.get(participant_id, {}).get(
                        "dominant_hand", "None"
                    ),
                    data[participant_id].get("heart_rate_filepath", "None"),
                    data[participant_id].get("heart_rate_record_count", "None"),
                    data[participant_id].get("average_heart_rate", "None"),
                    data[participant_id].get("oxygen_saturation_filepath", "None"),
                    data[participant_id].get("oxygen_saturation_record_count", "None"),
                    data[participant_id].get("average_oxygen_saturation", "None"),
                    data[participant_id].get("stress_filepath", "None"),
                    data[participant_id].get("stress_record_count", "None"),
                    data[participant_id].get("average_stress", "None"),
                    data[participant_id].get("sleep_filepath", "None"),
                    data[participant_id].get("sleep_record_count", "None"),
                    data[participant_id].get("average_sleep", "None"),
                    data[participant_id].get("respiratory_rate_filepath", "None"),
                    data[participant_id].get("respiratory_rate_record_count", "None"),
                    data[participant_id].get("average_respiratory_rate", "None"),
                    data[participant_id].get("physical_activity_filepath", "None"),
                    data[participant_id].get("physical_activity_num_days", "None"),
                    data[participant_id].get("average_physical_activity", "None"),
                    data[participant_id].get("active_calories_filepath", "None"),
                    data[participant_id].get("active_calories_record_count", "None"),
                    data[participant_id].get("average_active_calories", "None"),
                    data[participant_id].get("sensor_sampling_duration_days", "None"),
                    "Garmin",
                    "Vivosmart 5",
                ]
                writer.writerow(row)


# def main():
#     directories = {
#         "heart_rate": "wearable_activity_monitor/heart_rate/garmin_vivosmart5",
#         "oxygen_saturation": "wearable_activity_monitor/oxygen_saturation/garmin_vivosmart5",
#         "stress": "wearable_activity_monitor/stress/garmin_vivosmart5",
#         "sleep": "wearable_activity_monitor/sleep/garmin_vivosmart5",
#         "respiratory_rate": "wearable_activity_monitor/respiratory_rate/garmin_vivosmart5",
#         "activity": "wearable_activity_monitor/physical_activity/garmin_vivosmart5",
#         "calories": "wearable_activity_monitor/physical_activity_calorie/garmin_vivosmart5",
#     }

#     # Read wrist_worn_on and dominant_hand data from REDCap.tsv
#     read_redcap_file("REDCap.tsv")

#     for data_type, directory in directories.items():
#         globals()[f"process_{data_type}"](directory)

#     # Calculate sensor sampling duration based on heart rate data
#     calculate_sensor_sampling_duration(directories["heart_rate"])

#     # Write out the participants data as a TSV file
#     output_file = "manifest.tsv"
#     write_tsv(output_file, participants_data)


# if __name__ == "__main__":
#     main()
